const { test } = require('node:test')
const assert = require('node:assert/strict')
const { EventEmitter } = require('node:events')
const Module = require('node:module')
const { PsstTurnHandler } = require('../src/turn-handlers/psst-turn-handler')

class WorkerSocket extends EventEmitter {
  constructor () {
    super()
    this.readyState = WorkerSocket.OPEN
    setImmediate(() => this.emit('open'))
  }

  send (request) {
    if (JSON.parse(request).METHOD === 'initCall') {
      setImmediate(() => {
        this.frame({ type: 'callinfo', status: 'initialized', voipConfig: { sessionId: 'test-session' } })
        this.frame({ type: 'callinfo', status: 'connected' })
      })
    }
  }

  frame (frame) {
    this.emit('message', JSON.stringify(frame))
  }

  terminate () { this.readyState = WorkerSocket.CLOSED }
}
WorkerSocket.OPEN = 1
WorkerSocket.CLOSED = 3

// Exercise the real connector message handler and PSST queue without a SIP
// worker or the unrelated model-based turn handler dependencies.
const originalLoad = Module._load
let Connector
try {
  Module._load = function (request, parent, isMain) {
    if (request === 'onnxruntime-node') return {}
    if (parent && parent.filename.endsWith('/src/connector.js')) {
      if (request === 'ws') return WorkerSocket
      if (request === 'axios') return async () => ({ data: { port: 1234 }, headers: {} })
      if (request === './turn-handlers/create-turn-handler') {
        return { createTurnHandler: (caps, capabilities, ctx) => new PsstTurnHandler(ctx) }
      }
    }
    return originalLoad.call(this, request, parent, isMain)
  }
  Connector = require('../src/connector')
} finally {
  Module._load = originalLoad
}

const wait = ms => new Promise(resolve => setTimeout(resolve, ms))
const createConnector = async (t) => {
  const messages = []
  const connector = new Connector({
    queueBotSays: message => messages.push(message),
    eventEmitter: new EventEmitter(),
    caps: {
      VOIP_WORKER_URL: 'ws://worker.invalid',
      VOIP_STT_URL_STREAM: 'https://speech.invalid/stt',
      VOIP_STT_MESSAGE_HANDLING: 'PSST',
      VOIP_STT_MESSAGE_HANDLING_TIMEOUT: 10,
      VOIP_STT_CONFIDENCE_THRESHOLD: 0,
      VOIP_NAMO_VAD_ENABLE: false,
      VOIP_CED_ENABLE: false
    }
  })
  t.after(() => {
    connector._sttEndRecovery.clear()
    connector.turnHandler.reset()
    connector.ws.removeAllListeners()
  })
  await connector.Start()
  return { connector, messages, frame: value => connector.ws.frame(value) }
}

const partial = { type: 'stt', data: { type: 'stt', final: false, message: "we're sorry", start: 1.01, end: 6.28 } }
const final = { type: 'stt', data: { type: 'stt', final: true, message: "We're sorry, please try again later.", start: 1.01, end: 7.01, confidence: 1 } }

test('real disconnect / late-final path emits one bot turn with nonnegative silence', async t => {
  const h = await createConnector(t)
  h.frame(partial)
  h.frame({ type: 'callinfo', status: 'disconnected', connectDuration: 6 })
  await wait(134)
  assert.equal(h.messages.length, 0)
  h.frame(final)
  // The recording-end flush also covers final arrival before the PSST timer.
  h.connector._flushPendingBotMsgs('fullRecordEnd')
  await wait(30)
  assert.equal(h.messages.length, 1)
  assert.equal(h.messages[0].messageText, final.data.message)
  assert.equal(h.messages[0].sourceData[0].silenceDuration, 1.01)
  assert.equal(h.messages[0].sourceData[0].voiceDuration, 6)
})

test('recording end without a final recovers once and ignores the same late final', async t => {
  const h = await createConnector(t)
  h.frame(partial)
  h.frame({ type: 'callinfo', status: 'disconnected' })
  h.connector._flushPendingBotMsgs('fullRecordEnd')
  h.frame(final)
  await wait(30)
  assert.equal(h.messages.length, 1)
  assert.equal(h.messages[0].sourceData.partialRecovery, true)
  // A separate later segment must still be delivered.
  h.frame({ ...final, data: { ...final.data, start: 8, end: 9 } })
  h.connector._flushPendingBotMsgs('fullRecordEnd')
  await wait(30)
  assert.equal(h.messages.length, 2)
})
