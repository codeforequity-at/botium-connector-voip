const { test } = require('node:test')
const assert = require('node:assert/strict')
const { EventEmitter } = require('node:events')
const Module = require('node:module')
const { PsstTurnHandler } = require('../src/turn-handlers/psst-turn-handler')

class WorkerSocket extends EventEmitter {
  constructor () {
    super()
    this.readyState = WorkerSocket.OPEN
    this.sent = []
    setImmediate(() => this.emit('open'))
  }

  send (request) {
    this.sent.push(JSON.parse(request).METHOD)
    if (JSON.parse(request).METHOD === 'initCall') {
      setImmediate(() => {
        this.frame({ type: 'callinfo', status: 'initialized', voipConfig: { sessionId: 'test-session' } })
        if (WorkerSocket.reject487Setups > 0) {
          WorkerSocket.reject487Setups--
          this.frame({ type: 'callinfo', status: 'disconnected' })
          this.frame({ type: 'error', message: 'Disconnected because of error - Reason: 487 Request Terminated' })
          return
        }
        this.frame({ type: 'callinfo', status: 'connected' })
      })
    }
  }

  frame (frame) {
    this.emit('message', JSON.stringify(frame))
  }

  drop (code = 1006) {
    this.readyState = WorkerSocket.CLOSED
    this.emit('close', code, Buffer.from(''))
  }

  terminate () { this.readyState = WorkerSocket.CLOSED }
}
WorkerSocket.OPEN = 1
WorkerSocket.CLOSED = 3
WorkerSocket.reject487Setups = 0

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
const createConnector = async (t, extraCaps = {}) => {
  const messages = []
  const connector = new Connector({
    queueBotSays: message => messages.push(message),
    eventEmitter: new EventEmitter(),
    caps: {
      ...extraCaps,
      VOIP_WORKER_URL: 'ws://worker.invalid',
      VOIP_STT_URL_STREAM: 'https://speech.invalid/stt',
      VOIP_STT_MESSAGE_HANDLING: 'PSST',
      VOIP_STT_MESSAGE_HANDLING_TIMEOUT: 10,
      VOIP_STT_CONFIDENCE_THRESHOLD: 0,
      VOIP_NAMO_VAD_ENABLE: false,
      VOIP_CED_ENABLE: false
    }
  })
  await connector.Start()
  const ws = connector.ws
  t.after(() => {
    connector._sttEndRecovery.clear()
    connector.turnHandler.reset()
    ws.removeAllListeners()
  })
  return { connector, messages, ws }
}

test('unexpected websocket close mid-call fails the conversation', async t => {
  const h = await createConnector(t)
  h.ws.drop(1006)
  assert.equal(h.connector.end, true)
  await wait(10)
  assert.equal(h.messages.length, 1)
  assert.ok(h.messages[0] instanceof Error)
  assert.match(h.messages[0].message, /closed unexpectedly \(code 1006\)/)
})

test('websocket close after stopCall ends Stop() without waiting for a recording', async t => {
  const h = await createConnector(t)
  const started = Date.now()
  const stopping = h.connector.Stop()
  assert.ok(h.ws.sent.includes('stopCall'))
  setImmediate(() => h.ws.drop(1000))
  await stopping
  assert.ok(Date.now() - started < 1000)
  assert.equal(h.messages.length, 0)
})

test('websocket close after the call ended normally is not an error', async t => {
  const h = await createConnector(t)
  h.connector.end = true
  h.ws.drop(1000)
  await wait(10)
  assert.equal(h.messages.length, 0)
})

test('a 487 setup retry does not carry the disconnect over to the retried session', async t => {
  WorkerSocket.reject487Setups = 1
  const h = await createConnector(t, { VOIP_CALL_SETUP_RETRY_487_MAXRETRIES: 1, VOIP_CALL_SETUP_RETRY_487_TIMEOUT: 1 })
  assert.equal(WorkerSocket.reject487Setups, 0)
  h.ws.drop(1006)
  await wait(10)
  assert.equal(h.messages.length, 1)
  assert.match(h.messages[0].message, /closed unexpectedly/)
})

test('websocket close after a remote hangup without a recording is not an error', async t => {
  const h = await createConnector(t)
  h.ws.frame({ type: 'callinfo', status: 'disconnected', connectDuration: 3 })
  h.ws.drop(1000)
  await wait(10)
  assert.equal(h.connector.end, true)
  assert.equal(h.messages.length, 0)
})
