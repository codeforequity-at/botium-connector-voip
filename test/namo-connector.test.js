const test = require('node:test')
const assert = require('node:assert/strict')
const { EventEmitter } = require('node:events')
const Connector = require('../src/namo-connector')

const wait = ms => new Promise(resolve => setTimeout(resolve, ms))

test('holds incomplete STT finals and flushes their joined text when complete', async () => {
  const queued = []
  const decisions = [
    { eouProbability: 0.1, incompleteProbability: 0.9, inferenceMs: 2 },
    { eouProbability: 0.95, incompleteProbability: 0.05, inferenceMs: 3 }
  ]
  const connector = new Connector({
    queueBotSays: message => queued.push(message),
    eventEmitter: new EventEmitter(),
    caps: {
      VOIP_STT_MESSAGE_HANDLING: 'NAMO',
      VOIP_NAMO_EOU_THRESHOLD: 0.85,
      VOIP_NAMO_MIN_WAIT_MS: 0,
      VOIP_NAMO_MAX_WAIT_MS: 1000,
      VOIP_STT_MESSAGE_HANDLING_DELIMITER: ' '
    }
  })
  connector.detector = {
    predict: async () => decisions.shift()
  }

  connector._handleBaseMessage({
    messageText: 'Your available balance',
    sourceData: { data: { start: 1, end: 2 } }
  })
  await wait(10)
  assert.equal(queued.length, 0)

  connector._handleBaseMessage({
    messageText: 'is one hundred dollars.',
    sourceData: { data: { start: 2.5, end: 4 } }
  })
  await wait(10)

  assert.equal(queued.length, 1)
  assert.equal(queued[0].messageText, 'Your available balance is one hundred dollars.')
  assert.equal(queued[0].sourceData.length, 2)
  assert.equal(queued[0].namoGate.reason, 'model_complete')
  assert.equal(queued[0].namoGate.bufferedChunks, 2)
})
