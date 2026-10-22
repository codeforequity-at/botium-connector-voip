const { describe, it } = require('node:test')
const assert = require('node:assert/strict')
const { NamoTurnGate, isIncompleteReadbackChunk } = require('../src/turn-handlers/namo-turn-gate.js')

describe('isIncompleteReadbackChunk', () => {
  it('holds bare digit groups until a confirmation question arrives', () => {
    assert.equal(isIncompleteReadbackChunk('8888.', '8888.'), true)
    assert.equal(isIncompleteReadbackChunk('0000.', 'To confirm 4012. 0000.'), true)
    assert.equal(isIncompleteReadbackChunk('To confirm 4012.', 'To confirm 4012.'), true)
    assert.equal(isIncompleteReadbackChunk('6 right?', 'To confirm 4012. 8888. 6 right?'), false)
    assert.equal(isIncompleteReadbackChunk('8888.', '8888. 6 right?'), false)
  })
})

describe('NamoTurnGate readback hold', () => {
  it('does not flush on model_complete for a lone digit readback chunk', async () => {
    const messages = [{ messageText: '8888.' }]
    let flushed = false
    const gate = new NamoTurnGate({
      caps: {},
      Capabilities: {},
      detector: {
        predict: async () => ({ eouProbability: 0.99, incompleteProbability: 0.01, inferenceMs: 1 })
      },
      getMessages: () => messages,
      commitFlush: () => { flushed = true },
      getVadSpeechActive: () => false,
      getVadEnabled: () => false,
      getLastVadSpeechEndAt: () => null,
      setLastVadSpeechEndAt: () => {},
      eventEmitter: null,
      sessionId: 'test',
      _info: () => {},
      onInferenceError: () => {}
    })
    gate.turnState = 'buffering'
    gate.pendingStartedAt = Date.now()
    gate.vadEnabled = false
    gate.emitStableMs = 0

    await gate._evaluateCandidate(gate.candidateVersion)

    assert.equal(flushed, false)
    assert.equal(messages.length, 1)
  })
})
