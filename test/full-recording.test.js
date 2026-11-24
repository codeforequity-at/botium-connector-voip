const { test } = require('node:test')
const assert = require('node:assert/strict')
const {
  appendFullRecordingChunk,
  createFullRecordingState,
  getFullRecordingBase64,
  isFrameForSession,
  resetFullRecordingState
} = require('../src/full-recording')

test('full recording chunks are accumulated in sequence without duplicates', () => {
  const state = createFullRecordingState()

  assert.equal(appendFullRecordingChunk(state, 'first', 0), 'appended')
  assert.equal(appendFullRecordingChunk(state, 'duplicate', 0), 'duplicate')
  assert.equal(appendFullRecordingChunk(state, 'gap', 2), 'out-of-order')
  assert.equal(appendFullRecordingChunk(state, 'second', 1), 'appended')
  assert.equal(getFullRecordingBase64(state), 'firstsecond')
})

test('reset isolates a subsequent recording', () => {
  const state = createFullRecordingState()
  appendFullRecordingChunk(state, 'old', 0)

  resetFullRecordingState(state)
  appendFullRecordingChunk(state, 'new', 0)

  assert.equal(getFullRecordingBase64(state), 'new')
})

test('session matching supports top-level and callinfo session ids', () => {
  assert.equal(isFrameForSession({ sessionId: 'session-a' }, 'session-a'), true)
  assert.equal(isFrameForSession({ sessionId: 'session-b' }, 'session-a'), false)
  assert.equal(isFrameForSession({ voipConfig: { sessionId: 'session-a' } }, 'session-a'), true)
  assert.equal(isFrameForSession({ voipConfig: { sessionId: 'session-b' } }, 'session-a'), false)
  assert.equal(isFrameForSession({ type: 'legacy-frame' }, 'session-a'), true)
})
