const { test } = require('node:test')
const assert = require('node:assert/strict')
const { shouldStopAgentPlayback, inboundOverlapsAgentPlayback } = require('../src/inbound/playback-stop')

test('shouldStopAgentPlayback ignores DTMF and echo window', () => {
  const playback = { wireKind: 'dtmf', startAtMs: 1000, endAtMs: 3000, stopped: false }
  assert.equal(shouldStopAgentPlayback({ playback, nowMs: 1500 }), false)

  const tts = { wireKind: 'tts', startAtMs: 1000, endAtMs: 3000, stopped: false }
  assert.equal(shouldStopAgentPlayback({ playback: tts, nowMs: 1200, echoWindowMs: 400 }), false)
  assert.equal(shouldStopAgentPlayback({ playback: tts, nowMs: 1500, echoWindowMs: 400 }), true)
  assert.equal(shouldStopAgentPlayback({ playback: { ...tts, stopped: true }, nowMs: 1500 }), false)
  assert.equal(shouldStopAgentPlayback({ playback: tts, nowMs: 3100 }), false)
})

test('inboundOverlapsAgentPlayback includes tail window after playback end', () => {
  const playback = { startAtMs: 1000, endAtMs: 2000 }
  assert.equal(inboundOverlapsAgentPlayback({ playback, nowMs: 1500 }), true)
  assert.equal(inboundOverlapsAgentPlayback({ playback, nowMs: 2400, tailMs: 500 }), true)
  assert.equal(inboundOverlapsAgentPlayback({ playback, nowMs: 2600, tailMs: 500 }), false)
})
