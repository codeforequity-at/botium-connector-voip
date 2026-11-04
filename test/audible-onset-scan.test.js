const { test } = require('node:test')
const assert = require('node:assert/strict')

const BotiumConnectorVoip = require('../src/connector')

// Regression: _maybeDetectAgentAudibleOnRecording runs on EVERY inbound audio
// chunk. It used to Buffer.concat the entire recording and rescan on each
// chunk — O(recording length) per chunk, O(n^2) over a call. On an 11-minute
// call the worker pinned at ~120% CPU and downstream turn processing stalled
// minutes behind real time. The scan must be amortized: cached concat,
// throttled rescans, and no work at all once the onset is located.

const BYTES_PER_SEC = 16000 // 8kHz, 16-bit, mono

function makeConnectorWithStream () {
  const connector = Object.create(BotiumConnectorVoip.prototype)
  // 6s of audio: silence, then a loud segment from 2s to 4s.
  const pcm = Buffer.alloc(BYTES_PER_SEC * 6)
  for (let i = BYTES_PER_SEC * 2; i < BYTES_PER_SEC * 4; i += 2) {
    pcm.writeInt16LE(12000, i)
  }
  const parts = []
  for (let off = 0; off < pcm.length; off += 1600) {
    parts.push(pcm.subarray(off, off + 1600))
  }
  connector.audioStream = {
    pcmParts: parts,
    totalBytes: pcm.length,
    format: { sampleRate: 8000, channels: 1, bitsPerSample: 16 }
  }
  return connector
}

function countConcats (fn) {
  const orig = Buffer.concat
  let calls = 0
  Buffer.concat = (...args) => { calls++; return orig(...args) }
  try {
    fn()
  } finally {
    Buffer.concat = orig
  }
  return calls
}

test('finds the audible onset on the stream', () => {
  const connector = makeConnectorWithStream()
  const heard = connector._findAudibleRecordingStartSecOnStream(1.0, null)
  assert.ok(heard >= 1.9 && heard < 2.5, `expected onset ~2s, got ${heard}`)
})

test('concatenates the recording once, not once per scan', () => {
  const connector = makeConnectorWithStream()
  const first = countConcats(() => connector._findAudibleRecordingStartSecOnStream(1.0, null))
  assert.equal(first, 1)
  const second = countConcats(() => connector._findAudibleRecordingStartSecOnStream(1.0, null))
  assert.equal(second, 0, 'cached concat must be reused while totalBytes is unchanged')
})

test('does not rescan once the onset is located', () => {
  const connector = makeConnectorWithStream()
  const agent = { playedRecordingStartSec: 1.0, heardRecordingStartSec: 2.0 }
  const calls = countConcats(() => connector._maybeDetectAgentAudibleOnRecording(agent))
  assert.equal(calls, 0)
})

test('throttles rescans to ~0.5s of new audio', () => {
  const connector = makeConnectorWithStream()
  const agent = { playedRecordingStartSec: 1.0 }
  // First call scans (and thereby records the scanned byte count).
  connector._maybeDetectAgentAudibleOnRecording(agent)
  assert.ok(agent._lastAudibleScanBytes > 0)
  // An immediate second call with no meaningful new audio must not rescan.
  let rescanned = false
  const origApply = connector._applyAgentHeardRecordingStartSec
  connector._applyAgentHeardRecordingStartSec = () => { rescanned = true }
  try {
    connector._maybeDetectAgentAudibleOnRecording(agent)
  } finally {
    connector._applyAgentHeardRecordingStartSec = origApply
  }
  assert.equal(rescanned, false)
})
