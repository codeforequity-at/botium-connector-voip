const { test } = require('node:test')
const assert = require('assert')
const { trackSession, releaseOnnxSessions } = require('../src/onnx-lifecycle')

test('releaseOnnxSessions releases tracked sessions and ignores non-sessions', async () => {
  let released = 0
  trackSession({
    release: async () => {
      released++
    }
  })
  trackSession({ notASession: true })
  await releaseOnnxSessions()
  assert.strictEqual(released, 1)
  await releaseOnnxSessions()
  assert.strictEqual(released, 1)
})
