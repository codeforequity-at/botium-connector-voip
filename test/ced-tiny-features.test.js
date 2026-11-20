const { test } = require('node:test')
const assert = require('node:assert/strict')
const ort = require('onnxruntime-node')
const path = require('path')
const { computeCedFeats } = require('../src/audio-tagging/ced-tiny-features')
const { bundledModelPath } = require('../src/package-assets')

test('computeCedFeats produces ONNX-compatible [1, 64, T] tensor', async () => {
  const waveform = new Float32Array(16000)
  for (let i = 0; i < waveform.length; i++) {
    waveform[i] = 0.001 * Math.sin(i / 40)
  }
  const feats = computeCedFeats(waveform)
  assert.ok(feats)
  assert.equal(feats.dims[0], 1)
  assert.equal(feats.dims[1], 64)
  assert.ok(feats.dims[2] > 0)

  const modelPath = bundledModelPath('ced-tiny.int8.onnx')
  const session = await ort.InferenceSession.create(modelPath, { executionProviders: ['cpu'] })
  const out = await session.run({
    feats: new ort.Tensor('float32', feats.data, feats.dims)
  })
  const prob = out[session.outputNames[0]].data
  assert.equal(prob.length, 527)
})

// Original direct DFT, kept as the reference for the FFT implementation.
const referencePower = (frame, window) => {
  const out = new Float64Array(257)
  for (let k = 0; k < 257; k++) {
    let re = 0
    let im = 0
    for (let n = 0; n < 512; n++) {
      const x = frame[n] * window[n]
      const ang = (2 * Math.PI * k * n) / 512
      re += x * Math.cos(ang)
      im -= x * Math.sin(ang)
    }
    out[k] = re * re + im * im
  }
  return out
}

const hann = () => Float32Array.from({ length: 512 }, (_, i) => 0.5 * (1 - Math.cos((2 * Math.PI * i) / 511)))

test('rfft512Power matches the direct DFT power spectrum', () => {
  const { rfft512Power } = require('../src/audio-tagging/ced-tiny-features')
  const window = hann()
  for (const gen of [
    (i) => Math.sin(i / 7) * 0.4 + Math.cos(i / 31) * 0.2,
    (i) => ((i * 7919) % 97) / 97 - 0.5,
    (i) => (i === 100 ? 1 : 0)
  ]) {
    const frame = Float32Array.from({ length: 512 }, (_, i) => gen(i))
    const expected = referencePower(frame, window)
    const actual = new Float32Array(257)
    rfft512Power(frame, window, actual)
    const peak = Math.max(...expected)
    for (let k = 0; k < 257; k++) {
      assert.ok(Math.abs(actual[k] - expected[k]) <= peak * 1e-5 + 1e-9, `bin ${k}: ${actual[k]} vs ${expected[k]}`)
    }
  }
})
