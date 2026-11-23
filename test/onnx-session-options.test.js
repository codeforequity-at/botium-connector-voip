const { test } = require('node:test')
const assert = require('node:assert/strict')
const { cpuSessionOptions, namoSessionOptions, resolveThreads } = require('../src/onnx-session-options')

test('cpuSessionOptions defaults to a single non-spinning thread', () => {
  const opts = cpuSessionOptions({}, {})
  assert.deepEqual(opts.executionProviders, ['cpu'])
  assert.equal(opts.intraOpNumThreads, 1)
  assert.equal(opts.interOpNumThreads, 1)
  assert.equal(opts.executionMode, 'sequential')
  assert.equal(opts.extra.session.intra_op.allow_spinning, '0')
})

test('cpuSessionOptions keeps caller options and honours BOTIUM_VOIP_ONNX_THREADS', () => {
  const opts = cpuSessionOptions({ graphOptimizationLevel: 'all' }, { BOTIUM_VOIP_ONNX_THREADS: '4' })
  assert.equal(opts.graphOptimizationLevel, 'all')
  assert.equal(opts.intraOpNumThreads, 4)
  assert.equal(opts.extra, undefined)
})

test('resolveThreads ignores invalid values', () => {
  assert.equal(resolveThreads({ BOTIUM_VOIP_ONNX_THREADS: 'abc' }), 1)
  assert.equal(resolveThreads({ BOTIUM_VOIP_ONNX_THREADS: '0' }), 1)
  assert.equal(resolveThreads({}), 1)
})

test('single-thread options load the bundled TEN VAD model', async () => {
  const ort = require('onnxruntime-node')
  const { bundledModelPath } = require('../src/package-assets')
  const session = await ort.InferenceSession.create(bundledModelPath('ten-vad.int8.onnx'), cpuSessionOptions({}, {}))
  assert.ok(session.inputNames.length > 0)
  await session.release()
})

test('namoSessionOptions enables x64quantprecision and keeps the spinning settings', () => {
  const opts = namoSessionOptions({ graphOptimizationLevel: 'all' }, {})
  assert.equal(opts.graphOptimizationLevel, 'all')
  assert.equal(opts.extra.session.x64quantprecision, '1')
  assert.equal(opts.extra.session.intra_op.allow_spinning, '0')
})

test('namoSessionOptions can be disabled and works with multiple threads', () => {
  const off = namoSessionOptions({}, { BOTIUM_VOIP_NAMO_X64_QUANT_PRECISION: '0' })
  assert.equal(off.extra.session.x64quantprecision, undefined)
  const multi = namoSessionOptions({}, { BOTIUM_VOIP_ONNX_THREADS: '4' })
  assert.equal(multi.extra.session.x64quantprecision, '1')
  assert.equal(multi.extra.session.intra_op, undefined)
})
