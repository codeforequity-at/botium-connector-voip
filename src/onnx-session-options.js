/**
 * CPU session options shared by every ONNX model in the connector (TEN VAD, CED, Namo).
 *
 * By default onnxruntime sizes its thread pools to the host's core count, and pool threads
 * spin while idle. In a CPU-limited container (e.g. box-server) the process still sees all host
 * cores, so those threads burn the CPU quota and starve the event loop that reads the VoIP
 * WebSocket. These models are tiny, so a single thread is both enough and much faster there.
 * Override with BOTIUM_VOIP_ONNX_THREADS on hosts with spare cores.
 */
const DEFAULT_THREADS = 1

const resolveThreads = (env = process.env) => {
  const parsed = parseInt(env.BOTIUM_VOIP_ONNX_THREADS, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_THREADS
}

const cpuSessionOptions = (extra = {}, env = process.env) => {
  const threads = resolveThreads(env)
  const { extra: extraConfig, ...rest } = extra
  const options = {
    executionProviders: ['cpu'],
    intraOpNumThreads: threads,
    interOpNumThreads: 1,
    executionMode: 'sequential',
    ...rest
  }
  const session = { ...(extraConfig && extraConfig.session) }
  if (threads === 1) {
    // No pool threads exist to spin; also stop spin-waits from the caller side.
    session.intra_op = { allow_spinning: '0' }
    session.inter_op = { allow_spinning: '0' }
  }
  if (Object.keys(session).length > 0 || extraConfig) {
    options.extra = { ...extraConfig, session }
  }
  return options
}

/**
 * Namo ships as an int8-quantised model. onnxruntime's x86_64 AVX2 int8 GEMM kernel saturates
 * on it and returns a near-constant ~0.137 for every sentence (so Namo never decides a turn is
 * complete). `session.x64quantprecision=1` makes those kernels use higher-precision math: results
 * then match the fp32 model. It has no effect on ARM or non-AVX2 paths.
 * Set BOTIUM_VOIP_NAMO_X64_QUANT_PRECISION=0 to disable.
 */
const namoSessionOptions = (extra = {}, env = process.env) => {
  const enabled = String(env.BOTIUM_VOIP_NAMO_X64_QUANT_PRECISION || '1') !== '0'
  const session = enabled ? { x64quantprecision: '1' } : {}
  return cpuSessionOptions({ ...extra, extra: { session } }, env)
}

module.exports = {
  cpuSessionOptions,
  namoSessionOptions,
  resolveThreads
}
