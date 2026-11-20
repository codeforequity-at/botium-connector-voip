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
  const options = {
    executionProviders: ['cpu'],
    intraOpNumThreads: threads,
    interOpNumThreads: 1,
    executionMode: 'sequential',
    ...extra
  }
  if (threads === 1) {
    // No pool threads exist to spin; also stop spin-waits from the caller side.
    options.extra = { session: { intra_op: { allow_spinning: '0' }, inter_op: { allow_spinning: '0' } } }
  }
  return options
}

module.exports = {
  cpuSessionOptions,
  resolveThreads
}
