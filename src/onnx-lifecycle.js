const sessions = new Set()
let exitHookInstalled = false

const trackSession = (session) => {
  if (session && typeof session.release === 'function') sessions.add(session)
  return session
}

const releaseOnnxSessions = () => {
  const pending = [...sessions]
  sessions.clear()
  if (pending.length === 0) return Promise.resolve()
  return Promise.all(pending.map(async (session) => {
    try {
      await session.release()
    } catch (err) {
      // A session that is already closing should not block process shutdown.
    }
  })).then(() => undefined)
}

/**
 * Release ONNX sessions before the worker calls process.exit(). Leaving them
 * alive lets onnxruntime tear the addon down under an active session.
 *
 * onnxruntime-node 1.21 still aborts on macOS during process.exit itself
 * (SIGABRT, "mutex lock failed"), even after release. That build is fixed in
 * 1.24.1+. The objective-test parent treats a completed result file plus
 * SIGABRT as a finished run so an older binary cannot fail a successful job.
 */
const installOnnxExitHook = () => {
  if (exitHookInstalled) return
  exitHookInstalled = true
  const originalExit = process.exit.bind(process)
  process.exit = (code) => {
    if (process.__botiumVoipOnnxExiting) {
      originalExit(code)
      return
    }
    process.__botiumVoipOnnxExiting = true
    const timer = setTimeout(() => originalExit(code), 2000)
    Promise.resolve(releaseOnnxSessions())
      .catch(() => undefined)
      .then(() => {
        clearTimeout(timer)
        originalExit(code)
      })
  }
}

module.exports = {
  trackSession,
  releaseOnnxSessions,
  installOnnxExitHook
}
