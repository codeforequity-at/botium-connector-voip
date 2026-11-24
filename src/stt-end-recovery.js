// Disconnect can precede the worker's final STT result. Give that result a
// bounded drain window before falling back to an interim transcript.
const createSttEndRecovery = ({ recover, delayMs = 1500, schedule = setTimeout, cancel = clearTimeout }) => {
  let timer = null
  let recovered = null

  const clear = () => {
    if (timer !== null) cancel(timer)
    timer = null
  }

  return {
    defer () {
      if (timer !== null) return
      timer = schedule(() => {
        timer = null
        recover()
      }, delayMs)
    },
    clear,
    markRecovered (message) {
      recovered = message && message.sourceData && message.sourceData.data
    },
    acceptFinal (frame) {
      clear()
      const data = frame && frame.data
      // After the fallback has already been delivered, suppress only a final
      // for that same audio segment. Later, distinct speech must still pass.
      if (recovered && data &&
        Number.isFinite(recovered.start) && Number.isFinite(recovered.end) &&
        Number.isFinite(data.start) && Number.isFinite(data.end) &&
        Math.abs(data.start - recovered.start) < 0.05 &&
        Math.min(data.end, recovered.end) > Math.max(data.start, recovered.start)) {
        return false
      }
      return true
    }
  }
}

module.exports = { createSttEndRecovery }
