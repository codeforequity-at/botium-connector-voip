const HANDLING_EOU = 'EOU'

const normalizeHandling = (h) => String(h || '').trim().toUpperCase()

const isEouHandling = (handling) => normalizeHandling(handling) === HANDLING_EOU

const isBufferedSttHandling = (handling) => {
  const h = normalizeHandling(handling)
  return h === 'JOIN' || h === 'PSST' || h === 'CONCAT' || h === HANDLING_EOU
}

module.exports = {
  HANDLING_EOU,
  isEouHandling,
  isBufferedSttHandling,
  normalizeHandling
}
