const HANDLING_EOU = 'EOU'
const HANDLING_EOU_LEGACY = 'NAMO'

const normalizeHandling = (h) => {
  const value = String(h || '').trim().toUpperCase()
  if (value === HANDLING_EOU_LEGACY) return HANDLING_EOU
  return value
}

const isEouHandling = (handling) => normalizeHandling(handling) === HANDLING_EOU

const isBufferedSttHandling = (handling) => {
  const h = normalizeHandling(handling)
  return h === 'JOIN' || h === 'PSST' || h === 'CONCAT' || h === HANDLING_EOU
}

module.exports = {
  HANDLING_EOU,
  HANDLING_EOU_LEGACY,
  isEouHandling,
  isBufferedSttHandling,
  normalizeHandling
}
