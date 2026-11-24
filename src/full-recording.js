const createFullRecordingState = () => ({
  parts: [],
  nextSeq: 0
})

const resetFullRecordingState = (state) => {
  state.parts = []
  state.nextSeq = 0
}

const appendFullRecordingChunk = (state, base64, seq) => {
  if (!base64 || typeof base64 !== 'string') return 'empty'
  if (Number.isInteger(seq)) {
    if (seq < state.nextSeq) return 'duplicate'
    if (seq > state.nextSeq) return 'out-of-order'
    state.nextSeq++
  }
  state.parts.push(base64)
  return 'appended'
}

const getFullRecordingBase64 = (state) => state.parts.join('')

const getFrameSessionId = (frame) => frame && (frame.sessionId || (frame.voipConfig && frame.voipConfig.sessionId))

const isFrameForSession = (frame, sessionId) => {
  const frameSessionId = getFrameSessionId(frame)
  return !sessionId || !frameSessionId || frameSessionId === sessionId
}

module.exports = {
  appendFullRecordingChunk,
  createFullRecordingState,
  getFrameSessionId,
  getFullRecordingBase64,
  isFrameForSession,
  resetFullRecordingState
}
