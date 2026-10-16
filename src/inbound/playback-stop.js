const STOPPABLE_WIRE_KINDS = new Set(['tts', 'media'])

/**
 * Whether inbound speech should trigger a one-shot stopAudio for agent playback.
 * @param {{ playback: { wireKind?: string, startAtMs?: number, endAtMs?: number, stopped?: boolean }|null, nowMs: number, echoWindowMs?: number }} params
 * @returns {boolean}
 */
function shouldStopAgentPlayback ({ playback, nowMs, echoWindowMs = 400 }) {
  if (!playback || playback.stopped) return false
  const kind = playback.wireKind
  if (!kind || !STOPPABLE_WIRE_KINDS.has(kind)) return false
  const startAtMs = Number(playback.startAtMs)
  const endAtMs = Number(playback.endAtMs)
  if (!Number.isFinite(startAtMs) || !Number.isFinite(endAtMs)) return false
  const echo = Math.max(0, Number(echoWindowMs) || 0)
  if (nowMs < startAtMs + echo) return false
  return nowMs < endAtMs
}

/**
 * True when inbound speech overlaps agent playback or lands shortly after it ends.
 * @param {{ playback: { startAtMs?: number, endAtMs?: number }|null, nowMs: number, tailMs?: number }} params
 * @returns {boolean}
 */
function inboundOverlapsAgentPlayback ({ playback, nowMs, tailMs = 500 }) {
  if (!playback) return false
  const startAtMs = Number(playback.startAtMs)
  const endAtMs = Number(playback.endAtMs)
  if (!Number.isFinite(startAtMs) || !Number.isFinite(endAtMs)) return false
  const tail = Math.max(0, Number(tailMs) || 0)
  return nowMs >= startAtMs && nowMs <= endAtMs + tail
}

module.exports = { shouldStopAgentPlayback, inboundOverlapsAgentPlayback, STOPPABLE_WIRE_KINDS }
