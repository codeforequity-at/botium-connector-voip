const CONNECTOR_DEADLINE_FLOOR_MS = 800

const isTruthyCap = (v) => v !== false && v !== 'false' && v !== 0 && v !== '0'

const parseCapInt = (caps, key, fallback) => {
  const parsed = parseInt(caps[key], 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

/**
 * Wall-clock ms when IVR speech ended for the current STT final (speechEndSec on recording timeline).
 */
function speechEndAtMsFromFinal ({ finalAtMs, recordingAtSttFinalSec, speechEndSec }) {
  if (!Number.isFinite(finalAtMs)) return null
  if (!Number.isFinite(recordingAtSttFinalSec) || !Number.isFinite(speechEndSec)) return null
  const lagSec = recordingAtSttFinalSec - speechEndSec
  if (!Number.isFinite(lagSec) || lagSec < 0) return null
  return Math.round(finalAtMs - lagSec * 1000)
}

function computeConnectorDeadlineMs (caps, Capabilities) {
  const budget = parseCapInt(caps, Capabilities.VOIP_REPLY_BUDGET_MS, 0)
  if (!budget || budget <= 0) return null
  const coach = parseCapInt(caps, Capabilities.VOIP_REPLY_COACH_RESERVE_MS, 2500)
  const wire = parseCapInt(caps, Capabilities.VOIP_REPLY_WIRE_RESERVE_MS, 800)
  const raw = budget - coach - wire
  return Math.max(CONNECTOR_DEADLINE_FLOOR_MS, raw)
}

function computeReplyConnectorDeadlineAtMs (speechEndAtMs, caps, Capabilities) {
  const connectorMs = computeConnectorDeadlineMs(caps, Capabilities)
  if (!connectorMs || !Number.isFinite(speechEndAtMs)) return null
  return speechEndAtMs + connectorMs
}

/**
 * Remaining ms before max_wait flush, optionally capped by reply-budget connector deadline.
 */
function computeMaxWaitRemainingMs ({
  anchorMs,
  maxWaitMs,
  maxWaitExtended = false,
  vadExtensionMs = 0,
  deadlineAtMs = null,
  now = Date.now()
}) {
  if (!Number.isFinite(anchorMs)) {
    return { remainingMs: maxWaitMs, cappedByDeadline: false }
  }
  const elapsed = Math.max(0, now - anchorMs)
  const ceiling = maxWaitMs + (maxWaitExtended ? vadExtensionMs : 0)
  let remainingMs = Math.max(0, ceiling - elapsed)
  let cappedByDeadline = false
  if (Number.isFinite(deadlineAtMs)) {
    const deadlineRemaining = Math.max(0, deadlineAtMs - now)
    if (deadlineRemaining < remainingMs) {
      remainingMs = deadlineRemaining
      cappedByDeadline = true
    }
  }
  return { remainingMs, cappedByDeadline }
}

/**
 * When VOIP_OTS_LATENCY_PROFILE is enabled, apply latency-friendly defaults only for keys
 * the user did not set on the chatbot capability object.
 */
function applyOtsLatencyProfile (mergedCaps, userCaps, Capabilities, Defaults) {
  if (!isTruthyCap(mergedCaps[Capabilities.VOIP_OTS_LATENCY_PROFILE])) {
    return mergedCaps
  }
  const user = userCaps || {}
  const setIfUnset = (key, value) => {
    if (user[key] === undefined || user[key] === null || user[key] === '') {
      mergedCaps[key] = value
    }
  }
  setIfUnset(Capabilities.VOIP_REPLY_BUDGET_MS, 5000)
  setIfUnset(Capabilities.VOIP_REPLY_COACH_RESERVE_MS, 2500)
  setIfUnset(Capabilities.VOIP_REPLY_WIRE_RESERVE_MS, 800)
  setIfUnset(Capabilities.VOIP_NAMO_REOPEN_MS, 500)
  setIfUnset(Capabilities.VOIP_NAMO_EMIT_STABLE_MS, 280)
  setIfUnset(Capabilities.VOIP_NAMO_MAX_WAIT_MS, 4000)
  setIfUnset(Capabilities.VOIP_NAMO_MIN_WAIT_MS, 0)
  setIfUnset(Capabilities.VOIP_NAMO_QUESTION_FLUSH_MS, 0)
  setIfUnset(Capabilities.VOIP_NAMO_MAX_WAIT_VAD_EXTENSION_MS, 0)
  setIfUnset(Capabilities.VOIP_NAMO_JOINED_QUESTION_FLUSH_ENABLE, true)
  setIfUnset(Capabilities.VOIP_STT_AZURE_SEGMENTATION_SILENCE_TIMEOUT_MS, 400)
  setIfUnset(Capabilities.VOIP_CED_ENABLE, false)
  setIfUnset(Capabilities.VOIP_STT_MESSAGE_HANDLING, 'NAMO')
  return mergedCaps
}

module.exports = {
  CONNECTOR_DEADLINE_FLOOR_MS,
  speechEndAtMsFromFinal,
  computeConnectorDeadlineMs,
  computeReplyConnectorDeadlineAtMs,
  computeMaxWaitRemainingMs,
  applyOtsLatencyProfile,
  isTruthyCap,
  parseCapInt
}
