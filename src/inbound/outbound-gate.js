const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

/**
 * Wait until inbound speech (and optional STT partials) allow agent playout.
 * softMaxWaitMs: reply-budget wire reserve (may be ~800ms). hardMaxWaitMs: absolute
 * ceiling (VOIP_OUTBOUND_GATE_MAX_WAIT_MS) while STT partials or TEN VAD speech block.
 */
async function waitForOutboundAllowed ({
  inboundLane,
  sceneClassifier,
  quietMs,
  softMaxWaitMs,
  hardMaxWaitMs,
  maxWaitMs,
  pollMs = 50,
  hasRecentSttPartial = () => false
}) {
  const quiet = Math.max(0, Number(quietMs) || 430)
  const soft = Math.max(quiet, Number(softMaxWaitMs ?? maxWaitMs) || 5000)
  const hard = Math.max(soft, Number(hardMaxWaitMs ?? maxWaitMs) || soft)
  const start = Date.now()
  const hardDeadline = start + hard

  const isBlocked = () => {
    if (!inboundLane) return false
    const musicDominant = sceneClassifier && sceneClassifier.isMusicDominant
      ? sceneClassifier.isMusicDominant()
      : false
    return inboundLane.blocksOutbound({
      musicDominant,
      hasSttPartials: hasRecentSttPartial()
    })
  }

  while (Date.now() < hardDeadline) {
    if (!inboundLane) {
      return { waitedMs: Date.now() - start, allowed: true, reason: 'no_lane' }
    }
    const blocked = isBlocked()
    if (!blocked && inboundLane.msSinceSpeechSilent() >= quiet) {
      return { waitedMs: Date.now() - start, allowed: true, reason: 'inbound_quiet' }
    }
    await sleep(pollMs)
  }

  const sttOrSpeechActive = isBlocked()
  return {
    waitedMs: Date.now() - start,
    allowed: true,
    reason: 'max_wait',
    ...(sttOrSpeechActive ? { sttOrSpeechActive: true } : {})
  }
}

module.exports = { waitForOutboundAllowed }
