const { NamoTurnHandler } = require('./namo-turn-handler')
const { HANDLING_EOU } = require('../stt-message-handling')

const createTurnHandler = (caps, Capabilities, ctx) => {
  const raw = caps && caps[Capabilities.VOIP_STT_TURN_HANDLER]
  const key = String(raw || HANDLING_EOU).toUpperCase()
  if (key === 'SMART_TURN' || key === 'PSST' || key === 'NAMO') {
    ctx._info('voip_turn_handler_legacy_mapped', {
      sessionId: ctx.sessionId,
      from: key,
      to: HANDLING_EOU
    })
  }
  return new NamoTurnHandler(ctx)
}

module.exports = { createTurnHandler }
