const debug = require('debug')('botium-voip-logichook-confidence-threshold')

module.exports = class ConfidenceThresholdLogicHook {
  constructor (context, caps = {}, args = {}) {
    this.context = context
    this.caps = caps
    this.globalArgs = args
  }

  async onBotPrepare (props) {
    debug('Updated confidence threshold because of ConfidenceThreshold LogicHook')
  }
}
