const debug = require('debug')('botium-voip-logichook-ignore-silence-duration')

module.exports = class IgnoreSilenceDurationLogicHook {
  constructor (context, caps = {}, args = {}) {
    this.context = context
    this.caps = caps
    this.globalArgs = args
  }

  async onBotPrepare (props) {
    debug('Silence duration assertion ignored by logic hook')
  }
}
