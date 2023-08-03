const debug = require('debug')('botium-voip-logichook-join-silence-duration')

module.exports = class JoinLogicHook {
  constructor (context, caps = {}, args = {}) {
    this.context = context
    this.caps = caps
    this.globalArgs = args
  }

  async onBotPrepare (props) {
    debug('Joined bot messages because of JOIN logic hook')
  }
}
