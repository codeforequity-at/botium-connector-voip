const { v4: uuidv4 } = require('uuid')
const WebSocket = require('ws')
const _ = require('lodash')
const debug = require('debug')('botium-connector-voip')

const Capabilities = {
  VOIP_STT_URL_STREAM: 'VOIP_STT_URL_STREAM',
  VOIP_STT_PARAMS_STREAM: 'VOIP_STT_PARAMS_STREAM',
  VOIP_STT_METHOD_STREAM: 'VOIP_STT_METHOD_STREAM',
  VOIP_STT_BODY_STREAM: 'VOIP_STT_BODY_STREAM',
  VOIP_STT_HEADERS: 'VOIP_STT_HEADERS',
  VOIP_STT_TIMEOUT: 'VOIP_STT_TIMEOUT',
  VOIP_TTS_URL: 'VOIP_TTS_URL',
  VOIP_TTS_PARAMS: 'VOIP_TTS_PARAMS',
  VOIP_TTS_METHOD: 'VOIP_TTS_METHOD',
  VOIP_TTS_BODY: 'VOIP_TTS_BODY',
  VOIP_TTS_HEADERS: 'VOIP_TTS_HEADERS',
  VOIP_TTS_TIMEOUT: 'VOIP_TTS_TIMEOUT',
  VOIP_WORKER_URL: 'VOIP_WORKER_URL',
  VOIP_WORKER_APIKEY: 'VOIP_WORKER_APIKEY',
  VOIP_SIP_POOL_CALLER_ENABLE: 'VOIP_SIP_POOL_CALLER_ENABLE',
  VOIP_SIP_REG_HEADERS: 'VOIP_SIP_REG_HEADERS',
  VOIP_SIP_INVITE_HEADERS: 'VOIP_SIP_INVITE_HEADERS',
  VOIP_SIP_CALLEE_URI: 'VOIP_SIP_CALLEE_URI',
  VOIP_ICE_ENABLE: 'VOIP_ICE_ENABLE',
  VOIP_ICE_STUN_SERVERS: 'VOIP_ICE_STUN_SERVERS',
  VOIP_ICE_TURN_ENABLE: 'VOIP_ICE_TURN_ENABLE',
  VOIP_ICE_TURN_SERVER: 'VOIP_ICE_TURN_SERVER',
  VOIP_ICE_TURN_USER: 'VOIP_ICE_TURN_USER',
  VOIP_ICE_TURN_PASSWORD: 'VOIP_ICE_TURN_PASSWORD',
  VOIP_ICE_TURN_PROTOCOL: 'VOIP_ICE_TURN_PROTOCOL'
}

const Defaults = {
  VOIP_STT_METHOD: 'POST',
  VOIP_STT_TIMEOUT: 10000,
  VOIP_TTS_METHOD: 'GET',
  VOIP_TTS_TIMEOUT: 10000
}

class BotiumConnectorVoip {
  constructor ({ queueBotSays, caps }) {
    this.queueBotSays = queueBotSays
    this.caps = caps
  }

  Validate () {
    debug('Validate called')
    this.caps = Object.assign({}, Defaults, this.caps)
  }

  async Start () {
    debug('Start called')
    debug(this.caps[Capabilities.VOIP_TTS_URL])

    this.view = {
      container: this,
      context: {},
      msg: {},
      botium: {
        conversationId: uuidv4(),
        stepId: null
      }
    }

    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.caps[Capabilities.VOIP_WORKER_URL])

      if (!_.isArray(this.caps[Capabilities.VOIP_ICE_STUN_SERVERS])) {
        if (this.caps[Capabilities.VOIP_ICE_STUN_SERVERS] === '') {
          this.caps[Capabilities.VOIP_ICE_STUN_SERVERS] = []
        } else {
          this.caps[Capabilities.VOIP_ICE_STUN_SERVERS] = this.caps[Capabilities.VOIP_ICE_STUN_SERVERS].split(',')
        }
      }

      this.wsOpened = false
      this.ws.on('open', () => {
        this.wsOpened = true
        debug(`Websocket connection to ${this.caps[Capabilities.VOIP_WORKER_ENDPOINT]} opened.`)
        const request = {
          METHOD: 'initCall',
          API_KEY: this.caps[Capabilities.VOIP_WORKER_APIKEY],
          SIP_CALLER_AUTO: this.caps[Capabilities.VOIP_SIP_POOL_CALLER_ENABLE],
          SIP_CALLEE_URI: this.caps[Capabilities.VOIP_SIP_CALLEE_URI],
          SIP_REG_HEADERS: this.caps[Capabilities.VOIP_SIP_REG_HEADERS],
          SIP_INVITE_HEADERS: this.caps[Capabilities.VOIP_SIP_INVITE_HEADERS],
          ICE_ENABLE: this.caps[Capabilities.VOIP_ICE_ENABLE],
          ICE_STUN_SERVERS: this.caps[Capabilities.VOIP_ICE_STUN_SERVERS],
          ICE_TURN_SERVER: this.caps[Capabilities.VOIP_ICE_TURN_SERVER],
          ICE_TURN_USERNAME: this.caps[Capabilities.VOIP_ICE_TURN_USER],
          ICE_TURN_PASSWORD: this.caps[Capabilities.VOIP_ICE_TURN_PASSWORD],
          ICE_TURN_PROTOCOL: this.caps[Capabilities.VOIP_ICE_TURN_PROTOCOL] || 'TCP',
          STT_CONFIG: {
            stt_url: this.caps[Capabilities.VOIP_STT_URL_STREAM],
            stt_params: this.caps[Capabilities.VOIP_STT_PARAMS_STREAM],
            stt_body: this.caps[Capabilities.VOIP_STT_BODY_STREAM] || null
          },
          TTS_CONFIG: {
            tts_url: this.caps[Capabilities.VOIP_TTS_URL],
            tts_params: this.caps[Capabilities.VOIP_TTS_PARAMS],
            tts_body: this.caps[Capabilities.VOIP_TTS_BODY] || null
          }
        }
        debug(request)
        this.ws.send(JSON.stringify(request))
      })
      this.ws.on('close', () => {
        debug(`Websocket connection to ${this.caps[Capabilities.VOIP_WORKER_URL]} closed.`)
      })
      this.ws.on('error', (err) => {
        debug(err)
        if (!this.wsOpened) {
          reject(new Error(`Websocket connection to ${this.caps[Capabilities.VOIP_WORKER_URL]} error: ${err.message || err}`))
        }
      })
      this.ws.on('message', async (data) => {
        const parsedData = JSON.parse(data)
        const botMsgs = []

        debug(parsedData)

        if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'initialized') {
          this.sessionId = parsedData.voipConfig.sessionId
        }

        if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'forbidden') {
          reject(new Error('Cannot connect to VOIP Worker because of wrong APi key'))
        }

        if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'connected') {
          resolve()
        }

        if (parsedData && parsedData.data && parsedData.data.final) {
          const botMsg = { messageText: parsedData.data.message, sourceData: parsedData }
          botMsgs.push(botMsg)
        }

        botMsgs.forEach(botMsg => setTimeout(() => this.queueBotSays(botMsg), 0))
      })
    })
  }

  async UserSays (msg) {
    debug('UserSays called')

    debug(msg)

    setTimeout(() => {
      if (msg && msg.messageText) {
        const request = JSON.stringify({
          METHOD: 'sendTTS',
          sessionId: this.sessionId,
          message: msg.messageText
        })
        this.ws.send(request)
      }
      if (msg && msg.media && msg.media.length > 0 && msg.media[0].buffer) {
        msg.userInputs.forEach((userInput, index) => {
          const request = JSON.stringify({
            METHOD: 'sendAudio',
            PESQ: userInput.args.filter(a => a.includes(';PESQ')).length > 0,
            sessionId: this.sessionId,
            b64_buffer: msg.media[index].buffer.toString('base64')
          })
          this.ws.send(request)
        })
      }
    }, 500)
  }

  async Stop () {
    debug('Stop called')
    /* const request = JSON.stringify({
      METHOD: 'stopCall',
      sessionId: this.sessionId
    })
    this.ws.send(request)
    await new Promise(resolve => setTimeout(resolve, 2000))
    try {

    } catch (e) {
      debug('Cannot send stopCall method to VOIP-Worker')
    } */

    if (this.ws) {
      this.ws.close()
    }
    this.wsOpened = false
    this.ws = null
    this.view = {}
  }
}

module.exports = BotiumConnectorVoip
