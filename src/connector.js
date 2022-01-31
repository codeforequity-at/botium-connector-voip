const { v4: uuidv4 } = require('uuid')
const WebSocket = require('ws')
const _ = require('lodash')
const axios = require('axios')
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
  VOIP_SIP_CALLER_REGISTRAR_URI: 'VOIP_SIP_CALLER_REGISTRAR_URI',
  VOIP_SIP_CALLER_ADDRESS: 'VOIP_SIP_CALLER_ADDRESS',
  VOIP_SIP_CALLER_USERNAME: 'VOIP_SIP_CALLER_USERNAME',
  VOIP_SIP_CALLER_PASSWORD: 'VOIP_SIP_CALLER_PASSWORD',
  VOIP_SIP_PROXY: 'VOIP_SIP_PROXY',
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
  constructor ({ queueBotSays, eventEmitter, caps }) {
    this.queueBotSays = queueBotSays
    this.caps = caps
    this.eventEmitter = eventEmitter
  }

  async Validate () {
    debug('Validate called')

    if (this.caps.VOIP_TTS_URL) {
      this.axiosTtsParams = {
        url: this.caps.VOIP_TTS_URL,
        params: this._getParams(Capabilities.VOIP_TTS_PARAMS),
        method: this.caps.VOIP_TTS_METHOD,
        timeout: this.caps.VOIP_TTS_TIMEOUT,
        headers: this._getHeaders(Capabilities.VOIP_TTS_HEADERS)
      }
      try {
        const { data } = await axios({
          ...this.axiosTtsParams,
          url: this._getAxiosUrl(this.caps.VOIP_TTS_URL, '/api/status')
        })
        if (data && data.status === 'OK') {
          debug(`Checking TTS Status response: ${this._getAxiosShortenedOutput(data)}`)
        } else {
          throw new Error(`Checking TTS Status failed, response is: ${this._getAxiosShortenedOutput(data)}`)
        }
      } catch (err) {
        throw new Error(`Checking TTS Status failed - ${this._getAxiosErrOutput(err)}`)
      }
    }

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

    this.fullRecord = ''
    this.end = false

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
          SIP_PROXY: this.caps[Capabilities.VOIP_SIP_PROXY],
          SIP_CALLER_URI: this.caps[Capabilities.VOIP_SIP_CALLER_URI],
          SIP_CALLER_USERNAME: this.caps[Capabilities.VOIP_SIP_CALLER_USERNAME],
          SIP_CALLER_PASSWORD: this.caps[Capabilities.VOIP_SIP_CALLER_PASSWORD],
          SIP_CALLER_ADDRESS: this.caps[Capabilities.VOIP_SIP_CALLER_ADDRESS],
          SIP_CALLER_REGISTRAR_URI: this.caps[Capabilities.VOIP_SIP_CALLER_REGISTRAR_URI],
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
      this.ws.on('close', async () => {
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

        if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'unauthorized') {
          reject(new Error('Cannot open a call: SIP Authorization failed'))
        }

        if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'forbidden') {
          reject(new Error('Cannot connect to VOIP Worker because of wrong API key'))
        }

        if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'forbidden') {
          debug('error')
          reject(new Error('Cannot connect to VOIP Worker because of wrong API key'))
        }

        if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'connected') {
          resolve()
        }

        if (parsedData && parsedData.type === 'error') {
          reject(new Error(parsedData.message))
        }

        if (parsedData && parsedData.type === 'fullRecord') {
          this.end = true
          this.eventEmitter.emit('MESSAGE_ATTACHMENT', this.container, {
            name: 'full_record.wav',
            mimeType: 'audio/wav',
            base64: parsedData.fullRecord
          })
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

    if (!msg.attachments) {
      msg.attachments = []
    }
    setTimeout(async () => {
      if (msg && msg.buttons && msg.buttons.length > 0) {
        const request = JSON.stringify({
          METHOD: 'sendDtmf',
          digits: msg.buttons[0].payload,
          sessionId: this.sessionId
        })
        this.ws.send(request)
      } else if (msg && msg.messageText) {
        if (!this.axiosTtsParams) throw new Error('TTS not configured, only audio input supported')

        const ttsRequest = {
          ...this.axiosTtsParams,
          params: {
            ...(this.axiosTtsParams.params || {}),
            text: msg.messageText
          },
          data: this._getBody(Capabilities.BSP_TTS_BODY),
          responseType: 'arraybuffer'
        }
        msg.sourceData = ttsRequest

        let ttsResponse = null
        try {
          ttsResponse = await axios(ttsRequest)
        } catch (err) {
          throw new Error(`TTS "${msg.messageText}" failed - ${this._getAxiosErrOutput(err)}`)
        }
        if (Buffer.isBuffer(ttsResponse.data)) {
          const request = JSON.stringify({
            METHOD: 'sendAudio',
            PESQ: false,
            sessionId: this.sessionId,
            b64_buffer: ttsResponse.data.toString('base64')
          })
          msg.attachments.push({
            name: 'tts.wav',
            mimeType: 'audio/wav',
            base64: ttsResponse.data.toString('base64')
          })
          this.ws.send(request)
        } else {
          throw new Error(`TTS failed, response is: ${this._getAxiosShortenedOutput(ttsResponse.data)}`)
        }
      }
      if (msg && msg.media && msg.media.length > 0 && msg.media[0].buffer) {
        msg.userInputs.forEach((userInput, index) => {
          const request = JSON.stringify({
            METHOD: 'sendAudio',
            PESQ: userInput.args.filter(a => a.includes(';PESQ')).length > 0,
            sessionId: this.sessionId,
            b64_buffer: msg.media[index].buffer.toString('base64')
          })
          msg.attachments.push({
            name: msg.media[index].mediaUri,
            mimeType: msg.media[index].mimeType,
            base64: msg.media[index].buffer.toString('base64')
          })
          this.ws.send(request)
        })
      }
    }, 500)
  }

  async Stop () {
    debug('Stop called')
    const request = JSON.stringify({
      METHOD: 'stopCall',
      sessionId: this.sessionId
    })
    this.ws.send(request)
    await new Promise(resolve => {
      setTimeout(resolve, 50000)
      setInterval(() => {
        if (this.end) {
          resolve()
        }
      }, 1000)
    })

    if (this.ws) {
      this.ws.close()
    }
    this.wsOpened = false
    this.ws = null
    this.view = {}
  }

  _getParams (capParams) {
    if (this.caps[capParams]) {
      if (_.isString(this.caps[capParams])) return JSON.parse(this.caps[capParams])
      else return this.caps[capParams]
    }
    return {}
  }

  _getBody (capBody) {
    if (this.caps[capBody]) {
      if (_.isString(this.caps[capBody])) return JSON.parse(this.caps[capBody])
      else return this.caps[capBody]
    }
    return null
  }

  _getHeaders (capHeaders) {
    if (this.caps[capHeaders]) {
      if (_.isString(this.caps[capHeaders])) return JSON.parse(this.caps[capHeaders])
      else return this.caps[capHeaders]
    }
    return {}
  }

  _getAxiosUrl (baseUrl, extUrl) {
    return baseUrl.substr(0, baseUrl.indexOf('/', 8)) + extUrl
  }

  _getAxiosShortenedOutput (data) {
    if (data) {
      if (_.isBuffer(data)) {
        try {
          data = data.toString()
        } catch (err) {
        }
      }
      return _.truncate(_.isString(data) ? data : JSON.stringify(data), { length: 200 })
    } else {
      return ''
    }
  }

  _getAxiosErrOutput (err) {
    if (err && err.response) {
      return `Status: ${err.response.status} / Response: ${this._getAxiosShortenedOutput(err.response.data)}`
    } else {
      return err.message
    }
  }
}

module.exports = BotiumConnectorVoip
