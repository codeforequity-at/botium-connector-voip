const WebSocket = require('ws')
const _ = require('lodash')
const axios = require('axios')
const debug = require('debug')('botium-connector-voip')
const path = require('path')
const mm = require('music-metadata')

const Capabilities = {
  VOIP_STT_URL_STREAM: 'VOIP_STT_URL_STREAM',
  VOIP_STT_PARAMS_STREAM: 'VOIP_STT_PARAMS_STREAM',
  VOIP_STT_METHOD_STREAM: 'VOIP_STT_METHOD_STREAM',
  VOIP_STT_BODY_STREAM: 'VOIP_STT_BODY_STREAM',
  VOIP_STT_BODY: 'VOIP_STT_BODY',
  VOIP_STT_HEADERS: 'VOIP_STT_HEADERS',
  VOIP_STT_TIMEOUT: 'VOIP_STT_TIMEOUT',
  VOIP_STT_MESSAGE_HANDLING: 'VOIP_STT_MESSAGE_HANDLING',
  VOIP_STT_MESSAGE_HANDLING_TIMEOUT: 'VOIP_STT_MESSAGE_HANDLING_TIMEOUT',
  VOIP_STT_MESSAGE_HANDLING_DELIMITER: 'VOIP_STT_MESSAGE_HANDLING_DELIMITER',
  VOIP_STT_MESSAGE_HANDLING_PUNCTUATION: 'VOIP_STT_MESSAGE_HANDLING_PUNCTUATION',
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
  VOIP_SIP_PROTOCOL: 'VOIP_SIP_PROTOCOL',
  VOIP_SIP_REG_HEADERS: 'VOIP_SIP_REG_HEADERS',
  VOIP_SIP_INVITE_HEADERS: 'VOIP_SIP_INVITE_HEADERS',
  VOIP_SIP_CALLEE_URI: 'VOIP_SIP_CALLEE_URI',
  VOIP_ICE_ENABLE: 'VOIP_ICE_ENABLE',
  VOIP_ICE_STUN_SERVERS: 'VOIP_ICE_STUN_SERVERS',
  VOIP_ICE_TURN_ENABLE: 'VOIP_ICE_TURN_ENABLE',
  VOIP_ICE_TURN_SERVER: 'VOIP_ICE_TURN_SERVER',
  VOIP_ICE_TURN_USER: 'VOIP_ICE_TURN_USER',
  VOIP_ICE_TURN_PASSWORD: 'VOIP_ICE_TURN_PASSWORD',
  VOIP_ICE_TURN_PROTOCOL: 'VOIP_ICE_TURN_PROTOCOL',
  VOIP_WEBSOCKET_CONNECT_MAXRETRIES: 'VOIP_WEBSOCKET_CONNECT_MAXRETRIES',
  VOIP_WEBSOCKET_CONNECT_TIMEOUT: 'VOIP_WEBSOCKET_CONNECT_TIMEOUT',
  VOIP_SILENCE_DURATION_TIMEOUT_ENABLE: 'VOIP_SILENCE_DURATION_TIMEOUT_ENABLE',
  VOIP_SILENCE_DURATION_TIMEOUT: 'VOIP_SILENCE_DURATION_TIMEOUT',
  VOIP_SILENCE_DURATION_TIMEOUT_START_ENABLE: 'VOIP_SILENCE_DURATION_TIMEOUT_START_ENABLE',
  VOIP_SILENCE_DURATION_TIMEOUT_START: 'VOIP_SILENCE_DURATION_TIMEOUT_START',
  VOIP_STT_CONFIDENCE_THRESHOLD: 'VOIP_STT_CONFIDENCE_THRESHOLD',
  VOIP_USE_GLOBAL_VOIP_WORKER: 'VOIP_USE_GLOBAL_VOIP_WORKER'
}

const Defaults = {
  VOIP_STT_METHOD: 'POST',
  VOIP_STT_TIMEOUT: 10000,
  VOIP_TTS_METHOD: 'GET',
  VOIP_TTS_TIMEOUT: 10000,
  VOIP_STT_MESSAGE_HANDLING: 'ORIGINAL',
  VOIP_STT_MESSAGE_HANDLING_TIMEOUT: 2500,
  VOIP_STT_MESSAGE_HANDLING_DELIMITER: '. ',
  VOIP_STT_MESSAGE_HANDLING_PUNCTUATION: '.!?',
  VOIP_WEBSOCKET_CONNECT_TIMEOUT: 4000,
  VOIP_WEBSOCKET_CONNECT_MAXRETRIES: 5,
  VOIP_SILENCE_DURATION_TIMEOUT: 2500,
  VOIP_SILENCE_DURATION_TIMEOUT_ENABLE: false,
  VOIP_SILENCE_DURATION_TIMEOUT_START: 1000,
  VOIP_SILENCE_DURATION_TIMEOUT_START_ENABLE: false,
  VOIP_STT_CONFIDENCE_THRESHOLD: 0.5,
  VOIP_USE_GLOBAL_VOIP_WORKER: false,
  VOIP_SIP_PROTOCOL: 'TCP'
}

class BotiumConnectorVoip {
  constructor ({ queueBotSays, eventEmitter, caps }) {
    this.queueBotSays = queueBotSays
    this.caps = caps
    this.eventEmitter = eventEmitter
    this.botMsgs = []
    this.sentencesBuilding = 0
    this.sentencesFinal = 0
    this.sentenceBuilding = false
  }

  async Validate () {
    debug('Validate called')
    debug(this.caps.VOIP_STT_MESSAGE_HANDLING)

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

    this.stopCalled = false

    this.fullRecord = ''
    this.end = false
    this.connected = false
    this.convoStep = null

    const sendBotMsg = (botMsg) => { setTimeout(() => this.queueBotSays(botMsg), 0) }

    const joinBotMsg = (botMsgs, joinLastPrevMsg) => {
      const botMsg = {}
      botMsg.messageText = botMsgs.map(m => m.messageText).join(this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_DELIMITER] || '')
      botMsg.sourceData = botMsgs.map(m => m.sourceData)
      if (_.isNil(joinLastPrevMsg)) {
        botMsg.sourceData[0].silenceDuration = botMsgs[0].sourceData.data.start
        botMsg.sourceData[0].voiceDuration = botMsgs[botMsgs.length - 1].sourceData.data.end - botMsgs[0].sourceData.data.start
      } else {
        botMsg.sourceData[0].silenceDuration = botMsgs[0].sourceData.data.start - joinLastPrevMsg.sourceData.data.end
        botMsg.sourceData[0].voiceDuration = botMsgs[botMsgs.length - 1].sourceData.data.end - botMsgs[0].sourceData.data.start
      }
      return botMsg
    }

    const splitBotMsgs = botMsgs => {
      const splitSentences = s => s.match(new RegExp(`[^${this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_PUNCTUATION]}]+[${this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_PUNCTUATION]}]+`, 'g'))
      const botMsgsFinal = []
      for (const botMsg of botMsgs) {
        const sentences = splitSentences(botMsg.messageText)
        if (_.isNil(sentences)) {
          botMsgsFinal.push(botMsg)
        } else {
          let botMsgCount = 0
          for (const sentence of sentences) {
            botMsgsFinal.push({
              messageText: sentence,
              sourceData: (botMsgCount === 0) ? botMsg.sourceData : _.omit(botMsg.sourceData, ['silenceDuration'])
            })
            botMsgCount++
          }
        }
      }
      return botMsgsFinal
    }

    let data = null
    let headers = null
    const connect = async (retryIndex) => {
      retryIndex = retryIndex || 0
      try {
        const res = await axios({
          method: 'post',
          data: {
            API_KEY: this.caps[Capabilities.VOIP_USE_GLOBAL_VOIP_WORKER] ? process.env.BOTIUM_VOIP_WORKER_APIKEY : this.caps[Capabilities.VOIP_WORKER_APIKEY]
          },
          url: new URL(path.join(`${this.caps[Capabilities.VOIP_USE_GLOBAL_VOIP_WORKER] ? process.env.BOTIUM_VOIP_WORKER_URL : this.caps[Capabilities.VOIP_WORKER_URL].replace('wss', 'https').replace('ws', 'http')}`, 'initCall')).toString()
        })
        if (res) {
          data = res.data
          headers = res.headers
        }
      } catch (err) {
        debug(`Retry ${retryIndex} / ${this.caps[Capabilities.VOIP_WEBSOCKET_CONNECT_MAXRETRIES]}: Connecting to VOIP Worker failed: ${err.message || 'Not reachable'}`)
        if (retryIndex === this.caps[Capabilities.VOIP_WEBSOCKET_CONNECT_MAXRETRIES]) {
          throw new Error(`Connecting to VOIP Worker failed: ${err.message || 'Not reachable'}`)
        }
        // Retry after the defined timeout
        await new Promise(resolve => setTimeout(resolve, this.caps[Capabilities.VOIP_WEBSOCKET_CONNECT_TIMEOUT]))

        // Retry the connection
        await connect(retryIndex + 1)
      }
    }
    await connect()

    return new Promise((resolve, reject) => {
      const wsEndpoint = `${this.caps[Capabilities.VOIP_USE_GLOBAL_VOIP_WORKER] ? process.env.BOTIUM_VOIP_WORKER_URL : this.caps[Capabilities.VOIP_WORKER_URL]}/ws/${data.port}`
      const connect = (retryIndex) => {
        retryIndex = retryIndex || 0
        return new Promise((resolve, reject) => {
          if ('set-cookie' in headers) {
            this.ws = new WebSocket(wsEndpoint, {
              headers: {
                Cookie: headers['set-cookie']
              }
            })
          } else {
            this.ws = new WebSocket(wsEndpoint)
          }
          this.ws.on('open', () => {
            resolve()
          })
          this.ws.on('error', () => {
            if (retryIndex === this.caps[Capabilities.VOIP_WEBSOCKET_CONNECT_MAXRETRIES]) {
              reject(new Error(`Websocket connection failed to ${wsEndpoint}`))
            }
            setTimeout(() => connect(retryIndex + 1).then(resolve).catch(reject), this.caps[Capabilities.VOIP_WEBSOCKET_CONNECT_TIMEOUT])
          })
        })
      }
      connect().then(() => {
        if (!_.isArray(this.caps[Capabilities.VOIP_ICE_STUN_SERVERS])) {
          if (_.isEmpty(this.caps[Capabilities.VOIP_ICE_STUN_SERVERS])) {
            this.caps[Capabilities.VOIP_ICE_STUN_SERVERS] = []
          } else {
            this.caps[Capabilities.VOIP_ICE_STUN_SERVERS] = this.caps[Capabilities.VOIP_ICE_STUN_SERVERS].split(',')
          }
        }

        this.eventEmitter.on('CONVO_STEP_NEXT', (container, convoStep) => {
          this.convoStep = convoStep
        })

        this.silence = null
        this.msgCount = 0
        this.firstMsg = true
        this.firstSttInfoReceived = false
        this.silenceTimeout = null

        this.wsOpened = true
        debug(`Websocket connection to ${wsEndpoint} opened.`)
        const request = {
          METHOD: 'initCall',
          SIP_CALLER_AUTO: this.caps[Capabilities.VOIP_SIP_POOL_CALLER_ENABLE],
          SIP_PROXY: this.caps[Capabilities.VOIP_SIP_PROXY],
          SIP_PROTOCOL: this.caps[Capabilities.VOIP_SIP_PROTOCOL],
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
          MIN_SILENCE_DURATION: this.caps[Capabilities.VOIP_SILENCE_DURATION_TIMEOUT_ENABLE] ? this.caps[Capabilities.VOIP_SILENCE_DURATION_TIMEOUT] : null,
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

        debug(JSON.stringify(request, null, 2))
        this.ws.send(JSON.stringify(request))

        this.ws.on('error', (err) => {
          debug(err)
          if (!this.wsOpened) {
            this.end = true
            reject(new Error(`${this.sessionId} - Websocket connection to ${wsEndpoint} error: ${err.message || err}`))
          }
        })
        this.ws.on('message', async (data) => {
          const parsedData = JSON.parse(data)

          const parsedDataLog = _.cloneDeep(parsedData)
          parsedDataLog.fullRecord = '<full_record_buffer>'

          // debug(JSON.stringify(parsedDataLog, null, 2))

          if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'initialized') {
            this.sessionId = parsedData.voipConfig.sessionId
          }

          if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'unauthorized') {
            reject(new Error('Error: Cannot open a call: SIP Authorization failed'))
          }

          if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'forbidden' && parsedData.event === 'onCallRegState') {
            reject(new Error('Error: Sip Registration failed'))
          }

          if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'connected') {
            resolve()
          }

          if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'disconnected') {
            const apiKey = this._extractApiKey(this._getBody(Capabilities.VOIP_STT_BODY))
            if (parsedData.connectDuration && parsedData.connectDuration > 0) {
              this.eventEmitter.emit('CONSUMPTION_METADATA', this, {
                type: _.isNil(apiKey) ? 'INBUILT' : 'THIRD_PARTY',
                category: 'e2eVoiceIvr',
                metricName: 'consumption.e2e.voip.stt.seconds',
                transactions: parsedData.connectDuration,
                apiKey
              })
            }
          }

          if (parsedData && parsedData.type === 'error') {
            this.end = true
            reject(new Error(`Error: ${parsedData.message}`))
            sendBotMsg(new Error(`Error: ${parsedData.message}`))
          }

          if (parsedData && parsedData.type === 'silence') {
            if (_.isNil(this._getIgnoreSilenceDurationAsserterLogicHook(this.convoStep))) {
              if (_.isNil(this._getJoinLogicHook(this.convoStep)) && parsedData.data.silence.length > 0) {
                this.end = true
                sendBotMsg(new Error(`Silence Duration of ${parsedData.data.silence[0][2].toFixed(2)}s exceeded General Silence Duration Timeout of ${this.caps[Capabilities.VOIP_SILENCE_DURATION_TIMEOUT] / 1000}s`))
              }
            }
          }

          if (parsedData && parsedData.type === 'fullRecord') {
            this.eventEmitter.emit('MESSAGE_ATTACHMENT', this.container, {
              name: 'full_record.wav',
              mimeType: 'audio/wav',
              base64: parsedData.fullRecord
            })
            this.end = true
          }

          if (parsedData && parsedData.data && parsedData.data.final === false) {
            if (this.silenceTimeout) {
              clearTimeout(this.silenceTimeout)
            }
            if (_.isNil(this._getIgnoreSilenceDurationAsserterLogicHook(this.convoStep))) {
              if (!this.firstSttInfoReceived && _.isNil(this._getJoinLogicHook(this.convoStep)) && this.caps[Capabilities.VOIP_SILENCE_DURATION_TIMEOUT_START_ENABLE] && parsedData.data.start && parsedData.data.start * 1000 > this.caps[Capabilities.VOIP_SILENCE_DURATION_TIMEOUT_START]) {
                this.end = true
                sendBotMsg(new Error(`Silence Duration of ${parsedData.data.start}s exceeded Initial Silence Duration Timeout of ${this.caps[Capabilities.VOIP_SILENCE_DURATION_TIMEOUT_START] / 1000}s`))
              }
            }
            this.firstSttInfoReceived = true
            if (this.prevData) {
              if (!_.isNil(parsedData.data.start)) {
                const silenceDuration = parsedData.data.start - this.prevData.data.end
                const joinLogicHook = this._getJoinLogicHook(this.convoStep)
                const isJoinMethod = this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'JOIN'
                let matched = false
                if ((!_.isNil(joinLogicHook) && isJoinMethod && silenceDuration > parseInt(joinLogicHook.args[0])) / 1000) {
                  matched = true
                } else if ((_.isNil(joinLogicHook) && isJoinMethod && (silenceDuration > parseInt(this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_TIMEOUT]) / 1000))) {
                  matched = true
                }
                if (matched && this.botMsgs.length > 0) {
                  sendBotMsg(joinBotMsg(this.botMsgs, this.joinLastPrevMsg))
                  this.firstMsg = false
                  this.joinLastPrevMsg = this.botMsgs[this.botMsgs.length - 1]
                  this.botMsgs = []
                }
              }
            }
          }

          if (parsedData && parsedData.data && parsedData.data.type === 'stt' && parsedData.data.final) {
            const confidenceThreshold = ((this._getConfidenceScoreLogicHook(this.convoStep) && this._getConfidenceScoreLogicHook(this.convoStep).args[0]) || this.caps[Capabilities.VOIP_STT_CONFIDENCE_THRESHOLD])
            const successfulConfidenceScore = this._getConfidenceScore(parsedData) >= confidenceThreshold
            debug(`Message: ${parsedData.data.message} / Confidence Score: ${this._getConfidenceScore(parsedData)} (Threshold: ${confidenceThreshold})`)
            if (this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'ORIGINAL' && (_.isNil(this._getJoinLogicHook(this.convoStep)))) {
              let botMsg = { messageText: parsedData.data.message }
              if (this.firstMsg) {
                const sourceData = parsedData
                sourceData.silenceDuration = parsedData.data.start
                sourceData.voiceDuration = parsedData.data.end - parsedData.data.start
                botMsg = Object.assign({}, botMsg, { sourceData })
                this.firstMsg = false
              } else {
                const sourceData = parsedData
                sourceData.silenceDuration = parsedData.data.start - this.prevData.data.end
                sourceData.voiceDuration = parsedData.data.end - parsedData.data.start
                botMsg = Object.assign({}, botMsg, { sourceData })
              }
              this.prevData = parsedData
              if (successfulConfidenceScore) {
                this.botMsgs.push(botMsg)
                this.botMsgs.forEach(botMsg => sendBotMsg(botMsg))
              }
              this.botMsgs = []
            }
            if (this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'SPLIT' || this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'EXPAND') {
              let botMsg = { messageText: parsedData.data.message }
              if (this.firstMsg) {
                const sourceData = parsedData
                sourceData.silenceDuration = parsedData.data.start
                sourceData.voiceDuration = parsedData.data.end - parsedData.data.start
                botMsg = Object.assign({}, botMsg, { sourceData })
                this.firstMsg = false
              } else {
                const sourceData = parsedData
                sourceData.silenceDuration = parsedData.data.start - this.prevData.data.end
                sourceData.voiceDuration = parsedData.data.end - parsedData.data.start
                botMsg = Object.assign({}, botMsg, { sourceData })
              }
              this.prevData = parsedData
              if (successfulConfidenceScore) {
                this.botMsgs.push(botMsg)
                const botMsgsExpanded = splitBotMsgs(this.botMsgs)
                botMsgsExpanded.forEach(botMsg => sendBotMsg(botMsg))
              }
              this.botMsgs = []
            }
            if (this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'JOIN' || this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'CONCAT' || !_.isNil(this._getJoinLogicHook(this.convoStep))) {
              const botMsg = { messageText: parsedData.data.message, sourceData: parsedData }
              this.prevData = parsedData
              if (successfulConfidenceScore) {
                this.botMsgs.push(botMsg)
                this.silenceTimeout = setTimeout(() => {
                  if (this.botMsgs.length > 0) {
                    debug('Silence Duration Timeout (PSST):', (this._getJoinLogicHook(this.convoStep) && parseInt(this._getJoinLogicHook(this.convoStep).args[0])) || this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_TIMEOUT], 'ms')
                    sendBotMsg(joinBotMsg(this.botMsgs, this.joinLastPrevMsg))
                    this.firstMsg = false
                    this.joinLastPrevMsg = this.botMsgs[this.botMsgs.length - 1]
                    this.botMsgs = []
                  }
                }, (this._getJoinLogicHook(this.convoStep) && parseInt(this._getJoinLogicHook(this.convoStep).args[0])) || this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_TIMEOUT])
              }
            }
          }
        })
      }).catch(err => {
        reject(new Error('Error: ' + err))
      })
    })
  }

  async UserSays (msg) {
    debug('UserSays called')

    debug(msg)

    if (!msg.attachments) {
      msg.attachments = []
    }
    return new Promise((resolve, reject) => {
      setTimeout(async () => {
        let duration = 0
        if (msg && msg.buttons && msg.buttons.length > 0) {
          const request = JSON.stringify({
            METHOD: 'sendDtmf',
            digits: msg.buttons[0].payload,
            sessionId: this.sessionId
          })
          this.ws.send(request)
        } else if (msg && msg.messageText) {
          if (!this.axiosTtsParams) reject(new Error('TTS not configured, only audio input supported'))
          if (this.axiosTtsParams) {
            const ttsRequest = {
              ...this.axiosTtsParams,
              params: {
                ...(this.axiosTtsParams.params || {}),
                text: msg.messageText
              },
              data: this._getBody(Capabilities.VOIP_TTS_BODY),
              responseType: 'arraybuffer'
            }
            msg.sourceData = ttsRequest

            let ttsResponse = null
            try {
              ttsResponse = await axios(ttsRequest)
            } catch (err) {
              reject(new Error(`TTS "${msg.messageText}" failed - ${this._getAxiosErrOutput(err)}`))
            }
            if (msg && msg.messageText && msg.messageText.length > 0) {
              const apiKey = this._extractApiKey(this._getBody(Capabilities.VOIP_TTS_BODY))
              this.eventEmitter.emit('CONSUMPTION_METADATA', this.container, {
                type: _.isNil(apiKey) ? 'INBUILT' : 'THIRD_PARTY',
                metricName: 'consumption.e2e.voip.tts.characters',
                transactions: msg.messageText.length,
                apiKey
              })
            }
            if (Buffer.isBuffer(ttsResponse.data)) {
              duration = ttsResponse.headers['content-duration']
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
              reject(new Error(`TTS failed, response is: ${this._getAxiosShortenedOutput(ttsResponse.data)}`))
            }
          }
        }
        if (msg && msg.media && msg.media.length > 0 && msg.media[0].buffer) {
          const request = JSON.stringify({
            METHOD: 'sendAudio',
            sessionId: this.sessionId,
            b64_buffer: msg.media[0].buffer.toString('base64')
          })
          this.ws.send(request)
          msg.attachments.push({
            name: msg.media[0].mediaUri,
            mimeType: msg.media[0].mimeType,
            base64: msg.media[0].buffer.toString('base64')
          })

          try {
            const metadata = await mm.parseBuffer(msg.media[0].buffer)
            if (metadata && metadata.format && metadata.format.duration) {
              debug('Audio duration of user audio:', metadata.format.duration, 'seconds')
              duration = Math.round(metadata.format.duration)
            } else {
              reject(new Error('Could not determine audio duration from metadata'))
            }
          } catch (err) {
            reject(new Error(`Getting audio duration failed: ${err.message}`))
          }
        }
        setTimeout(resolve, duration * 1000)
      }, 0)
    })
  }

  async Stop () {
    debug(`${this.sessionId} - Stop called`)
    if (this.ws && this.ws.readyState !== WebSocket.CLOSED) {
      const request = JSON.stringify({
        METHOD: 'stopCall',
        sessionId: this.sessionId
      })
      this.ws.send(request)
      await new Promise(resolve => {
        setTimeout(resolve, 100000)
        setInterval(() => {
          if (this.end) {
            this.wsOpened = false
            this.ws = null
            this.end = false
            this.convoStep = null
            this.firstSttInfoReceived = false
            resolve()
          }
        }, 100)
      })
    } else {
      this.wsOpened = false
      this.ws = null
      this.end = false
      this.convoStep = null
      this.firstSttInfoReceived = false
    }
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

  _extractApiKey (body) {
    return _.get(body, 'polly.credentials.accessKeyId') ||
      _.get(body, 'google.credentials.client_email') ||
      _.get(body, 'ibm.credentials.apikey') ||
      _.get(body, 'awstranscribe.credentials.accessKeyId') ||
      _.get(body, 'azure.credentials.subscriptionKey') ||
      null
  }

  _getJoinLogicHook (convoStep) {
    if (_.isNil(convoStep)) return null
    if (_.isNil(convoStep.logicHooks)) return null
    return convoStep && convoStep.logicHooks && convoStep.logicHooks.find(lh => lh.name === 'VOIP_JOIN_SILENCE_DURATION')
  }

  _getIgnoreSilenceDurationAsserterLogicHook (convoStep) {
    if (_.isNil(convoStep)) return null
    if (_.isNil(convoStep.logicHooks)) return null
    return convoStep && convoStep.logicHooks && convoStep.logicHooks.find(lh => lh.name === 'VOIP_IGNORE_SILENCE_DURATION')
  }

  _getConfidenceScoreLogicHook (convoStep) {
    if (_.isNil(convoStep)) return null
    if (_.isNil(convoStep.logicHooks)) return null
    return convoStep && convoStep.logicHooks && convoStep.logicHooks.find(lh => lh.name === 'VOIP_CONFIDENCE_THRESHOLD')
  }

  _getConfidenceScore (parsedData) {
    // Azure Speech Service
    if (parsedData?.data?.source?.debug?.privJson) {
      const privJson = JSON.parse(parsedData?.data?.source?.debug?.privJson)
      if (privJson && privJson.NBest && privJson.NBest.length > 0) {
        return Math.max(...privJson.NBest.map(n => n.Confidence))
      }
    }
    return null
  }
}

module.exports = BotiumConnectorVoip
