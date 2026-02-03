const WebSocket = require('ws')
const _ = require('lodash')
const axios = require('axios')
const http = require('http')
const https = require('https')
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
  VOIP_TTS_CACHE_ENABLE: 'VOIP_TTS_CACHE_ENABLE',
  VOIP_TTS_CACHE_SIZE: 'VOIP_TTS_CACHE_SIZE',
  VOIP_TTS_PREFETCH_ENABLE: 'VOIP_TTS_PREFETCH_ENABLE',
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
  VOIP_TTS_CACHE_ENABLE: true,
  VOIP_TTS_CACHE_SIZE: 50,
  VOIP_TTS_PREFETCH_ENABLE: true,
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

const TTS_HTTP_AGENT = new http.Agent({ keepAlive: true })
const TTS_HTTPS_AGENT = new https.Agent({ keepAlive: true })

class BotiumConnectorVoip {
  constructor ({ queueBotSays, eventEmitter, caps }) {
    this.queueBotSays = queueBotSays
    this.caps = caps
    this.eventEmitter = eventEmitter
    this.botMsgs = []
    this.sentencesBuilding = 0
    this.sentencesFinal = 0
    this.sentenceBuilding = false
    this.ttsCache = new Map()
    this.ttsCacheEnabled = false
    this.ttsCacheMaxEntries = 0
    this.ttsPrefetchEnabled = false

    // For debugging latency between incoming STT (bot says) and outgoing audio (sendAudio)
    this._lastBotSaysQueuedAt = null
    this._lastBotSaysText = null
  }

  async Validate () {
    debug('Validate called')
    this.caps = Object.assign({}, Defaults, this.caps)
    debug(this.caps.VOIP_STT_MESSAGE_HANDLING)

    if (this.caps.VOIP_TTS_URL) {
      this.axiosTtsParams = {
        url: this.caps.VOIP_TTS_URL,
        params: this._getParams(Capabilities.VOIP_TTS_PARAMS),
        method: this.caps.VOIP_TTS_METHOD,
        timeout: this.caps.VOIP_TTS_TIMEOUT,
        headers: this._getHeaders(Capabilities.VOIP_TTS_HEADERS),
        httpAgent: TTS_HTTP_AGENT,
        httpsAgent: TTS_HTTPS_AGENT
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
    const cacheSize = parseInt(this.caps[Capabilities.VOIP_TTS_CACHE_SIZE], 10)
    this.ttsCacheEnabled = !!this.caps[Capabilities.VOIP_TTS_CACHE_ENABLE]
    this.ttsCacheMaxEntries = _.isFinite(cacheSize) && cacheSize > 0 ? cacheSize : 0
    this.ttsPrefetchEnabled = !!this.caps[Capabilities.VOIP_TTS_PREFETCH_ENABLE]
  }

  async Start () {
    debug('Start called')
    debug(this.caps[Capabilities.VOIP_TTS_URL])

    this.stopCalled = false

    this.fullRecord = ''
    this.end = false
    this.connected = false
    this.convoStep = null
    this._lastBotSaysQueuedAt = null
    this._lastBotSaysText = null
    if (this.ttsCache) {
      this.ttsCache.clear()
    }

    const sendBotMsg = (botMsg) => {
      this._lastBotSaysQueuedAt = Date.now()
      this._lastBotSaysText = botMsg && botMsg.messageText ? String(botMsg.messageText) : null
      setTimeout(() => this.queueBotSays(botMsg), 0)
    }

    const joinBotMsg = (botMsgs, joinLastPrevMsg) => {
      const botMsg = {}
      botMsg.messageText = botMsgs.map(m => m.messageText).join(this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_DELIMITER] || '')
      botMsg.sourceData = botMsgs.map(m => m.sourceData)
      const firstStart = _.get(botMsgs, '[0].sourceData.data.start', null)
      const lastEnd = _.get(botMsgs, `[${botMsgs.length - 1}].sourceData.data.end`, null)
      const prevEnd = _.get(joinLastPrevMsg, 'sourceData.data.end', null)
      // joinLastPrevMsg can be null (first joined message) OR incomplete (in case JOIN timeout fired before we got any final STT info).
      // In those cases, fall back to "start-of-first-chunk" semantics (same as previous behaviour for nil joinLastPrevMsg).
      if (_.isFinite(firstStart) && _.isFinite(lastEnd)) {
        botMsg.sourceData[0].silenceDuration = (_.isFinite(prevEnd) ? (firstStart - prevEnd) : firstStart)
        botMsg.sourceData[0].voiceDuration = lastEnd - firstStart
      } else {
        botMsg.sourceData[0].silenceDuration = _.isFinite(firstStart) ? firstStart : null
        botMsg.sourceData[0].voiceDuration = (_.isFinite(firstStart) && _.isFinite(lastEnd)) ? (lastEnd - firstStart) : null
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
          this._maybePrefetchTts(convoStep)
          // For PSST: send join silence duration per step to VOIP worker (controls PSST silence trigger)
          try {
            if (this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'PSST' && this.ws && this.ws.readyState === WebSocket.OPEN) {
              const joinLogicHook = this._getJoinLogicHook(convoStep)
              let silenceMs = null
              if (joinLogicHook && joinLogicHook.args && joinLogicHook.args.length > 0) {
                silenceMs = parseInt(joinLogicHook.args[0], 10)
              }
              // Fallback to global timeout if no per-step hook is set
              if (!_.isFinite(silenceMs) || silenceMs <= 0) {
                silenceMs = parseInt(this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_TIMEOUT], 10)
              }
              // PSST: treat like JOIN, but subtract 500ms from timeout
              if (_.isFinite(silenceMs) && silenceMs > 0) {
                silenceMs = Math.max(0, silenceMs - 500)
              }
              if (_.isFinite(silenceMs) && silenceMs > 0 && this.sessionId) {
                debug(`PSST: sending silenceDurationMs=${silenceMs} for sessionId=${this.sessionId}`)
                this.ws.send(JSON.stringify({
                  METHOD: 'setSttSilenceDuration',
                  sessionId: this.sessionId,
                  silenceDurationMs: silenceMs
                }))
              }
            }
          } catch (err) {
            debug(`Failed sending PSST silence duration to VOIP worker: ${err.message || err}`)
          }
        })

        this.silence = null
        this.msgCount = 0
        this.firstMsg = true
        this.firstSttInfoReceived = false
        this.silenceTimeout = null

        this.wsOpened = true
        debug(`Websocket connection to ${wsEndpoint} opened.`)
        const sttHandling = this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING]
        const isPsst = sttHandling === 'PSST'
        const sttLegacy = true
        let sttUrl = this.caps[Capabilities.VOIP_STT_URL_STREAM]
        if (isPsst && _.isString(sttUrl)) {
          const replaced = sttUrl.replace('/api/sttstream/', '/api/stt/')
          if (replaced !== sttUrl) {
            sttUrl = replaced
          } else {
            debug(`PSST: Could not derive /api/stt url from ${sttUrl}, using as-is`)
          }
        }
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
          STT_LEGACY: sttLegacy,
          STT_CONFIG: {
            stt_url: sttUrl,
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
          // Allow fullRecord delivery even if Stop() was already called.
          // Otherwise the recording can be dropped if the worker streams it late (common on hangup).
          if (this.stopCalled) {
            const allowedTypes = ['fullRecord', 'fullRecordStart', 'fullRecordChunk', 'fullRecordEnd', 'error']
            if (!parsedData || !parsedData.type || !allowedTypes.includes(parsedData.type)) {
              debug(`${this.sessionId} - Stop already called, ignoring incoming message`)
              return
            }
          }

          const parsedDataLog = _.cloneDeep(parsedData)
          // Sanitize all potential base64 fields for logging (don't dump huge buffers)
          const sanitizeBase64Fields = (obj, prefix = '') => {
            if (!obj || typeof obj !== 'object') return
            for (const key of Object.keys(obj)) {
              const val = obj[key]
              if (typeof val === 'string' && val.length > 500) {
                obj[key] = `<base64:${val.length}chars>`
              } else if (val && typeof val === 'object' && !Array.isArray(val)) {
                sanitizeBase64Fields(val, `${prefix}${key}.`)
              }
            }
          }
          sanitizeBase64Fields(parsedDataLog)

          debug(JSON.stringify(parsedDataLog, null, 2))

          const _extractFullRecordBase64 = (pd) => {
            if (!pd) return null
            // Different VOIP workers may put the payload in various fields - search all string fields
            // First try known field names
            const knownFields = [
              pd.fullRecord,
              pd.full_record,
              pd.data?.fullRecord,
              pd.data?.full_record,
              pd.data?.b64_buffer,
              pd.data?.base64,
              pd.data?.full_record_buffer,
              pd.data?.buffer,
              pd.buffer,
              pd.base64,
              pd.audio,
              pd.audioData,
              pd.data?.audio,
              pd.data?.audioData
            ]
            for (const val of knownFields) {
              if (typeof val === 'string' && val.length > 100) return val
            }
            // Fallback: find any large string field (likely base64)
            const findLargeString = (obj, depth = 0) => {
              if (!obj || typeof obj !== 'object' || depth > 3) return null
              for (const key of Object.keys(obj)) {
                const val = obj[key]
                if (typeof val === 'string' && val.length > 1000) {
                  debug(`${this.sessionId} - Found base64 in field '${key}' (len=${val.length})`)
                  return val
                }
                if (val && typeof val === 'object' && !Array.isArray(val)) {
                  const found = findLargeString(val, depth + 1)
                  if (found) return found
                }
              }
              return null
            }
            return findLargeString(pd)
          }

          const emitFullRecordAttachment = (base64) => {
            if (!base64 || typeof base64 !== 'string' || base64.length === 0) {
              // Log available fields (length only) to help diagnose worker payload shape without dumping base64
              try {
                const candidates = {
                  fullRecord: typeof parsedData?.fullRecord === 'string' ? parsedData.fullRecord.length : null,
                  full_record: typeof parsedData?.full_record === 'string' ? parsedData.full_record.length : null,
                  data_fullRecord: typeof parsedData?.data?.fullRecord === 'string' ? parsedData.data.fullRecord.length : null,
                  data_full_record: typeof parsedData?.data?.full_record === 'string' ? parsedData.data.full_record.length : null,
                  data_b64_buffer: typeof parsedData?.data?.b64_buffer === 'string' ? parsedData.data.b64_buffer.length : null,
                  data_base64: typeof parsedData?.data?.base64 === 'string' ? parsedData.data.base64.length : null,
                  data_full_record_buffer: typeof parsedData?.data?.full_record_buffer === 'string' ? parsedData.data.full_record_buffer.length : null,
                  data_buffer: typeof parsedData?.data?.buffer === 'string' ? parsedData.data.buffer.length : null
                }
                debug(`${this.sessionId} - fullRecordEnd received but base64 is empty, skipping attachment emit. CandidateLengths=${JSON.stringify(candidates)}`)
              } catch (err) {
                debug(`${this.sessionId} - fullRecordEnd received but base64 is empty, skipping attachment emit (diag failed: ${err.message || err})`)
              }
              return
            }
            debug(`${this.sessionId} - Emitting MESSAGE_ATTACHMENT full_record.wav (base64Len=${base64.length}, stopCalled=${!!this.stopCalled})`)
            this.eventEmitter.emit('MESSAGE_ATTACHMENT', this.container, {
              name: 'full_record.wav',
              mimeType: 'audio/wav',
              base64,
              // Session context from capabilities for correlation
              sessionContext: {
                testSessionId: this.caps.VOIP_TEST_SESSION_ID || null,
                testSessionJobId: this.caps.VOIP_TEST_SESSION_JOB_ID || null
              }
            })
          }

          if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'initialized') {
            this.sessionId = parsedData.voipConfig.sessionId
          }

          // if sessionId is not the same as the one in the callinfo, return
          if (parsedData && parsedData.voipConfig && parsedData.voipConfig.sessionId && parsedData.voipConfig.sessionId !== this.sessionId) {
            debug('sessionId mismatch, returning')
            return
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

          // Full record streaming support:
          // - some VOIP workers send the recording in chunks and an end marker
          if (parsedData && parsedData.type === 'fullRecordStart') {
            this.fullRecord = ''
          }
          if (parsedData && parsedData.type === 'fullRecordChunk') {
            const chunk = _extractFullRecordBase64(parsedData)
            if (chunk) this.fullRecord = (this.fullRecord || '') + chunk
          }
          if (parsedData && parsedData.type === 'fullRecordEnd') {
            const tail = _extractFullRecordBase64(parsedData)
            if (tail) this.fullRecord = (this.fullRecord || '') + tail
            emitFullRecordAttachment(this.fullRecord || tail)
            this.end = true
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
            // Non-chunked full record
            emitFullRecordAttachment(_extractFullRecordBase64(parsedData))
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
            if (this.prevData && this.prevData.data && !_.isNil(this.prevData.data.end)) {
              if (!_.isNil(parsedData.data.start)) {
                const silenceDuration = parsedData.data.start - this.prevData.data.end
                const joinLogicHook = this._getJoinLogicHook(this.convoStep)
                const sttHandling = this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING]
                const isPsst = sttHandling === 'PSST'
                const isJoinMethod = sttHandling === 'JOIN' || isPsst
                const toJoinTimeoutMs = (ms) => {
                  const parsed = parseInt(ms, 10)
                  if (!_.isFinite(parsed) || parsed <= 0) return null
                  return isPsst ? Math.max(0, parsed - 500) : parsed
                }
                let matched = false
                if (!_.isNil(joinLogicHook) && isJoinMethod) {
                  const joinTimeoutMs = toJoinTimeoutMs(joinLogicHook && joinLogicHook.args && joinLogicHook.args[0])
                  if (_.isFinite(joinTimeoutMs) && joinTimeoutMs > 0 && silenceDuration > (joinTimeoutMs / 1000)) {
                    matched = true
                  }
                } else if (_.isNil(joinLogicHook) && isJoinMethod) {
                  const joinTimeoutMs = toJoinTimeoutMs(this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_TIMEOUT])
                  if (_.isFinite(joinTimeoutMs) && joinTimeoutMs > 0 && silenceDuration > (joinTimeoutMs / 1000)) {
                    matched = true
                  }
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
            // ORIGINAL: emit final message immediately, unless join hooks are active.
            if (
              (this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'ORIGINAL' && (_.isNil(this._getJoinLogicHook(this.convoStep))))
            ) {
              let botMsg = { messageText: parsedData.data.message }
              if (this.firstMsg) {
                const sourceData = parsedData
                sourceData.silenceDuration = parsedData.data.start
                sourceData.voiceDuration = parsedData.data.end - parsedData.data.start
                botMsg = Object.assign({}, botMsg, { sourceData })
                this.firstMsg = false
              } else {
                const sourceData = parsedData
                const start = _.get(parsedData, 'data.start', null)
                const prevEnd = _.get(this.prevData, 'data.end', null)
                sourceData.silenceDuration = (_.isFinite(start) && _.isFinite(prevEnd)) ? (start - prevEnd) : (_.isFinite(start) ? start : null)
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
                const start = _.get(parsedData, 'data.start', null)
                const prevEnd = _.get(this.prevData, 'data.end', null)
                sourceData.silenceDuration = (_.isFinite(start) && _.isFinite(prevEnd)) ? (start - prevEnd) : (_.isFinite(start) ? start : null)
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
            if (this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'JOIN' || this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'PSST' || this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'CONCAT' || (!_.isNil(this._getJoinLogicHook(this.convoStep)))) {
              const botMsg = { messageText: parsedData.data.message, sourceData: parsedData }
              this.prevData = parsedData
              if (successfulConfidenceScore) {
                this.botMsgs.push(botMsg)
                const sttHandling = this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING]
                const isPsst = sttHandling === 'PSST'
                const toJoinTimeoutMs = (ms) => {
                  const parsed = parseInt(ms, 10)
                  if (!_.isFinite(parsed) || parsed <= 0) return null
                  return isPsst ? Math.max(0, parsed - 500) : parsed
                }
                const joinLogicHook = this._getJoinLogicHook(this.convoStep)
                let joinTimeoutMs = toJoinTimeoutMs(_.get(joinLogicHook, 'args[0]'))
                if (!_.isFinite(joinTimeoutMs) || joinTimeoutMs <= 0) {
                  joinTimeoutMs = toJoinTimeoutMs(this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_TIMEOUT])
                }
                this.silenceTimeout = setTimeout(() => {
                  if (this.botMsgs.length > 0) {
                    debug('Silence Duration Timeout (JOIN/PSST):', joinTimeoutMs, 'ms')
                    sendBotMsg(joinBotMsg(this.botMsgs, this.joinLastPrevMsg))
                    this.firstMsg = false
                    this.joinLastPrevMsg = this.botMsgs[this.botMsgs.length - 1]
                    this.botMsgs = []
                  }
                }, joinTimeoutMs || 0)
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
    // Avoid logging large buffers/base64 (can break job logs and overwhelm stdout)
    try {
      const safeLog = {
        messageText: msg && msg.messageText ? String(msg.messageText).substring(0, 200) : undefined,
        buttons: msg && Array.isArray(msg.buttons) ? msg.buttons.length : 0,
        hasMedia: !!(msg && msg.media && msg.media.length > 0),
        mediaMimeType: msg && msg.media && msg.media[0] ? msg.media[0].mimeType : undefined,
        mediaSize: msg && msg.media && msg.media[0] && Buffer.isBuffer(msg.media[0].buffer) ? msg.media[0].buffer.length : undefined,
        attachments: msg && Array.isArray(msg.attachments) ? msg.attachments.map(a => ({
          name: a && (a.name || a.title),
          mimeType: a && a.mimeType,
          base64Len: a && a.base64 ? a.base64.length : 0
        })) : []
      }
      debug(safeLog)
    } catch (err) {
      debug(`UserSays: failed to build safe log: ${err.message || err}`)
    }

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
          // Check for DTMF tag in messageText: <DTMF>1234</DTMF>
          const dtmfMatch = msg.messageText.match(/<DTMF>([^<]+)<\/DTMF>/i)
          if (dtmfMatch && dtmfMatch[1]) {
            const digits = dtmfMatch[1]
            debug(`Sending DTMF from messageText: ${digits}`)
            const request = JSON.stringify({
              METHOD: 'sendDtmf',
              digits,
              sessionId: this.sessionId
            })
            this.ws.send(request)
            return resolve()
          }
          if (!this.axiosTtsParams) {
            if (!(msg.media && msg.media.length > 0 && msg.media[0].buffer)) {
              return reject(new Error('TTS not configured, only audio input supported'))
            }
          } else {
            const ttsRequest = this._buildTtsRequest(msg.messageText)
            if (!ttsRequest) return reject(new Error('TTS not configured, only audio input supported'))
            msg.sourceData = ttsRequest

            let ttsResult = null
            try {
              ttsResult = await this._getTtsAudio(ttsRequest, msg.messageText)
            } catch (err) {
              return reject(new Error(`TTS "${msg.messageText}" failed - ${this._getAxiosErrOutput(err)}`))
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
            if (ttsResult && Buffer.isBuffer(ttsResult.buffer)) {
              duration = parseFloat(ttsResult.duration) || 0
              const b64Buffer = ttsResult.buffer.toString('base64')
              const request = JSON.stringify({
                METHOD: 'sendAudio',
                PESQ: false,
                sessionId: this.sessionId,
                b64_buffer: b64Buffer
              })
              if (this._lastBotSaysQueuedAt) {
                const latencyMs = Date.now() - this._lastBotSaysQueuedAt
                const botText = this._lastBotSaysText ? this._lastBotSaysText.substring(0, 120) : '<unknown>'
                debug(`Latency (bot says -> sendAudio/TTS): ${latencyMs} ms (last bot: ${botText})`)
              }
              msg.attachments.push({
                name: 'tts.wav',
                mimeType: 'audio/wav',
                base64: b64Buffer
              })
              this.ws.send(request)
            } else {
              return reject(new Error('TTS failed, response is empty'))
            }
          }
        }
        if (msg && msg.media && msg.media.length > 0 && msg.media[0].buffer) {
          const request = JSON.stringify({
            METHOD: 'sendAudio',
            sessionId: this.sessionId,
            b64_buffer: msg.media[0].buffer.toString('base64')
          })
          if (this._lastBotSaysQueuedAt) {
            const latencyMs = Date.now() - this._lastBotSaysQueuedAt
            const botText = this._lastBotSaysText ? this._lastBotSaysText.substring(0, 120) : '<unknown>'
            debug(`Latency (bot says -> sendAudio/MEDIA): ${latencyMs} ms (last bot: ${botText})`)
          }
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
    this.stopCalled = true
    if (this.silenceTimeout) {
      clearTimeout(this.silenceTimeout)
      this.silenceTimeout = null
    }
    if (this.ws && this.ws.readyState !== WebSocket.CLOSED) {
      const request = JSON.stringify({
        METHOD: 'stopCall',
        sessionId: this.sessionId
      })
      this.ws.send(request)
      await new Promise(resolve => {
        const stopTimeout = setTimeout(resolve, 100000)
        this._stopWaitInterval = setInterval(() => {
          if (this.end) {
            clearTimeout(stopTimeout)
            clearInterval(this._stopWaitInterval)
            this._stopWaitInterval = null
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

  _isTtsCacheEnabled () {
    return this.ttsCacheEnabled && this.ttsCacheMaxEntries > 0
  }

  _touchTtsCache (cacheKey, entry) {
    if (!this._isTtsCacheEnabled()) return
    this.ttsCache.delete(cacheKey)
    this.ttsCache.set(cacheKey, entry)
  }

  _setTtsCacheEntry (cacheKey, entry) {
    if (!this._isTtsCacheEnabled()) return
    this.ttsCache.set(cacheKey, entry)
    this._trimTtsCache()
  }

  _deleteTtsCacheEntry (cacheKey) {
    if (this.ttsCache) {
      this.ttsCache.delete(cacheKey)
    }
  }

  _trimTtsCache () {
    if (!this._isTtsCacheEnabled()) return
    while (this.ttsCache.size > this.ttsCacheMaxEntries) {
      const oldestKey = this.ttsCache.keys().next().value
      this.ttsCache.delete(oldestKey)
    }
  }

  _shouldPrefetchTtsText (text) {
    if (!_.isString(text)) return false
    if (!text.trim()) return false
    return !/\$[A-Za-z]\w*/.test(text)
  }

  _maybePrefetchTts (convoStep) {
    if (!this.ttsPrefetchEnabled || !this._isTtsCacheEnabled()) return
    if (!convoStep || convoStep.sender !== 'me') return
    if (!this._shouldPrefetchTtsText(convoStep.messageText)) return
    const ttsRequest = this._buildTtsRequest(convoStep.messageText)
    if (!ttsRequest) return
    this._getTtsAudio(ttsRequest, convoStep.messageText).catch(err => {
      debug(`TTS prefetch failed - ${this._getAxiosErrOutput(err)}`)
    })
  }

  _buildTtsRequest (text) {
    if (!this.axiosTtsParams) return null
    return {
      ...this.axiosTtsParams,
      params: {
        ...(this.axiosTtsParams.params || {}),
        text
      },
      data: this._getBody(Capabilities.VOIP_TTS_BODY),
      responseType: 'arraybuffer'
    }
  }

  _getTtsCacheKey (ttsRequest) {
    const cacheKeyObj = {
      url: ttsRequest.url,
      method: ttsRequest.method,
      params: ttsRequest.params,
      data: ttsRequest.data
    }
    try {
      return JSON.stringify(cacheKeyObj)
    } catch (err) {
      return `${ttsRequest.url}|${ttsRequest.method}|${_.get(ttsRequest, 'params.text', '')}`
    }
  }

  async _fetchTts (ttsRequest, text) {
    const ttsStart = Date.now()
    const ttsResponse = await axios(ttsRequest)
    const ttsMs = Date.now() - ttsStart
    const textLen = _.isString(text) ? text.length : 0
    debug(`TTS response ${ttsMs} ms (chars: ${textLen})`)

    if (!ttsResponse || !Buffer.isBuffer(ttsResponse.data)) {
      throw new Error(`TTS failed, response is: ${this._getAxiosShortenedOutput(ttsResponse && ttsResponse.data)}`)
    }
    const durationHeader = ttsResponse.headers && ttsResponse.headers['content-duration']
    return {
      buffer: ttsResponse.data,
      duration: durationHeader
    }
  }

  async _getTtsAudio (ttsRequest, text) {
    if (!ttsRequest) throw new Error('TTS request not configured')
    const cacheKey = this._getTtsCacheKey(ttsRequest)

    if (this._isTtsCacheEnabled()) {
      const cached = this.ttsCache.get(cacheKey)
      if (cached) {
        if (cached.state === 'ready') {
          this._touchTtsCache(cacheKey, cached)
          debug(`TTS cache hit (chars: ${_.isString(text) ? text.length : 0})`)
          return { buffer: cached.buffer, duration: cached.duration }
        }
        if (cached.state === 'pending') {
          return cached.promise
        }
      }
    }

    const fetchPromise = this._fetchTts(ttsRequest, text)
      .then(result => {
        if (this._isTtsCacheEnabled()) {
          this._setTtsCacheEntry(cacheKey, { state: 'ready', buffer: result.buffer, duration: result.duration })
        }
        return result
      })
      .catch(err => {
        this._deleteTtsCacheEntry(cacheKey)
        throw err
      })

    if (this._isTtsCacheEnabled()) {
      this._setTtsCacheEntry(cacheKey, { state: 'pending', promise: fetchPromise })
    }

    return fetchPromise
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
