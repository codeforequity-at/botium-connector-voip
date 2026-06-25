const WebSocket = require('ws')
const _ = require('lodash')
const axios = require('axios')
const http = require('http')
const https = require('https')
const debug = require('debug')('botium-connector-voip')
const mm = require('music-metadata')

// Logging policy: info = rare, business-relevant lifecycle events (always visible).
// debug = high-frequency diagnostics (DEBUG=botium-connector-voip). warn = degraded but continuing.
// error = abort/failure. No secrets in info; STT text only as length or truncated in info.

/** WS frame types logged only at start/end handlers — not per-chunk (hundreds per call). */
const WS_DEBUG_SILENT_TYPES = new Set(['audioStreamChunk', 'fullRecordChunk'])
const AGENT_SPEECH_RMS_WINDOW_MS = 100
const DEFAULT_AGENT_SPEECH_RMS_THRESHOLD = 500
const DEFAULT_AGENT_SPEECH_SUSTAINED_WINDOWS = 2
const WS_DEBUG_BASE64_FIELD_NAMES = new Set([
  'chunk', 'buffer', 'base64', 'fullRecord', 'full_record', 'audio', 'audioData', 'b64_buffer'
])

const _info = (event, data) => {
  const parts = Object.entries({ event, ...data })
    .filter(([, v]) => v != null && v !== '')
    .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
  console.info(`[botium-connector-voip] ${parts.join(' ')}`)
}

/** DTMF for RTP/PJSIP: 0–9, *, #, A–D. Strips spaces and other separators so the worker gets one compact string. */
const sanitizeDtmfDigits = (raw) => {
  if (raw == null) return ''
  return String(raw).replace(/[^0-9*#ABCDabcd]/g, '').toUpperCase()
}

// Matches voipcall.generate_dtmf_sequence defaults (100 ms tone, 50 ms pause between digits).
const DTMF_TONE_MS = 100
const DTMF_PAUSE_MS = 50
const DTMF_MS_PER_DIGIT = 200
const DEFAULT_AUDIO_STREAM_INTERVAL_MS = 250

const audioStreamIntervalMs = () => {
  const n = Number(process.env.VOIP_AUDIO_STREAM_INTERVAL_MS || process.env.AUDIO_STREAM_INTERVAL_MS)
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_AUDIO_STREAM_INTERVAL_MS
}

const dtmfPlaybackMs = (digitCount) => {
  if (!digitCount || digitCount <= 0) return 0
  return digitCount * DTMF_TONE_MS + Math.max(0, digitCount - 1) * DTMF_PAUSE_MS
}

/** Wait before turn-audio slice so DTMF PCM is flushed through audioStream (250 ms default). */
const dtmfTurnAudioWaitMs = (digitCount) => {
  const playbackMs = dtmfPlaybackMs(digitCount)
  const streamMs = audioStreamIntervalMs()
  return Math.max(digitCount * DTMF_MS_PER_DIGIT, playbackMs + streamMs + 50)
}

const Capabilities = {
  VOIP_STT_URL_STREAM: 'VOIP_STT_URL_STREAM',
  VOIP_STT_PARAMS_STREAM: 'VOIP_STT_PARAMS_STREAM',
  VOIP_STT_METHOD_STREAM: 'VOIP_STT_METHOD_STREAM',
  VOIP_STT_BODY_STREAM: 'VOIP_STT_BODY_STREAM',
  VOIP_STT_BODY: 'VOIP_STT_BODY',
  VOIP_STT_AZURE_SEGMENTATION_SILENCE_TIMEOUT_MS: 'VOIP_STT_AZURE_SEGMENTATION_SILENCE_TIMEOUT_MS',
  VOIP_STT_HEADERS: 'VOIP_STT_HEADERS',
  VOIP_STT_TIMEOUT: 'VOIP_STT_TIMEOUT',
  VOIP_STT_MESSAGE_HANDLING: 'VOIP_STT_MESSAGE_HANDLING',
  VOIP_STT_MESSAGE_HANDLING_TIMEOUT: 'VOIP_STT_MESSAGE_HANDLING_TIMEOUT',
  VOIP_STT_MESSAGE_HANDLING_TIMEOUT_SUBSEQUENT: 'VOIP_STT_MESSAGE_HANDLING_TIMEOUT_SUBSEQUENT',
  VOIP_JOIN_SILENCE_DURATION_BY_SUBSTRING: 'VOIP_JOIN_SILENCE_DURATION_BY_SUBSTRING',
  VOIP_STT_DICTIONARY_REPLACEMENTS: 'VOIP_STT_DICTIONARY_REPLACEMENTS',
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
  VOIP_USE_GLOBAL_VOIP_WORKER: 'VOIP_USE_GLOBAL_VOIP_WORKER',
  VOIP_USER_INPUT_PREFER_VOICE: 'VOIP_USER_INPUT_PREFER_VOICE',
  VOIP_EMIT_SPECULATIVE_TEXT: 'VOIP_EMIT_SPECULATIVE_TEXT',
  VOIP_SDP_MEDIA_TYPE_TEXT_ENABLE: 'VOIP_SDP_MEDIA_TYPE_TEXT_ENABLE',
  VOIP_TURN_AUDIO_ENABLE: 'VOIP_TURN_AUDIO_ENABLE',
  VOIP_TURN_AUDIO_PADDING_MS: 'VOIP_TURN_AUDIO_PADDING_MS',
  VOIP_TURN_AUDIO_OFFSET_MS: 'VOIP_TURN_AUDIO_OFFSET_MS'
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
  VOIP_STT_AZURE_SEGMENTATION_SILENCE_TIMEOUT_MS: 500,
  VOIP_USE_GLOBAL_VOIP_WORKER: false,
  VOIP_SIP_PROTOCOL: 'TCP',
  VOIP_USER_INPUT_PREFER_VOICE: true,
  VOIP_SDP_MEDIA_TYPE_TEXT_ENABLE: false,
  VOIP_TURN_AUDIO_ENABLE: true,
  VOIP_TURN_AUDIO_PADDING_MS: 150,
  VOIP_TURN_AUDIO_OFFSET_MS: 0
}

// Inject the Azure end-of-speech segmentation timeout into the STT body. botium-speech-processing
// applies azure.config.properties via SpeechConfig.setProperty(), so this controls how long Azure
// waits after the last word before emitting final=true. Only applied for the Azure engine, never
// overrides a value already present in the profile config, and clones so the capability object stays
// untouched. Set the capability to 0/empty to disable injection.
const injectAzureSegmentationTimeout = (body, sttParams, timeoutMs) => {
  const ms = Number(timeoutMs)
  if (!_.isFinite(ms) || ms <= 0) return body
  const isAzure = (sttParams && sttParams.stt === 'azure') ||
    (body && typeof body === 'object' && body.azure) ||
    (typeof body === 'string' && body.indexOf('azure') !== -1)
  if (!isAzure) return body

  let next
  if (body == null) {
    next = {}
  } else if (typeof body === 'string') {
    try { next = JSON.parse(body) } catch (err) { return body }
  } else {
    next = _.cloneDeep(body)
  }
  if (!_.isObject(next.azure)) next.azure = {}
  if (!_.isObject(next.azure.config)) next.azure.config = {}
  if (!_.isObject(next.azure.config.properties)) next.azure.config.properties = {}
  if (next.azure.config.properties.Speech_SegmentationSilenceTimeoutMs == null) {
    next.azure.config.properties.Speech_SegmentationSilenceTimeoutMs = String(Math.round(ms))
  }
  return next
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
    this._replyTrace = null
    this._activeUserSaysVoipAgent = null
    this._speculativeTurnToken = 0
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
    this.fullRecordAttachmentEmitted = false
    this.end = false
    this.connected = false
    this.convoStep = null
    this._lastBotSaysQueuedAt = null
    this._lastBotSaysText = null
    this._replyTrace = null
    this._activeUserSaysVoipAgent = null
    this.audioStream = { format: null, pcmParts: [], totalBytes: 0, complete: false }
    this._turnAudioCounter = 0
    this._lastBotTurnStartSec = null
    if (this.ttsCache) {
      this.ttsCache.clear()
    }

    const sendBotMsg = (botMsg) => {
      const queuedAt = Date.now()
      this._lastBotSaysQueuedAt = queuedAt
      this._lastBotSaysText = botMsg && botMsg.messageText ? String(botMsg.messageText) : null
      this._captureBotQueuedForReplyTrace(queuedAt)
      // Stamp the wall-clock instant at which the connector released the bot
      // utterance to botium-core's queue. Paired with `_receivedAtMs` (last
      // STT final frame) this gives the true "join silence" the connector
      // imposed, independent of how long the coach later takes to pick the
      // message up with WaitBotSays().
      if (botMsg && botMsg.sourceData) {
        const head = Array.isArray(botMsg.sourceData) ? botMsg.sourceData[0] : botMsg.sourceData
        if (head && typeof head === 'object') {
          if (!('flushedAtMs' in head)) head.flushedAtMs = queuedAt
          if (this._replyTrace) {
            if (_.isFinite(this._replyTrace.psstFireDelayMs)) head.psstFireDelayMs = this._replyTrace.psstFireDelayMs
            if (_.isFinite(this._replyTrace.psstScheduledMs)) head.psstScheduledMs = this._replyTrace.psstScheduledMs
          }
        }
      }
      // A turn = bot speaks first, then me responds.
      // Save the bot's audio start so UserSays can slice the full exchange
      // (bot + me) once me has finished speaking.
      if (this.caps[Capabilities.VOIP_TURN_AUDIO_ENABLE] && botMsg && !(botMsg instanceof Error)) {
        try {
          const sd = botMsg.sourceData
          let startSec = null
          if (Array.isArray(sd) && sd.length > 0) {
            startSec = _.get(sd, '[0].data.start', null)
          } else if (sd && typeof sd === 'object') {
            startSec = _.get(sd, 'data.start', null)
          }
          // Only record the FIRST bot message's start; if several bot messages
          // arrive before the next UserSays they all belong to the same turn.
          if (_.isFinite(startSec) && this._lastBotTurnStartSec === null) {
            this._lastBotTurnStartSec = startSec
          }
        } catch (err) {
          debug(`${this.sessionId} - sendBotMsg: saving turn start error: ${err && err.message}`)
        }
      }
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
      const lastSpeechEnd = _.get(botMsgs, `[${botMsgs.length - 1}].sourceData.data.speechEndSec`, null)
      if (_.isFinite(lastSpeechEnd) && botMsg.sourceData[0] && botMsg.sourceData[0].data) {
        botMsg.sourceData[0].data.speechEndSec = lastSpeechEnd
      }
      return botMsg
    }

    // Returns true when a partial represents a NEW utterance rather than a tail/echo
    // of buffered text. Used by PSST partial handler to extend the silence timer.
    // Heuristic: substring check, then word-overlap fallback (<70% overlap = new).
    const partialLooksLikeNewUtterance = (partialText, botMsgs) => {
      const normalize = s => String(s || '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim()
      const partial = normalize(partialText)
      if (!partial) return false
      const buffered = normalize((botMsgs || []).map(m => (m && m.messageText) || '').join(' '))
      if (!buffered) return true
      if (buffered.includes(partial)) return false
      const partialWords = partial.split(' ').filter(Boolean)
      if (partialWords.length === 0) return false
      const bufferedWords = new Set(buffered.split(' ').filter(Boolean))
      const overlap = partialWords.filter(w => bufferedWords.has(w)).length
      return (overlap / partialWords.length) < 0.7
    }

    // Arm (or re-arm) the JOIN/PSST silence timer that flushes buffered STT
    // chunks once the bot has been silent for `joinTimeoutMs`. No-op outside
    // JOIN/PSST/CONCAT modes (other modes emit finals immediately).
    const armJoinSilenceTimer = () => {
      if (!this.botMsgs || this.botMsgs.length === 0) return
      const sttHandling = this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING]
      const isJoinMethod = sttHandling === 'JOIN' || sttHandling === 'PSST' || sttHandling === 'CONCAT' || this._hasJoinLogicHookOrRule(this.convoStep)
      if (!isJoinMethod) return
      const joinTimeoutMs = this._getEffectiveJoinTimeoutMs(this.convoStep, this.botMsgs)
      if (this.silenceTimeout) {
        clearTimeout(this.silenceTimeout)
        this.silenceTimeout = null
      }
      const bufferedAtArm = this.botMsgs.length
      const armedAt = Date.now()
      this._markReplyTrace({ psstTimerArmedAtMs: armedAt, psstScheduledMs: joinTimeoutMs || 0 })
      _info('psst_timer_armed', {
        sessionId: this.sessionId,
        joinTimeoutMs: joinTimeoutMs || 0,
        bufferedChunks: bufferedAtArm,
        stopCalled: !!this.stopCalled
      })
      // Emit the authoritative join timeout so downstream consumers (e.g.
      // SpeculationBuffer) can size their quiet threshold relative to it.
      if (this.eventEmitter && _.isFinite(joinTimeoutMs) && joinTimeoutMs > 0) {
        try {
          this.eventEmitter.emit('voip.psstTimerArmed', {
            sessionId: this.sessionId,
            joinTimeoutMs,
            bufferedChunks: bufferedAtArm,
            armedAt
          })
        } catch (emitErr) {
          // Never block the silence timer on listener errors.
          debug(`voip.psstTimerArmed emission failed: ${emitErr && emitErr.message}`)
        }
      }
      this.silenceTimeout = setTimeout(() => {
        const fireDelay = Date.now() - armedAt
        this._markReplyTrace({ psstTimerFiredAtMs: Date.now(), psstFireDelayMs: fireDelay })
        if (this.botMsgs.length > 0) {
          _info('psst_timer_fired', {
            sessionId: this.sessionId,
            bufferedChunks: this.botMsgs.length,
            actualDelayMs: fireDelay,
            scheduledDelayMs: joinTimeoutMs || 0,
            outcome: 'emit'
          })
          debug('Silence Duration Timeout (JOIN/PSST):', joinTimeoutMs, 'ms')
          sendBotMsg(joinBotMsg(this.botMsgs, this.joinLastPrevMsg))
          this.firstMsg = false
          this.joinLastPrevMsg = this.botMsgs[this.botMsgs.length - 1]
          this.botMsgs = []
          // Reset partial-driven extension budget for next cycle.
          this.psstRearmCount = 0
          this.psstFirstRearmAt = null
        } else {
          _info('psst_timer_fired', {
            sessionId: this.sessionId,
            bufferedChunks: 0,
            actualDelayMs: fireDelay,
            scheduledDelayMs: joinTimeoutMs || 0,
            outcome: 'noop_empty_buffer'
          })
        }
      }, joinTimeoutMs || 0)
    }

    // Flush buffered STT chunks on teardown so a late final is not lost when
    // the PSST silence timer is cleared by Stop(). Falls back to the cached
    // interim transcript (`lastPartialBotMsg`) when STT never delivered a
    // final for the closing utterance. Must be called from every terminal
    // path before `this.end` is flipped.
    const flushPendingBotMsgs = (reason) => {
      if (this.silenceTimeout) {
        _info('psst_timer_cleared', {
          sessionId: this.sessionId,
          reason: `flush:${reason}`,
          bufferedChunks: (this.botMsgs && this.botMsgs.length) || 0
        })
        clearTimeout(this.silenceTimeout)
        this.silenceTimeout = null
      }
      if (this.botMsgs && this.botMsgs.length > 0) {
        const chunkCount = this.botMsgs.length
        _info('stt_buffer_flushed_on_end', {
          sessionId: this.sessionId,
          reason,
          outcome: 'flushed',
          chunks: chunkCount
        })
        debug(`Flushing ${chunkCount} buffered STT chunk(s) on ${reason}`)
        sendBotMsg(joinBotMsg(this.botMsgs, this.joinLastPrevMsg))
        this.firstMsg = false
        this.joinLastPrevMsg = this.botMsgs[this.botMsgs.length - 1]
        this.botMsgs = []
        this.lastPartialBotMsg = null
        this.psstRearmCount = 0
        this.psstFirstRearmAt = null
        return
      }

      if (this.lastPartialBotMsg && typeof this.lastPartialBotMsg.messageText === 'string' && this.lastPartialBotMsg.messageText.trim().length > 0) {
        const recovered = this.lastPartialBotMsg
        const textLen = recovered.messageText.length
        const preview = recovered.messageText.trim().substring(0, 80)
        _info('stt_partial_recovery_on_end', {
          sessionId: this.sessionId,
          reason,
          outcome: 'partial_recovery',
          messageLength: textLen,
          message: preview
        })
        debug(`Recovering last STT partial on ${reason} (no final arrived): "${preview}"`)
        sendBotMsg(recovered)
        this.firstMsg = false
        this.joinLastPrevMsg = recovered
        this.lastPartialBotMsg = null
        return
      }

      _info('stt_flush_noop', {
        sessionId: this.sessionId,
        reason,
        outcome: 'noop',
        sttPartialCount: this.sttPartialCount || 0
      })
      debug(`flushPendingBotMsgs(${reason}): nothing buffered, no partial available (sttPartialCount=${this.sttPartialCount || 0})`)
    }
    // Expose on the instance so Stop() (outside this closure) can call it as a final safety net.
    this._flushPendingBotMsgs = flushPendingBotMsgs

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
    let httpInitRetries = 0
    const connectHttp = async (retryIndex) => {
      retryIndex = retryIndex || 0
      let initCallUrl = ''
      try {
        const workerUrl = this.caps[Capabilities.VOIP_USE_GLOBAL_VOIP_WORKER]
          ? process.env.BOTIUM_VOIP_WORKER_URL
          : this.caps[Capabilities.VOIP_WORKER_URL].replace('wss', 'https').replace('ws', 'http')
        const baseUrl = workerUrl.endsWith('/') ? workerUrl.slice(0, -1) : workerUrl
        initCallUrl = `${baseUrl}/initCall`
        const postPayload = {
          API_KEY: this.caps[Capabilities.VOIP_USE_GLOBAL_VOIP_WORKER] ? process.env.BOTIUM_VOIP_WORKER_APIKEY : this.caps[Capabilities.VOIP_WORKER_APIKEY]
        }
        const payloadForLog = { ...postPayload, API_KEY: postPayload.API_KEY != null && postPayload.API_KEY !== '' ? '[REDACTED]' : postPayload.API_KEY }
        debug(`HTTP initCall: POST ${initCallUrl} payload=${JSON.stringify(payloadForLog)}`)
        const res = await axios({
          method: 'post',
          data: postPayload,
          url: initCallUrl
        })
        if (res) {
          data = res.data
          headers = res.headers
          debug(`HTTP initCall: response status=${res.status} data=${this._getAxiosShortenedOutput(res.data)}`)
        }
      } catch (err) {
        debug(`HTTP initCall: failed ${initCallUrl} — ${this._getAxiosErrOutput(err)}`)
        debug(`Retry ${retryIndex} / ${this.caps[Capabilities.VOIP_WEBSOCKET_CONNECT_MAXRETRIES]}: Connecting to VOIP Worker failed: ${err.message || 'Not reachable'}`)
        if (retryIndex === this.caps[Capabilities.VOIP_WEBSOCKET_CONNECT_MAXRETRIES]) {
          throw new Error(`Connecting to VOIP Worker failed: ${err.message || 'Not reachable'}`)
        }
        httpInitRetries = retryIndex + 1
        // Retry after the defined timeout
        await new Promise(resolve => setTimeout(resolve, this.caps[Capabilities.VOIP_WEBSOCKET_CONNECT_TIMEOUT]))

        // Retry the connection
        await connectHttp(retryIndex + 1)
      }
    }
    await connectHttp()
    if (httpInitRetries > 0) {
      _info('connected_after_retries', { phase: 'initCall', retries: httpInitRetries })
    }

    return new Promise((resolve, reject) => {
      const wsEndpoint = `${this.caps[Capabilities.VOIP_USE_GLOBAL_VOIP_WORKER] ? process.env.BOTIUM_VOIP_WORKER_URL : this.caps[Capabilities.VOIP_WORKER_URL]}/ws/${data.port}`
      const connectWs = (retryIndex) => {
        retryIndex = retryIndex || 0
        return new Promise((resolve, reject) => {
          const useCookie = headers && 'set-cookie' in headers
          const connectPayload = useCookie
            ? { withCookie: true, cookieSize: (headers['set-cookie'] && String(headers['set-cookie']).length) || 0 }
            : { withCookie: false }
          debug(
            `WebSocket connect: url=${wsEndpoint} attempt=${retryIndex + 1} max=${this.caps[Capabilities.VOIP_WEBSOCKET_CONNECT_MAXRETRIES] + 1} ` +
            `options=${JSON.stringify(connectPayload)}`
          )
          if (useCookie) {
            this.ws = new WebSocket(wsEndpoint, {
              headers: {
                Cookie: headers['set-cookie']
              }
            })
          } else {
            this.ws = new WebSocket(wsEndpoint)
          }
          this.ws.on('open', () => {
            debug(
              `WebSocket connect: result=open url=${wsEndpoint} attempt=${retryIndex + 1} ` +
              `readyState=${this.ws && this.ws.readyState}`
            )
            resolve(retryIndex)
          })
          this.ws.on('error', (err) => {
            debug(
              `WebSocket connect: result=error url=${wsEndpoint} attempt=${retryIndex + 1} ` +
              `err=${(err && err.message) || err || 'unknown'}`
            )
            if (retryIndex === this.caps[Capabilities.VOIP_WEBSOCKET_CONNECT_MAXRETRIES]) {
              reject(new Error(`Websocket connection failed to ${wsEndpoint}`))
            }
            setTimeout(() => connectWs(retryIndex + 1).then(resolve).catch(reject), this.caps[Capabilities.VOIP_WEBSOCKET_CONNECT_TIMEOUT])
          })
        })
      }
      connectWs().then((wsRetries) => {
        if (wsRetries > 0) {
          _info('connected_after_retries', { phase: 'websocket', retries: wsRetries })
        }
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
              const silenceMs = this._getEffectiveJoinTimeoutMs(convoStep, this.botMsgs)
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
        this.sttPartialCount = 0
        // Last interim transcript, used by `flushPendingBotMsgs` when no final arrives.
        this.lastPartialBotMsg = null
        this.firstMsg = true
        this.firstSttInfoReceived = false
        this.silenceTimeout = null
        // PSST re-arm tracking: new-utterance partials reset the silence timer,
        // bounded by MAX_EXTENSION_MS to prevent infinite stranding.
        this.psstRearmCount = 0
        this.psstFirstRearmAt = null

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
          SDP_MEDIA_TYPE_TEXT_ENABLE: !!this.caps[Capabilities.VOIP_SDP_MEDIA_TYPE_TEXT_ENABLE],
          AUDIO_STREAM: !!this.caps[Capabilities.VOIP_TURN_AUDIO_ENABLE],
          STT_LEGACY: sttLegacy,
          STT_CONFIG: {
            stt_url: sttUrl,
            stt_params: this.caps[Capabilities.VOIP_STT_PARAMS_STREAM],
            stt_body: injectAzureSegmentationTimeout(
              this.caps[Capabilities.VOIP_STT_BODY_STREAM] || null,
              this.caps[Capabilities.VOIP_STT_PARAMS_STREAM],
              this.caps[Capabilities.VOIP_STT_AZURE_SEGMENTATION_SILENCE_TIMEOUT_MS]
            )
          },
          TTS_CONFIG: {
            tts_url: this.caps[Capabilities.VOIP_TTS_URL],
            tts_params: this.caps[Capabilities.VOIP_TTS_PARAMS],
            tts_body: this.caps[Capabilities.VOIP_TTS_BODY] || null
          }
        }

        _info('initcall_sent', {
          sipCallerAddress: request.SIP_CALLER_ADDRESS,
          sipRegistrarUri: request.SIP_CALLER_REGISTRAR_URI,
          sipCalleeUri: request.SIP_CALLEE_URI,
          sipCallerUsername: request.SIP_CALLER_USERNAME,
          sipProxy: request.SIP_PROXY || null,
          iceEnable: request.ICE_ENABLE,
          sttUrl: request.STT_CONFIG && request.STT_CONFIG.stt_url,
          ttsUrl: request.TTS_CONFIG && request.TTS_CONFIG.tts_url
        })
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
          // Drop non-JSON gateway frames (rare, but crash the process without this guard).
          let parsedData
          try {
            parsedData = JSON.parse(data)
            // Earliest local observation of the frame. Consumed downstream
            // as the "bot finished speaking" anchor for `recordingEpochMs`
            // (propagates via `sourceData = parsedData` reference).
            parsedData._receivedAtMs = Date.now()
          } catch (parseErr) {
            const rawString = (() => {
              try {
                if (Buffer.isBuffer(data)) return data.toString('utf8')
                if (typeof data === 'string') return data
                if (data && typeof data.toString === 'function') return data.toString()
                return '<non-stringable>'
              } catch (_) { return '<unreadable>' }
            })()
            const preview = rawString.length > 200 ? `${rawString.substring(0, 200)}...` : rawString
            debug(`${this.sessionId} - Ignoring non-JSON WS frame from gateway: ${preview}`)
            if (typeof _info === 'function') {
              try {
                _info('ws_non_json_frame', {
                  sessionId: this.sessionId,
                  preview,
                  byteLength: rawString.length,
                  parseError: parseErr && parseErr.message ? parseErr.message : String(parseErr)
                })
              } catch (_infoErr) { /* structured logger is best-effort */ }
            }
            return
          }
          // After Stop(), still accept late-arriving recording frames
          // (fullRecord*) and hard errors so `full_record.wav` is delivered
          // on early-completion hangups. Post-Stop STT frames remain blocked.
          if (this.stopCalled) {
            const allowedPostStopTypes = ['fullRecord', 'fullRecordStart', 'fullRecordChunk', 'fullRecordEnd', 'error', 'audioStreamStart', 'audioStreamChunk', 'audioStreamEnd']
            if (!parsedData || !allowedPostStopTypes.includes(parsedData.type)) {
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
              if (typeof val === 'string' && val.length > 0 &&
                  (WS_DEBUG_BASE64_FIELD_NAMES.has(key) || val.length > 500)) {
                obj[key] = `<base64:${val.length}chars>`
              } else if (val && typeof val === 'object' && !Array.isArray(val)) {
                sanitizeBase64Fields(val, `${prefix}${key}.`)
              }
            }
          }
          sanitizeBase64Fields(parsedDataLog)

          if (!WS_DEBUG_SILENT_TYPES.has(parsedData?.type)) {
            debug(JSON.stringify(parsedDataLog, null, 2))
          }

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
            this.fullRecordAttachmentEmitted = true
          }
          this._emitBufferedFullRecordIfAny = (reason) => {
            if (this.fullRecordAttachmentEmitted) return false
            if (!this.fullRecord || typeof this.fullRecord !== 'string' || this.fullRecord.length === 0) return false
            emitFullRecordAttachment(this.fullRecord)
            _info('recording_attached', {
              sessionId: this.sessionId,
              source: reason,
              base64Len: this.fullRecord.length
            })
            return true
          }

          if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'initialized') {
            this.sessionId = parsedData.voipConfig.sessionId
            _info('callinfo_initialized', {
              sessionId: this.sessionId,
              sttHandling: this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING]
            })
          }

          // if sessionId is not the same as the one in the callinfo, return
          if (parsedData && parsedData.voipConfig && parsedData.voipConfig.sessionId && parsedData.voipConfig.sessionId !== this.sessionId) {
            debug('sessionId mismatch, returning')
            return
          }

          if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'unauthorized') {
            _info('callinfo_sip_unauthorized', { sessionId: this.sessionId, event: parsedData.event || null, sipCallerUsername: this.caps[Capabilities.VOIP_SIP_CALLER_USERNAME], sipRegistrarUri: this.caps[Capabilities.VOIP_SIP_CALLER_REGISTRAR_URI] })
            reject(new Error('Error: Cannot open a call: SIP Authorization failed'))
          }

          if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'forbidden' && parsedData.event === 'onCallRegState') {
            _info('callinfo_sip_reg_failed', { sessionId: this.sessionId, event: parsedData.event || null, sipCallerAddress: this.caps[Capabilities.VOIP_SIP_CALLER_ADDRESS], sipCallerUsername: this.caps[Capabilities.VOIP_SIP_CALLER_USERNAME], sipRegistrarUri: this.caps[Capabilities.VOIP_SIP_CALLER_REGISTRAR_URI] })
            reject(new Error('Error: Sip Registration failed'))
          }

          if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'connected') {
            _info('callinfo_connected', { sessionId: this.sessionId })
            resolve()
          }

          if (parsedData && parsedData.type === 'callinfo' && parsedData.status === 'disconnected') {
            _info('callinfo_disconnected', {
              sessionId: this.sessionId,
              connectDurationSec: parsedData.connectDuration || null,
              sttPartialCount: this.sttPartialCount
            })
            flushPendingBotMsgs('callinfo_disconnected')
            // Some workers may disconnect without sending fullRecordEnd.
            // If we already buffered full-record chunks, emit them now.
            this._emitBufferedFullRecordIfAny('callinfo_disconnected_buffered')
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

          if (parsedData && parsedData.type === 'callinfo' && !['initialized', 'unauthorized', 'forbidden', 'connected', 'disconnected'].includes(parsedData.status)) {
            _info('callinfo_unknown_status', { sessionId: this.sessionId, status: parsedData.status || null, event: parsedData.event || null })
          }

          if (parsedData && parsedData.type === 'error') {
            flushPendingBotMsgs('error')
            // Ensure buffered recording is not lost on terminal worker errors.
            this._emitBufferedFullRecordIfAny('error_buffered')
            _info('ws_error_msg', { sessionId: this.sessionId, message: parsedData.message || null, code: parsedData.code || null })
            this.end = true
            reject(new Error(`Error: ${parsedData.message}`))
            sendBotMsg(new Error(`Error: ${parsedData.message}`))
          }

          // Per-turn audio stream: continuous PCM chunks received during the call.
          // The connector buffers them so _sliceTurnAudio() can extract per-turn segments.
          if (parsedData && parsedData.type === 'audioStreamStart') {
            this.audioStream = {
              format: {
                sampleRate: parsedData.sampleRate,
                channels: parsedData.channels,
                bitsPerSample: parsedData.bitsPerSample,
                dataOffset: parsedData.dataOffset,
              },
              pcmParts: [],
              totalBytes: 0,
              complete: false,
            }
            debug(`${this.sessionId} - audioStreamStart sampleRate=${parsedData.sampleRate} channels=${parsedData.channels} bitsPerSample=${parsedData.bitsPerSample}`)
          }
          if (parsedData && parsedData.type === 'audioStreamChunk') {
            if (this.audioStream && parsedData.chunk) {
              try {
                const buf = Buffer.from(parsedData.chunk, 'base64')
                this.audioStream.pcmParts.push(buf)
                this.audioStream.totalBytes += buf.length
                this._maybeDetectAgentAudibleOnRecording(this._activeUserSaysVoipAgent)
              } catch (e) {
                debug(`${this.sessionId} - audioStreamChunk decode error: ${e && e.message}`)
              }
            }
          }
          if (parsedData && parsedData.type === 'audioStreamEnd') {
            if (this.audioStream) {
              this.audioStream.complete = true
            }
            debug(`${this.sessionId} - audioStreamEnd totalBytes=${parsedData.totalBytes}`)
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
            const base64Len = (this.fullRecord || tail || '').length
            emitFullRecordAttachment(this.fullRecord || tail)
            _info('recording_attached', { sessionId: this.sessionId, source: 'fullRecordEnd', base64Len })
            // Flush before `this.end = true` so the buffered final STT is not
            // dropped when Stop() clears the PSST silence timer on teardown.
            flushPendingBotMsgs('fullRecordEnd')
            this.end = true
          }

          if (parsedData && parsedData.type === 'agentPlaybackStarted') {
            const playbackData = parsedData.data || {}
            const playedSec = playbackData.playedRecordingStartSec
            const active = this._activeUserSaysVoipAgent
            if (active && _.isFinite(playedSec)) {
              active.playedRecordingStartSec = playedSec
              active.playbackAtMs = playbackData.playbackAtMs
              if (_.isFinite(playbackData.requestedDurationMs)) {
                active.playbackRequestedDurationMs = playbackData.requestedDurationMs
              }
              if (_.isFinite(playbackData.digitCount)) {
                active.digitCount = playbackData.digitCount
              }
              this._markReplyTrace({
                playedRecordingStartSec: playedSec,
                playbackAtMs: playbackData.playbackAtMs
              })
              const heardSec = playbackData.wireKind === 'dtmf'
                ? playedSec
                : this._applyAgentHeardRecordingStartSec(active)
              if (_.isFinite(heardSec)) {
                if (playbackData.wireKind === 'dtmf') {
                  active.heardRecordingStartSec = heardSec
                }
                debug(`${this.sessionId} - agent audible on recording at ${heardSec}s (played=${playedSec}s)`)
              }
            }
          }

          if (parsedData && parsedData.type === 'silence') {
            if (_.isNil(this._getIgnoreSilenceDurationAsserterLogicHook(this.convoStep))) {
              if (!this._hasJoinLogicHookOrRule(this.convoStep) && parsedData.data.silence.length > 0) {
                flushPendingBotMsgs('silence_exceeded')
                this.end = true
                sendBotMsg(new Error(`Silence Duration of ${parsedData.data.silence[0][2].toFixed(2)}s exceeded General Silence Duration Timeout of ${this.caps[Capabilities.VOIP_SILENCE_DURATION_TIMEOUT] / 1000}s`))
              }
            }
          }

          if (parsedData && parsedData.type === 'fullRecord') {
            // Non-chunked full record
            const base64 = _extractFullRecordBase64(parsedData)
            emitFullRecordAttachment(base64)
            _info('recording_attached', { sessionId: this.sessionId, source: 'fullRecord', base64Len: (base64 || '').length })
            flushPendingBotMsgs('fullRecord')
            this.end = true
          }

          if (parsedData && parsedData.data && parsedData.data.final === false) {
            this.sttPartialCount++
            const partialText = parsedData.data.message
            if (typeof partialText === 'string' && partialText.trim().length > 0) {
              const replacementResult = this._applySttDictionaryReplacements(partialText)
              this.lastPartialBotMsg = {
                messageText: replacementResult.text,
                sourceData: Object.assign(
                  {},
                  this._decorateSourceDataWithSttDictionaryReplacements(parsedData, replacementResult),
                  { partialRecovery: true }
                )
              }
            }
            const partialPreview = typeof partialText === 'string' ? partialText.trim() : ''
            _info('stt_partial_received', {
              sessionId: this.sessionId,
              partialIndex: this.sttPartialCount,
              preview: partialPreview,
              bufferedChunks: (this.botMsgs && this.botMsgs.length) || 0,
              timerActive: !!this.silenceTimeout
            })
            // Emit liveness keep-alive so waiters can distinguish "bot silent"
            // from "bot streaming partials between finals".
            if (this.caps[Capabilities.VOIP_EMIT_SPECULATIVE_TEXT] && this.eventEmitter && typeof partialText === 'string' && partialText.trim().length > 0) {
              this.eventEmitter.emit('voip.botActivity', {
                sessionId: this.sessionId,
                kind: 'partial',
                partialIndex: this.sttPartialCount,
                bufferedChunks: (this.botMsgs && this.botMsgs.length) || 0
              })
            }
            // PSST partial timer logic:
            //   Empty buffer → clear timer (final will arm fresh).
            //   Buffered finals + new-utterance partial → re-arm within cap.
            //   Tail/echo or cap-exceeded partials → ignore (legacy path).
            if (this.silenceTimeout) {
              if (!this.botMsgs || this.botMsgs.length === 0) {
                clearTimeout(this.silenceTimeout)
                this.silenceTimeout = null
              } else {
                const sttHandling = this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING]
                const isPsstMode = sttHandling === 'PSST'
                if (isPsstMode && typeof partialText === 'string' && partialText.trim().length > 0) {
                  const MAX_EXTENSION_MS = 60000
                  const now = Date.now()
                  const withinCap = this.psstFirstRearmAt == null || (now - this.psstFirstRearmAt) < MAX_EXTENSION_MS
                  if (withinCap && partialLooksLikeNewUtterance(partialText, this.botMsgs)) {
                    if (this.psstFirstRearmAt == null) this.psstFirstRearmAt = now
                    this.psstRearmCount++
                    _info('psst_timer_extended', {
                      sessionId: this.sessionId,
                      rearmCount: this.psstRearmCount,
                      msSinceFirstRearm: now - this.psstFirstRearmAt,
                      bufferedChunks: this.botMsgs.length,
                      reason: 'new_utterance_partial',
                      partialPreview
                    })
                    // armJoinSilenceTimer() handles clearing the existing timeout.
                    armJoinSilenceTimer()
                  }
                }
              }
            }
            if (_.isNil(this._getIgnoreSilenceDurationAsserterLogicHook(this.convoStep))) {
              if (!this.firstSttInfoReceived && !this._hasJoinLogicHookOrRule(this.convoStep) && this.caps[Capabilities.VOIP_SILENCE_DURATION_TIMEOUT_START_ENABLE] && parsedData.data.start && parsedData.data.start * 1000 > this.caps[Capabilities.VOIP_SILENCE_DURATION_TIMEOUT_START]) {
                this.end = true
                sendBotMsg(new Error(`Silence Duration of ${parsedData.data.start}s exceeded Initial Silence Duration Timeout of ${this.caps[Capabilities.VOIP_SILENCE_DURATION_TIMEOUT_START] / 1000}s`))
              }
            }
            this.firstSttInfoReceived = true
            if (this.prevData && this.prevData.data && !_.isNil(this.prevData.data.end)) {
              if (!_.isNil(parsedData.data.start)) {
                const silenceDuration = parsedData.data.start - this.prevData.data.end
                const sttHandling = this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING]
                const isJoinMethod = sttHandling === 'JOIN' || sttHandling === 'PSST'
                let matched = false
                if (isJoinMethod) {
                  const joinTimeoutMs = this._getEffectiveJoinTimeoutMs(this.convoStep, this.botMsgs)
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
            const msgText = parsedData.data.message || ''
            const replacementResult = this._applySttDictionaryReplacements(msgText)
            const normalizedMsgText = replacementResult.text
            const msgLen = typeof msgText === 'string' ? msgText.length : 0
            const msgPreview = typeof normalizedMsgText === 'string' ? normalizedMsgText.trim() : ''
            // A final supersedes the cached interim; clear to avoid duplicate tail emission.
            this.lastPartialBotMsg = null
            debug(`Message: ${normalizedMsgText} / Confidence Score: ${this._getConfidenceScore(parsedData)} (Threshold: ${confidenceThreshold})`)
            _info('stt_final', {
              sessionId: this.sessionId,
              message: msgPreview,
              messageLength: msgLen,
              confidence: this._getConfidenceScore(parsedData),
              threshold: confidenceThreshold,
              accepted: successfulConfidenceScore,
              segmentEndSec: _.isFinite(_.get(parsedData, 'data.end')) ? parsedData.data.end : null,
              speechEndSec: _.isFinite(_.get(parsedData, 'data.speechEndSec')) ? parsedData.data.speechEndSec : null
            })
            if (successfulConfidenceScore) {
              this._captureSttFinalForReplyTrace(parsedData, msgPreview)
            }
            // ORIGINAL: always emit final message immediately (ignore JOIN hooks/rules).
            if (this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'ORIGINAL') {
              let botMsg = { messageText: normalizedMsgText }
              if (this.firstMsg) {
                const sourceData = this._decorateSourceDataWithSttDictionaryReplacements(parsedData, replacementResult)
                sourceData.silenceDuration = parsedData.data.start
                sourceData.voiceDuration = parsedData.data.end - parsedData.data.start
                botMsg = Object.assign({}, botMsg, { sourceData })
                this.firstMsg = false
              } else {
                const sourceData = this._decorateSourceDataWithSttDictionaryReplacements(parsedData, replacementResult)
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
              let botMsg = { messageText: normalizedMsgText }
              if (this.firstMsg) {
                const sourceData = this._decorateSourceDataWithSttDictionaryReplacements(parsedData, replacementResult)
                sourceData.silenceDuration = parsedData.data.start
                sourceData.voiceDuration = parsedData.data.end - parsedData.data.start
                botMsg = Object.assign({}, botMsg, { sourceData })
                this.firstMsg = false
              } else {
                const sourceData = this._decorateSourceDataWithSttDictionaryReplacements(parsedData, replacementResult)
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
            if (this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'JOIN' || this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'PSST' || this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING] === 'CONCAT') {
              const botMsg = {
                messageText: normalizedMsgText,
                sourceData: this._decorateSourceDataWithSttDictionaryReplacements(parsedData, replacementResult)
              }
              this.prevData = parsedData
              if (successfulConfidenceScore) {
                this.botMsgs.push(botMsg)

                if (this.caps[Capabilities.VOIP_EMIT_SPECULATIVE_TEXT] && this.eventEmitter) {
                  const tentative = joinBotMsg(this.botMsgs, this.joinLastPrevMsg)
                  this._speculativeTurnToken++
                  // speech{Start,End}Sec are audio-clock offsets (seconds since
                  // call connect) taken from the STT worker, so downstream
                  // consumers can align with the recording playhead directly.
                  const firstChunkStart = _.get(this.botMsgs, '[0].sourceData.data.start', null)
                  const thisFrameStart = _.get(parsedData, 'data.start', null)
                  const thisFrameEnd = _.get(parsedData, 'data.end', null)
                  const speechStartSec = _.isFinite(firstChunkStart)
                    ? firstChunkStart
                    : (_.isFinite(thisFrameStart) ? thisFrameStart : null)
                  const speechEndSec = _.isFinite(thisFrameEnd) ? thisFrameEnd : null
                  this.eventEmitter.emit('voip.speculativeBotText', {
                    sessionId: this.sessionId,
                    messageText: tentative.messageText,
                    turnToken: this._speculativeTurnToken,
                    speechStartSec,
                    speechEndSec
                  })
                }

                // New final arrived — reset extension budget for next pause.
                this.psstRearmCount = 0
                this.psstFirstRearmAt = null
                armJoinSilenceTimer()
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
    const hasText = !!(msg && msg.messageText)
    const hasVoiceMedia = !!(msg && msg.media && msg.media.length > 0 && msg.media[0].buffer)
    const hasDtmf = !!(msg && msg.buttons && msg.buttons.length > 0)
    const dtmfMatch = msg && msg.messageText && msg.messageText.match(/<DTMF>([^<]+)<\/DTMF>/i)
    const inputType = hasDtmf || dtmfMatch ? 'dtmf' : (hasText && hasVoiceMedia ? 'mixed' : hasText ? 'text' : hasVoiceMedia ? 'media' : 'unknown')
    const msgPreview = hasText && msg.messageText ? String(msg.messageText).trim().substring(0, 80) : ''
    _info('user_says', {
      sessionId: this.sessionId,
      inputType,
      message: msgPreview || undefined,
      messageLength: hasText && msg.messageText ? msg.messageText.length : undefined,
      mediaSize: hasVoiceMedia && msg.media[0] && Buffer.isBuffer(msg.media[0].buffer) ? msg.media[0].buffer.length : undefined
    })
    this._captureUserSaysStart(msgPreview)
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
        try {
          let duration = 0
          const preferVoiceCapRaw = this.caps[Capabilities.VOIP_USER_INPUT_PREFER_VOICE]
          const preferVoice = !!preferVoiceCapRaw
          const skipTtsForMixedInput = preferVoice && hasText && hasVoiceMedia
          debug(`UserSays routing: hasText=${hasText} hasVoiceMedia=${hasVoiceMedia} preferVoice=${preferVoice} preferVoiceRaw=${JSON.stringify(preferVoiceCapRaw)} skipTtsForMixedInput=${skipTtsForMixedInput}`)

          // Stamp `msg.voipAgent` at the moment bytes leave the WebSocket so
          // the coach can place the agent turn on the recording timeline.
          // `requestedDurationMs` is the best estimate of on-wire playback
          // length (DTMF tones × digits, TTS synth output, parsed media duration).
          const recordingSecNow = () => this._recordingSecNow()
          const stampAgentWire = (wireKind, requestedDurationMs, extras = {}) => {
            const wireRecordingStartSec = recordingSecNow()
            msg.voipAgent = {
              wireSentAtMs: Date.now(),
              inputType,
              wireKind,
              requestedDurationMs: Math.max(0, Math.round(requestedDurationMs || 0)),
              ...(wireRecordingStartSec != null ? { wireRecordingStartSec } : {}),
              ...extras
            }
            this._activeUserSaysVoipAgent = msg.voipAgent
            this._captureAgentWire(msg.voipAgent, inputType)
          }
          const sendAgentWire = (request) => {
            this._sendUserSaysWs(request)
            this._markReplyTrace({ sendAudioAtMs: Date.now() })
            this._logReplyTrace('wire_sent')
          }
          // Twilio default: 100 ms tone + 100 ms gap per digit. Drives agent-bar width only.
          const DTMF_AGENT_BAR_MS_PER_DIGIT = DTMF_MS_PER_DIGIT

          if (msg && msg.buttons && msg.buttons.length > 0) {
            const digits = sanitizeDtmfDigits(msg.buttons[0].payload)
            if (!digits) {
              debug('sendDtmf skipped: no valid DTMF digits after sanitizing button payload')
              return resolve()
            }
            debug(`Sending DTMF digits: ${digits}`)
            const request = JSON.stringify({
              METHOD: 'sendDtmf',
              digits,
              sessionId: this.sessionId
            })
            stampAgentWire('dtmf', digits.length * DTMF_AGENT_BAR_MS_PER_DIGIT, { digitCount: digits.length, dtmfDigits: digits })
            sendAgentWire(request)
            // Wait for DTMF playback plus at least one audioStream flush before slicing turn audio.
            duration = dtmfTurnAudioWaitMs(digits.length) / 1000
          } else if (msg && msg.messageText) {
          // Check for DTMF tag in messageText: <DTMF>1234</DTMF>
            const dtmfMatch = msg.messageText.match(/<DTMF>([^<]+)<\/DTMF>/i)
            if (dtmfMatch && dtmfMatch[1]) {
              const rawDigits = dtmfMatch[1]
              const digits = sanitizeDtmfDigits(rawDigits)
              if (!digits) {
                debug(`sendDtmf skipped: no valid DTMF digits after sanitizing <DTMF> content (raw length=${String(rawDigits).length})`)
                return resolve()
              }
              if (digits !== String(rawDigits).replace(/\s/g, '')) {
                debug(`Sending DTMF from messageText (sanitized): "${rawDigits}" -> "${digits}"`)
              } else {
                debug(`Sending DTMF from messageText: ${digits}`)
              }
              const request = JSON.stringify({
                METHOD: 'sendDtmf',
                digits,
                sessionId: this.sessionId
              })
              stampAgentWire('dtmf', digits.length * DTMF_AGENT_BAR_MS_PER_DIGIT, { digitCount: digits.length, dtmfDigits: digits })
              sendAgentWire(request)
              // Wait for DTMF playback plus at least one audioStream flush before slicing turn audio.
              duration = dtmfTurnAudioWaitMs(digits.length) / 1000
            } else if (!skipTtsForMixedInput) {
              if (!this.axiosTtsParams) {
                if (!(msg.media && msg.media.length > 0 && msg.media[0].buffer)) {
                  return reject(new Error('TTS not configured, only audio input supported'))
                }
              } else {
                debug('UserSays routing: executing TTS branch')
                const ttsRequest = this._buildTtsRequest(msg.messageText)
                if (!ttsRequest) return reject(new Error('TTS not configured, only audio input supported'))
                msg.sourceData = ttsRequest

                let ttsResult = null
                const ttsStartedAt = Date.now()
                this._markReplyTrace({ ttsStartAtMs: ttsStartedAt })
                let ttsSynthMs = 0
                try {
                  ttsResult = await this._getTtsAudio(ttsRequest, msg.messageText)
                  ttsSynthMs = Date.now() - ttsStartedAt
                  this._markReplyTrace({ ttsEndAtMs: Date.now(), ttsSynthMs })
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
                  stampAgentWire('tts', (duration || 0) * 1000, {
                    ttsSynthMs,
                    textLength: msg.messageText ? msg.messageText.length : 0
                  })
                  sendAgentWire(request)
                } else {
                  return reject(new Error('TTS failed, response is empty'))
                }
              }
            } else {
              debug('UserSays routing: skipping TTS for mixed input because VOIP_USER_INPUT_PREFER_VOICE is enabled')
            }
          }
          if (msg && msg.media && msg.media.length > 0 && msg.media[0].buffer) {
            debug('UserSays routing: executing MEDIA branch')
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
            // Stamp now; `requestedDurationMs` is backfilled once media metadata is parsed.
            stampAgentWire('media', 0, { mediaUri: msg.media[0].mediaUri || null })
            sendAgentWire(request)
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
                if (msg.voipAgent) {
                  msg.voipAgent.requestedDurationMs = Math.max(0, Math.round((duration || 0) * 1000))
                }
              } else {
                reject(new Error('Could not determine audio duration from metadata'))
              }
            } catch (err) {
              reject(new Error(`Getting audio duration failed: ${err.message}`))
            }
          }
          const requestedDurationMs = Math.max(0, Math.round((duration || 0) * 1000))
          const isDtmfTurn = msg.voipAgent && msg.voipAgent.wireKind === 'dtmf'

          // After the TTS/media finishes playing, slice the full turn audio
          // (bot spoke first + me just finished) and attach it to this me-step.
          const attachTurnAudioAndResolve = (endSecOverride) => {
            try {
              const fmt = this.audioStream && this.audioStream.format
              const bytesPerSec = fmt ? fmt.sampleRate * fmt.channels * (fmt.bitsPerSample / 8) : null
              const endSec = _.isFinite(endSecOverride)
                ? endSecOverride
                : ((bytesPerSec && this.audioStream && this.audioStream.totalBytes > 0)
                  ? this.audioStream.totalBytes / bytesPerSec
                  : null)
              if (msg.voipAgent) {
                const heardSec = msg.voipAgent.wireKind === 'dtmf' && _.isFinite(msg.voipAgent.playedRecordingStartSec)
                  ? msg.voipAgent.playedRecordingStartSec
                  : this._applyAgentHeardRecordingStartSec(msg.voipAgent, msg.attachments)
                if (!_.isFinite(heardSec) && _.isFinite(endSec) && requestedDurationMs > 0) {
                  const requestedSec = msg.voipAgent.wireKind === 'dtmf'
                    ? this._dtmfPlaybackSec(msg.voipAgent, msg.voipAgent.digitCount || 1)
                    : (requestedDurationMs / 1000)
                  const fromEnd = Math.max(0, endSec - requestedSec)
                  const wireSec = msg.voipAgent.wireRecordingStartSec
                  msg.voipAgent.heardRecordingStartSec = _.isFinite(wireSec)
                    ? Math.max(wireSec, fromEnd)
                    : fromEnd
                } else if (_.isFinite(heardSec) && !_.isFinite(msg.voipAgent.heardRecordingStartSec)) {
                  msg.voipAgent.heardRecordingStartSec = heardSec
                }
              }
              if (msg.voipAgent && _.isFinite(msg.voipAgent.heardRecordingStartSec)) {
                this._markReplyTrace({ heardRecordingStartSec: msg.voipAgent.heardRecordingStartSec })
              }
              this._logReplyTraceHeard(endSec)
              if (this.caps[Capabilities.VOIP_TURN_AUDIO_ENABLE] && _.isFinite(this._lastBotTurnStartSec) && _.isFinite(endSec) && endSec > this._lastBotTurnStartSec) {
                // The real DTMF tone is mixed into full_record by the worker and arrives via the
                // audioStream tail, so the per-turn slice already contains it (the slice end waits for
                // the DTMF tail via _waitForDtmfTurnEndSec). No synthesized tone is injected.
                const audioBase64 = this._sliceTurnAudio(this._lastBotTurnStartSec, endSec)
                if (audioBase64) {
                  this._turnAudioCounter = (this._turnAudioCounter || 0) + 1
                  msg.attachments.push({
                    name: `turn_${this._turnAudioCounter}.wav`,
                    mimeType: 'audio/wav',
                    base64: audioBase64,
                  })
                }
                this._lastBotTurnStartSec = null
              }
            } catch (err) {
              debug(`UserSays: turn audio slice error: ${err && err.message}`)
            }
            this._activeUserSaysVoipAgent = null
            this._finalizeWallPipeline(msg.voipAgent)
            resolve()
          }

          const finalizeUserSays = async () => {
            let endSecOverride = null
            if (isDtmfTurn) {
              const digitCount = msg.voipAgent.digitCount || 1
              const paddingSec = (this.caps[Capabilities.VOIP_TURN_AUDIO_PADDING_MS] || 0) / 1000
              const timeoutMs = dtmfTurnAudioWaitMs(digitCount) + 1500
              endSecOverride = await this._waitForDtmfTurnEndSec(msg.voipAgent, digitCount, paddingSec, timeoutMs)
            }
            attachTurnAudioAndResolve(endSecOverride)
          }

          if (isDtmfTurn) {
            finalizeUserSays()
          } else if (requestedDurationMs <= 0) {
            attachTurnAudioAndResolve()
          } else {
            setTimeout(() => attachTurnAudioAndResolve(), requestedDurationMs)
          }
        } catch (err) {
          reject(err)
        }
      }, 0)
    })
  }

  _recordingSecNow () {
    const fmt = this.audioStream && this.audioStream.format
    const bytesPerSec = fmt ? fmt.sampleRate * fmt.channels * (fmt.bitsPerSample / 8) : null
    if (!bytesPerSec || !this.audioStream || !(this.audioStream.totalBytes > 0)) return null
    return this.audioStream.totalBytes / bytesPerSec
  }

  /**
   * Poll audioStream until DTMF PCM has been flushed through audioStream.
   * Slice end must be derived from playedRecordingStartSec (worker timeline),
   * not wireRecordingStartSec — the WS send can be hundreds of ms before DTMF
   * is injected into full_recorder, and silence after bot can satisfy an early
   * wire-based threshold for single-digit DTMF.
   */
  _dtmfPlaybackSec (voipAgent, digitCount) {
    const playbackMs = voipAgent && voipAgent.playbackRequestedDurationMs
    if (_.isFinite(playbackMs) && playbackMs > 0) return playbackMs / 1000
    return dtmfPlaybackMs(digitCount) / 1000
  }

  _waitForDtmfTurnEndSec (voipAgent, digitCount, paddingSec, timeoutMs) {
    const deadline = Date.now() + timeoutMs
    return new Promise((resolve) => {
      const tick = () => {
        const streamEndSec = this._recordingSecNow()
        const playedSec = voipAgent && voipAgent.playedRecordingStartSec
        const streamSlackSec = (audioStreamIntervalMs() + 50) / 1000

        if (!_.isFinite(playedSec)) {
          if (Date.now() >= deadline) {
            debug(`${this.sessionId} - DTMF turn audio wait timeout without playedRecordingStartSec (digits=${digitCount})`)
            return resolve(streamEndSec)
          }
          return setTimeout(tick, 50)
        }

        const dtmfSec = this._dtmfPlaybackSec(voipAgent, digitCount)
        const minEndSec = playedSec + dtmfSec + paddingSec + streamSlackSec

        if (_.isFinite(streamEndSec) && streamEndSec >= minEndSec) {
          debug(`${this.sessionId} - DTMF turn audio ready at ${streamEndSec.toFixed(3)}s (played=${playedSec.toFixed(3)}s need=${minEndSec.toFixed(3)}s digits=${digitCount} dtmfSec=${dtmfSec.toFixed(3)})`)
          return resolve(streamEndSec)
        }
        if (Date.now() >= deadline) {
          debug(`${this.sessionId} - DTMF turn audio wait timeout at ${streamEndSec != null ? streamEndSec.toFixed(3) : 'null'}s (played=${playedSec.toFixed(3)}s need=${minEndSec.toFixed(3)}s digits=${digitCount})`)
          return resolve(streamEndSec)
        }
        setTimeout(tick, 50)
      }
      tick()
    })
  }

  _agentSpeechRmsThreshold () {
    const raw = process.env.VOIP_AGENT_SPEECH_RMS_THRESHOLD
    const n = raw != null ? Number(raw) : DEFAULT_AGENT_SPEECH_RMS_THRESHOLD
    return Number.isFinite(n) && n > 0 ? n : DEFAULT_AGENT_SPEECH_RMS_THRESHOLD
  }

  _agentSpeechSustainedWindows () {
    const raw = process.env.VOIP_AGENT_SPEECH_SUSTAINED_WINDOWS
    const n = raw != null ? parseInt(raw, 10) : DEFAULT_AGENT_SPEECH_SUSTAINED_WINDOWS
    return Number.isFinite(n) && n > 0 ? n : DEFAULT_AGENT_SPEECH_SUSTAINED_WINDOWS
  }

  _audioStreamBytesPerSec () {
    const fmt = this.audioStream && this.audioStream.format
    if (!fmt) return null
    return fmt.sampleRate * fmt.channels * (fmt.bitsPerSample / 8)
  }

  _pcmBufferRms (pcm, bitsPerSample) {
    if (!pcm || pcm.length < 2 || bitsPerSample !== 16) return 0
    let sum = 0
    let count = 0
    for (let i = 0; i + 1 < pcm.length; i += 2) {
      const sample = pcm.readInt16LE(i)
      sum += sample * sample
      count += 1
    }
    return count > 0 ? Math.sqrt(sum / count) : 0
  }

  _readWavPcmInfo (wavBuffer) {
    if (!wavBuffer || wavBuffer.length < 44) return null
    if (wavBuffer.toString('ascii', 0, 4) !== 'RIFF' || wavBuffer.toString('ascii', 8, 12) !== 'WAVE') {
      return null
    }
    let offset = 12
    let sampleRate = null
    let channels = null
    let bitsPerSample = null
    let dataOffset = null
    let dataLength = null
    while (offset + 8 <= wavBuffer.length) {
      const chunkId = wavBuffer.toString('ascii', offset, offset + 4)
      const chunkSize = wavBuffer.readUInt32LE(offset + 4)
      const chunkStart = offset + 8
      if (chunkId === 'fmt ' && chunkSize >= 16) {
        channels = wavBuffer.readUInt16LE(chunkStart + 2)
        sampleRate = wavBuffer.readUInt32LE(chunkStart + 4)
        bitsPerSample = wavBuffer.readUInt16LE(chunkStart + 14)
      } else if (chunkId === 'data') {
        dataOffset = chunkStart
        dataLength = chunkSize
        break
      }
      offset = chunkStart + chunkSize + (chunkSize % 2)
    }
    if (!sampleRate || !channels || !bitsPerSample || dataOffset == null) return null
    const bytesPerSec = sampleRate * channels * (bitsPerSample / 8)
    if (!bytesPerSec) return null
    const pcmLength = dataLength != null
      ? Math.min(dataLength, wavBuffer.length - dataOffset)
      : (wavBuffer.length - dataOffset)
    return { pcmOffset: dataOffset, pcmLength, bytesPerSec, bitsPerSample, sampleRate, channels }
  }

  _findAudibleLeadInSecFromPcm (pcm, bytesPerSec, bitsPerSample) {
    if (!pcm || !bytesPerSec) return null
    const threshold = this._agentSpeechRmsThreshold()
    const sustainedWindows = this._agentSpeechSustainedWindows()
    const windowBytes = Math.max(2, Math.floor(bytesPerSec * (AGENT_SPEECH_RMS_WINDOW_MS / 1000)))
    const hopBytes = Math.max(2, Math.floor(windowBytes / 2))
    let streak = 0
    let onsetPos = null
    for (let pos = 0; pos + windowBytes <= pcm.length; pos += hopBytes) {
      const rms = this._pcmBufferRms(pcm.subarray(pos, pos + windowBytes), bitsPerSample)
      if (rms >= threshold) {
        if (streak === 0) onsetPos = pos
        streak += 1
        if (streak >= sustainedWindows) {
          return onsetPos / bytesPerSec
        }
      } else {
        streak = 0
        onsetPos = null
      }
    }
    return null
  }

  _findAudibleLeadInSecFromWavBuffer (wavBuffer) {
    const info = this._readWavPcmInfo(wavBuffer)
    if (!info) return null
    const pcm = wavBuffer.subarray(info.pcmOffset, info.pcmOffset + info.pcmLength)
    return this._findAudibleLeadInSecFromPcm(pcm, info.bytesPerSec, info.bitsPerSample)
  }

  _findAudibleRecordingStartSecOnStream (playedSec, wireSec) {
    if (!_.isFinite(playedSec)) return null
    const bytesPerSec = this._audioStreamBytesPerSec()
    const stream = this.audioStream
    if (!bytesPerSec || !stream || !stream.pcmParts.length) return null
    const startByte = Math.max(0, Math.floor(playedSec * bytesPerSec))
    const pcm = Buffer.concat(stream.pcmParts)
    if (startByte >= pcm.length) return null
    const bitsPerSample = stream.format.bitsPerSample
    const leadInSec = this._findAudibleLeadInSecFromPcm(
      pcm.subarray(startByte),
      bytesPerSec,
      bitsPerSample
    )
    if (!_.isFinite(leadInSec)) return null
    let heardSec = playedSec + leadInSec
    if (_.isFinite(wireSec)) heardSec = Math.max(wireSec, heardSec)
    return heardSec
  }

  _findAudibleRecordingStartSecFromAttachments (playedSec, wireSec, attachments) {
    if (!_.isFinite(playedSec) || !_.isArray(attachments)) return null
    const tts = attachments.find((a) => a && a.name === 'tts.wav' && a.base64)
    if (!tts) return null
    try {
      const wavBuffer = Buffer.from(tts.base64, 'base64')
      const leadInSec = this._findAudibleLeadInSecFromWavBuffer(wavBuffer)
      if (!_.isFinite(leadInSec)) return null
      let heardSec = playedSec + leadInSec
      if (_.isFinite(wireSec)) heardSec = Math.max(wireSec, heardSec)
      return heardSec
    } catch (err) {
      debug(`${this.sessionId} - TTS lead-in scan failed: ${err && err.message}`)
      return null
    }
  }

  _resolveAgentHeardRecordingStartSec (voipAgent, attachments) {
    if (!voipAgent || !_.isFinite(voipAgent.playedRecordingStartSec)) return null
    const playedSec = voipAgent.playedRecordingStartSec
    const wireSec = voipAgent.wireRecordingStartSec
    const fromTts = this._findAudibleRecordingStartSecFromAttachments(playedSec, wireSec, attachments)
    const fromStream = this._findAudibleRecordingStartSecOnStream(playedSec, wireSec)
    const candidates = [fromTts, fromStream].filter((s) => _.isFinite(s))
    if (!candidates.length) return null
    // Prefer the later onset — mixed recording can spike before clear TTS speech.
    return Math.max(...candidates)
  }

  _applyAgentHeardRecordingStartSec (voipAgent, attachments) {
    if (!voipAgent || !_.isFinite(voipAgent.playedRecordingStartSec)) return null
    const heardSec = this._resolveAgentHeardRecordingStartSec(voipAgent, attachments)
    if (!_.isFinite(heardSec) || heardSec <= voipAgent.playedRecordingStartSec) {
      return _.isFinite(voipAgent.heardRecordingStartSec) ? voipAgent.heardRecordingStartSec : null
    }
    const prev = voipAgent.heardRecordingStartSec
    if (_.isFinite(prev) && prev >= heardSec) return prev
    voipAgent.heardRecordingStartSec = heardSec
    this._markReplyTrace({ heardRecordingStartSec: heardSec })
    return heardSec
  }

  _maybeDetectAgentAudibleOnRecording (voipAgent) {
    if (!voipAgent || !_.isFinite(voipAgent.playedRecordingStartSec)) return
    this._applyAgentHeardRecordingStartSec(voipAgent)
  }

  _markReplyTrace (patch) {
    if (!this._replyTrace || !patch) return
    Object.assign(this._replyTrace, patch)
  }

  _captureSttFinalForReplyTrace (parsedData, msgPreview) {
    const data = parsedData && parsedData.data
    const atMs = parsedData._receivedAtMs || Date.now()
    const recordingAtSttFinalSec = this._recordingSecNow()
    if (parsedData && _.isFinite(recordingAtSttFinalSec)) {
      parsedData.recordingAtSttFinalSec = recordingAtSttFinalSec
    }
    // Dedicated, self-documenting anchor for the downstream "STT transport" sub-phase
    // (receivedAtMs - finalEmittedWallMs). Unlike the generic _receivedAtMs (stamped on
    // every WS frame), this is set only on the accepted STT-final.
    if (parsedData && _.isFinite(atMs)) {
      parsedData.sttFinalReceivedAtMs = atMs
    }
    this._replyTrace = {
      sessionId: this.sessionId,
      botMessagePreview: msgPreview || undefined,
      sttFinalAtMs: atMs,
      sttRecordingStartSec: _.isFinite(_.get(data, 'start')) ? data.start : null,
      sttRecordingEndSec: _.isFinite(_.get(data, 'end')) ? data.end : null,
      sttSpeechEndSec: _.isFinite(_.get(data, 'speechEndSec')) ? data.speechEndSec : null,
      recordingAtSttFinalSec: _.isFinite(recordingAtSttFinalSec) ? recordingAtSttFinalSec : null,
      queueAtMs: null,
      recordingAtQueueSec: null,
      psstTimerArmedAtMs: null,
      psstScheduledMs: null,
      psstTimerFiredAtMs: null,
      psstFireDelayMs: null,
      userSaysAtMs: null,
      coachWaitMs: null,
      ttsStartAtMs: null,
      ttsEndAtMs: null,
      ttsSynthMs: null,
      wireAtMs: null,
      wireRecordingStartSec: null,
      sendAudioAtMs: null,
      playedRecordingStartSec: null,
      playbackAtMs: null,
      heardRecordingStartSec: null,
      agentEndRecordingSec: null,
      wireKind: null,
      inputType: null,
      requestedDurationMs: null,
      meMessagePreview: null
    }
  }

  _captureBotQueuedForReplyTrace (queuedAt) {
    if (!this._replyTrace) return
    this._replyTrace.queueAtMs = queuedAt
    this._replyTrace.recordingAtQueueSec = this._recordingSecNow()
  }

  _captureUserSaysStart (msgPreview) {
    if (!this._replyTrace) return
    const now = Date.now()
    this._replyTrace.userSaysAtMs = now
    this._replyTrace.meMessagePreview = msgPreview || undefined
    const queueAt = this._replyTrace.queueAtMs || this._lastBotSaysQueuedAt
    if (_.isFinite(queueAt)) {
      if (!this._replyTrace.queueAtMs) this._replyTrace.queueAtMs = queueAt
      this._replyTrace.coachWaitMs = now - queueAt
    }
  }

  _captureAgentWire (voipAgent, inputType) {
    if (!this._replyTrace || !voipAgent) return
    this._replyTrace.wireAtMs = voipAgent.wireSentAtMs
    this._replyTrace.wireRecordingStartSec = voipAgent.wireRecordingStartSec
    this._replyTrace.wireKind = voipAgent.wireKind
    this._replyTrace.inputType = inputType
    this._replyTrace.requestedDurationMs = voipAgent.requestedDurationMs
    if (_.isFinite(voipAgent.ttsSynthMs)) this._replyTrace.ttsSynthMs = voipAgent.ttsSynthMs
  }

  _finalizeWallPipeline (voipAgent) {
    const t = this._replyTrace
    if (!voipAgent || !t) return
    voipAgent.wallPipeline = {
      psstScheduledMs: _.isFinite(t.psstScheduledMs) ? t.psstScheduledMs : null,
      psstFireDelayMs: _.isFinite(t.psstFireDelayMs) ? t.psstFireDelayMs : null,
      coachWaitMs: _.isFinite(t.coachWaitMs) ? t.coachWaitMs : null,
      userSaysAtMs: _.isFinite(t.userSaysAtMs) ? t.userSaysAtMs : null,
      ttsStartAtMs: _.isFinite(t.ttsStartAtMs) ? t.ttsStartAtMs : null,
      ttsEndAtMs: _.isFinite(t.ttsEndAtMs) ? t.ttsEndAtMs : null,
      ttsSynthMs: _.isFinite(t.ttsSynthMs) ? t.ttsSynthMs : null,
      wireAtMs: _.isFinite(t.wireAtMs) ? t.wireAtMs : null,
      sendAudioAtMs: _.isFinite(t.sendAudioAtMs) ? t.sendAudioAtMs : null
    }
  }

  _replyTraceMsFromSttFinal (atMs) {
    const anchor = this._replyTrace && this._replyTrace.sttFinalAtMs
    if (!_.isFinite(anchor) || !_.isFinite(atMs)) return null
    return Math.round(atMs - anchor)
  }

  _replyTraceRecMs (fromSec, toSec) {
    if (!_.isFinite(fromSec) || !_.isFinite(toSec)) return null
    return Math.round((toSec - fromSec) * 1000)
  }

  _logReplyTrace (trigger) {
    const t = this._replyTrace
    if (!t || !_.isFinite(t.sttFinalAtMs)) return
    _info('voip_reply_trace', {
      sessionId: t.sessionId,
      trigger,
      botPreview: t.botMessagePreview,
      mePreview: t.meMessagePreview,
      sttRecordingStartSec: t.sttRecordingStartSec,
      sttRecordingEndSec: t.sttRecordingEndSec,
      sttSpeechEndSec: t.sttSpeechEndSec,
      recordingAtSttFinalSec: t.recordingAtSttFinalSec,
      recordingAtQueueSec: t.recordingAtQueueSec,
      wireRecordingStartSec: t.wireRecordingStartSec,
      wireKind: t.wireKind,
      inputType: t.inputType,
      requestedDurationMs: t.requestedDurationMs,
      ttsSynthMs: t.ttsSynthMs,
      coachWaitMs: t.coachWaitMs,
      psstScheduledMs: t.psstScheduledMs,
      psstFireDelayMs: t.psstFireDelayMs,
      ms_sttFinal_to_queue: this._replyTraceMsFromSttFinal(t.queueAtMs),
      ms_sttFinal_to_psstFire: this._replyTraceMsFromSttFinal(t.psstTimerFiredAtMs),
      ms_sttFinal_to_userSays: this._replyTraceMsFromSttFinal(t.userSaysAtMs),
      ms_sttFinal_to_ttsStart: this._replyTraceMsFromSttFinal(t.ttsStartAtMs),
      ms_sttFinal_to_ttsEnd: this._replyTraceMsFromSttFinal(t.ttsEndAtMs),
      ms_sttFinal_to_wire: this._replyTraceMsFromSttFinal(t.wireAtMs),
      ms_sttFinal_to_sendAudio: this._replyTraceMsFromSttFinal(t.sendAudioAtMs),
      ms_userSays_to_ttsStart: (_.isFinite(t.userSaysAtMs) && _.isFinite(t.ttsStartAtMs))
        ? Math.round(t.ttsStartAtMs - t.userSaysAtMs) : null,
      ms_userSays_to_wire: (_.isFinite(t.userSaysAtMs) && _.isFinite(t.wireAtMs))
        ? Math.round(t.wireAtMs - t.userSaysAtMs) : null,
      ms_queue_to_userSays: t.coachWaitMs,
      recMs_sttEnd_to_queue: this._replyTraceRecMs(t.sttRecordingEndSec, t.recordingAtQueueSec),
      recMs_sttEnd_to_wire: this._replyTraceRecMs(t.sttRecordingEndSec, t.wireRecordingStartSec),
      recMs_speechEnd_to_wire: this._replyTraceRecMs(t.sttSpeechEndSec, t.wireRecordingStartSec)
    })
  }

  _logReplyTraceHeard (agentEndRecordingSec) {
    const t = this._replyTrace
    if (!t || !_.isFinite(t.sttFinalAtMs)) return
    const heardSec = t.heardRecordingStartSec
    const playedSec = t.playedRecordingStartSec
    if (_.isFinite(agentEndRecordingSec)) {
      t.agentEndRecordingSec = agentEndRecordingSec
    }
    _info('voip_reply_trace_heard', {
      sessionId: t.sessionId,
      playedRecordingStartSec: playedSec,
      heardRecordingStartSec: heardSec,
      agentEndRecordingSec: t.agentEndRecordingSec,
      wireRecordingStartSec: t.wireRecordingStartSec,
      sttRecordingEndSec: t.sttRecordingEndSec,
      sttSpeechEndSec: t.sttSpeechEndSec,
      recMs_sttEnd_to_played: this._replyTraceRecMs(t.sttRecordingEndSec, playedSec),
      recMs_speechEnd_to_played: this._replyTraceRecMs(t.sttSpeechEndSec, playedSec),
      recMs_sttEnd_to_heard: this._replyTraceRecMs(t.sttRecordingEndSec, heardSec),
      recMs_sttEnd_to_wire: this._replyTraceRecMs(t.sttRecordingEndSec, t.wireRecordingStartSec),
      recMs_wire_to_played: this._replyTraceRecMs(t.wireRecordingStartSec, playedSec),
      recMs_wire_to_heard: this._replyTraceRecMs(t.wireRecordingStartSec, heardSec)
    })
    this._replyTrace = null
  }

  _voipWsCanSend () {
    return !this.stopCalled && this.ws && this.ws.readyState === WebSocket.OPEN
  }

  _sendUserSaysWs (request) {
    if (!this._voipWsCanSend()) {
      throw new Error('VoIP session stopped')
    }
    this.ws.send(request)
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
      // Final guard: if worker never emitted fullRecordEnd/fullRecord but
      // chunks were buffered, publish the attachment before teardown.
      if (typeof this._emitBufferedFullRecordIfAny === 'function') {
        this._emitBufferedFullRecordIfAny('stop_final_guard')
      }
    } else {
      this.wsOpened = false
      this.ws = null
      this.end = false
      this.convoStep = null
      this.firstSttInfoReceived = false
      if (typeof this._emitBufferedFullRecordIfAny === 'function') {
        this._emitBufferedFullRecordIfAny('stop_final_guard')
      }
    }
    this._emitBufferedFullRecordIfAny = null
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

  // After the first flush, use the shorter SUBSEQUENT timeout if configured.
  // Falls back to the global timeout — fully backward-compatible.
  _getEffectiveMessageHandlingTimeout () {
    if (!this.firstMsg) {
      const sub = parseInt(this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_TIMEOUT_SUBSEQUENT], 10)
      if (_.isFinite(sub) && sub > 0) return sub
    }
    return this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING_TIMEOUT]
  }

  _getJoinLogicHook (convoStep) {
    if (_.isNil(convoStep)) return null
    if (_.isNil(convoStep.logicHooks)) return null
    return convoStep && convoStep.logicHooks && convoStep.logicHooks.find(lh => lh.name === 'VOIP_JOIN_SILENCE_DURATION')
  }

  _normalizeJoinRulesBySubstring () {
    const rawRules = this.caps[Capabilities.VOIP_JOIN_SILENCE_DURATION_BY_SUBSTRING]
    if (_.isNil(rawRules) || rawRules === '') return []
    let parsedRules = rawRules
    if (_.isString(rawRules)) {
      try {
        parsedRules = JSON.parse(rawRules)
      } catch (err) {
        debug(`Invalid ${Capabilities.VOIP_JOIN_SILENCE_DURATION_BY_SUBSTRING} JSON: ${err.message || err}`)
        return []
      }
    }
    if (!_.isArray(parsedRules)) {
      debug(`Invalid ${Capabilities.VOIP_JOIN_SILENCE_DURATION_BY_SUBSTRING}: expected array`)
      return []
    }
    return parsedRules
      .map(rule => {
        if (!rule || typeof rule !== 'object') return null
        const substring = typeof rule.substring === 'string' ? rule.substring.trim() : ''
        const timeoutMs = parseInt(rule.timeoutMs, 10)
        if (!substring || !_.isFinite(timeoutMs) || timeoutMs <= 0) return null
        return {
          substring,
          timeoutMs
        }
      })
      .filter(Boolean)
  }

  // Matches a substring rule against ONLY the latest buffered STT final chunk.
  // This keeps the rule's custom timeout in effect for just the one following
  // final: once a final no longer matches, callers fall back to the default
  // timeout (the convoStep expected text and the cumulative buffer are
  // deliberately not considered, so a stale match cannot stick).
  _getJoinRuleBySubstring (botMsgs) {
    if (!_.isArray(botMsgs) || botMsgs.length === 0) return null
    const last = botMsgs[botMsgs.length - 1]
    const text = (last && typeof last.messageText === 'string') ? last.messageText : ''
    if (!text) return null
    const loweredText = text.toLowerCase()
    const rules = this._normalizeJoinRulesBySubstring()
    return rules.find(rule => loweredText.includes(rule.substring.toLowerCase())) || null
  }

  _normalizeSttDictionaryReplacements () {
    const rawRules = this.caps[Capabilities.VOIP_STT_DICTIONARY_REPLACEMENTS]
    if (_.isNil(rawRules) || rawRules === '') return []
    let parsedRules = rawRules
    if (_.isString(rawRules)) {
      try {
        parsedRules = JSON.parse(rawRules)
      } catch (err) {
        debug(`Invalid ${Capabilities.VOIP_STT_DICTIONARY_REPLACEMENTS} JSON: ${err.message || err}`)
        return []
      }
    }
    if (!_.isArray(parsedRules)) {
      debug(`Invalid ${Capabilities.VOIP_STT_DICTIONARY_REPLACEMENTS}: expected array`)
      return []
    }
    return parsedRules
      .map(rule => {
        if (!rule || typeof rule !== 'object') return null
        const fromValues = _.isArray(rule.from) ? rule.from : [rule.from]
        const from = _.uniq(fromValues
          .map(value => value != null ? String(value).trim() : '')
          .filter(Boolean))
        const to = rule.to != null ? String(rule.to).trim() : ''
        if (from.length === 0 || !to) return null
        return { from, to }
      })
      .filter(Boolean)
  }

  _applySttDictionaryReplacements (text) {
    if (!_.isString(text) || text.length === 0) return { text, applied: [] }
    const replacements = this._normalizeSttDictionaryReplacements()
    if (replacements.length === 0) return { text, applied: [] }

    const replacementsByFrom = new Map()
    const fromAlternatives = []
    replacements.forEach(rule => {
      rule.from.forEach(from => {
        const fromKey = from.toLowerCase()
        if (!replacementsByFrom.has(fromKey)) {
          replacementsByFrom.set(fromKey, rule.to)
          fromAlternatives.push(from)
        }
      })
    })

    if (fromAlternatives.length === 0) return { text, applied: [] }

    fromAlternatives.sort((a, b) => b.length - a.length)
    const matcher = new RegExp(fromAlternatives.map(value => _.escapeRegExp(value)).join('|'), 'gi')
    const applied = []
    const replacedText = text.replace(matcher, match => {
      const to = replacementsByFrom.get(match.toLowerCase())
      applied.push({ from: match, to })
      return to
    })
    return { text: replacedText, applied }
  }

  _decorateSourceDataWithSttDictionaryReplacements (sourceData, replacementResult) {
    if (!replacementResult || !_.isArray(replacementResult.applied) || replacementResult.applied.length === 0) return sourceData
    return Object.assign({}, sourceData, {
      sttDictionaryOriginalMessage: replacementResult.text === sourceData?.data?.message ? undefined : sourceData?.data?.message,
      sttDictionaryReplacements: replacementResult.applied
    })
  }

  _hasJoinLogicHookOrRule (convoStep) {
    return !_.isNil(this._getJoinLogicHook(convoStep)) || !_.isNil(this._getJoinRuleBySubstring(this.botMsgs))
  }

  _toJoinTimeoutMs (ms, isPsst) {
    const parsed = parseInt(ms, 10)
    if (!_.isFinite(parsed) || parsed <= 0) return null
    return isPsst ? Math.max(0, parsed - 500) : parsed
  }

  _getEffectiveJoinTimeoutMs (convoStep, botMsgs) {
    const sttHandling = this.caps[Capabilities.VOIP_STT_MESSAGE_HANDLING]
    const isPsst = sttHandling === 'PSST'
    const joinLogicHook = this._getJoinLogicHook(convoStep)
    if (joinLogicHook && joinLogicHook.args && joinLogicHook.args.length > 0) {
      const joinHookTimeoutMs = this._toJoinTimeoutMs(joinLogicHook.args[0], isPsst)
      if (_.isFinite(joinHookTimeoutMs) && joinHookTimeoutMs > 0) return joinHookTimeoutMs
    }
    const joinRule = this._getJoinRuleBySubstring(botMsgs)
    if (joinRule) {
      const joinRuleTimeoutMs = this._toJoinTimeoutMs(joinRule.timeoutMs, isPsst)
      if (_.isFinite(joinRuleTimeoutMs) && joinRuleTimeoutMs > 0) return joinRuleTimeoutMs
    }
    return this._toJoinTimeoutMs(this._getEffectiveMessageHandlingTimeout(), isPsst)
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

  /**
   * Build a well-formed WAV Buffer from raw PCM bytes and a format descriptor.
   * @param {Buffer} pcm   raw PCM bytes (no header)
   * @param {{ sampleRate: number, channels: number, bitsPerSample: number }} fmt
   * @returns {Buffer}
   */
  _buildWavBuffer (pcm, fmt) {
    const { sampleRate, channels, bitsPerSample } = fmt
    const byteRate = sampleRate * channels * (bitsPerSample / 8)
    const blockAlign = channels * (bitsPerSample / 8)
    const dataSize = pcm.length
    const header = Buffer.alloc(44)
    header.write('RIFF', 0)
    header.writeUInt32LE(36 + dataSize, 4)
    header.write('WAVE', 8)
    header.write('fmt ', 12)
    header.writeUInt32LE(16, 16)          // fmt chunk size
    header.writeUInt16LE(1, 20)           // PCM format
    header.writeUInt16LE(channels, 22)
    header.writeUInt32LE(sampleRate, 24)
    header.writeUInt32LE(byteRate, 28)
    header.writeUInt16LE(blockAlign, 32)
    header.writeUInt16LE(bitsPerSample, 34)
    header.write('data', 36)
    header.writeUInt32LE(dataSize, 40)
    return Buffer.concat([header, pcm])
  }

  /**
   * Slice a segment of the continuously buffered PCM audio stream and return
   * it as a base64-encoded WAV string.
   *
   * @param {number} startSec  start of the segment (seconds from call connect)
   * @param {number} endSec    end of the segment (seconds from call connect)
   * @returns {string|null}    base64 WAV or null if the stream is not ready
   */
  _sliceTurnAudio (startSec, endSec) {
    const stream = this.audioStream
    if (!stream || !stream.format || !stream.pcmParts || !stream.pcmParts.length) return null
    if (!_.isFinite(startSec) || !_.isFinite(endSec) || endSec <= startSec) return null

    const { sampleRate, channels, bitsPerSample } = stream.format
    const bytesPerSec = sampleRate * channels * (bitsPerSample / 8)
    const frameBytes = channels * (bitsPerSample / 8)

    const offsetSec = (this.caps[Capabilities.VOIP_TURN_AUDIO_OFFSET_MS] || 0) / 1000
    const paddingSec = (this.caps[Capabilities.VOIP_TURN_AUDIO_PADDING_MS] || 0) / 1000

    const adjStart = Math.max(0, startSec + offsetSec)
    const adjEnd = endSec + paddingSec

    // Frame-align the byte boundaries.
    const startByte = Math.floor(adjStart * bytesPerSec / frameBytes) * frameBytes
    const endByte = Math.ceil(adjEnd * bytesPerSec / frameBytes) * frameBytes

    if (startByte >= stream.totalBytes) {
      debug(`${this.sessionId} - _sliceTurnAudio: startByte ${startByte} >= totalBytes ${stream.totalBytes}, skipping`)
      return null
    }

    const clampedEnd = Math.min(endByte, stream.totalBytes)
    const sliceLen = clampedEnd - startByte
    if (sliceLen <= 0) return null

    // Materialise only the bytes we need from the part list.
    const pcm = Buffer.allocUnsafe(sliceLen)
    let written = 0
    let offset = 0
    for (const part of stream.pcmParts) {
      const partEnd = offset + part.length
      if (partEnd <= startByte) {
        offset += part.length
        continue
      }
      if (offset >= clampedEnd) break
      const copyFrom = Math.max(0, startByte - offset)
      const copyTo = Math.min(part.length, clampedEnd - offset)
      part.copy(pcm, written, copyFrom, copyTo)
      written += copyTo - copyFrom
      offset += part.length
    }

    if (written === 0) return null
    const slicedPcm = written < sliceLen ? pcm.slice(0, written) : pcm
    const wavBuf = this._buildWavBuffer(slicedPcm, { sampleRate, channels, bitsPerSample })
    return wavBuf.toString('base64')
  }
}

module.exports = BotiumConnectorVoip
