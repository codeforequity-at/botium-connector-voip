const BotiumConnectorVoip = require('./connector')
const { NamoDetector, DEFAULT_MODEL_ID, DEFAULT_MODEL_REVISION } = require('./namo-detector')

const HANDLING_NAMO = 'NAMO'
const DEFAULT_THRESHOLD = 0.85
const DEFAULT_MIN_WAIT_MS = 250
const DEFAULT_MAX_WAIT_MS = 8000

const numberCapability = (caps, name, fallback, { min = 0, max = Number.MAX_SAFE_INTEGER } = {}) => {
  const parsed = Number(caps[name])
  return Number.isFinite(parsed) && parsed >= min && parsed <= max ? parsed : fallback
}

const logValue = value => {
  if (value === undefined) return null
  if (typeof value === 'string') return JSON.stringify(value)
  try {
    return JSON.stringify(value)
  } catch (err) {
    return JSON.stringify(String(value))
  }
}

const info = (event, data = {}) => {
  const fields = Object.entries(data)
    .map(([key, value]) => value === undefined ? null : `${key}=${logValue(value)}`)
    .filter(Boolean)
  console.info(`[voip] event=${JSON.stringify(event)}${fields.length ? ` ${fields.join(' ')}` : ''}`)
}

const sourceDataParts = message => {
  if (!message || message.sourceData == null) return []
  return Array.isArray(message.sourceData) ? message.sourceData : [message.sourceData]
}

class BotiumConnectorVoipWithNamo {
  constructor ({ queueBotSays, eventEmitter, caps }) {
    this.queueBotSays = queueBotSays
    this.eventEmitter = eventEmitter
    this.caps = caps || {}
    this.namoEnabled = String(this.caps.VOIP_STT_MESSAGE_HANDLING || '').toUpperCase() === HANDLING_NAMO
    this.threshold = numberCapability(this.caps, 'VOIP_NAMO_EOU_THRESHOLD', DEFAULT_THRESHOLD, { min: 0, max: 1 })
    this.minWaitMs = numberCapability(this.caps, 'VOIP_NAMO_MIN_WAIT_MS', DEFAULT_MIN_WAIT_MS, { min: 0 })
    this.maxWaitMs = numberCapability(this.caps, 'VOIP_NAMO_MAX_WAIT_MS', DEFAULT_MAX_WAIT_MS, { min: 1 })
    this.delimiter = this.caps.VOIP_STT_MESSAGE_HANDLING_DELIMITER || '. '
    this.pendingMessages = []
    this.pendingStartedAt = null
    this.candidateVersion = 0
    this.decisionTimer = null
    this.maxWaitTimer = null
    this.decisionChain = Promise.resolve()

    const baseCaps = {
      ...this.caps,
      // Namo owns message joining. The base connector should release every
      // accepted STT final without imposing a second silence timer.
      ...(this.namoEnabled ? { VOIP_STT_MESSAGE_HANDLING: 'ORIGINAL' } : {})
    }
    this.base = new BotiumConnectorVoip({
      queueBotSays: message => this._handleBaseMessage(message),
      eventEmitter,
      caps: baseCaps
    })

    Object.defineProperty(this, 'container', {
      configurable: true,
      get: () => this.base.container,
      set: value => { this.base.container = value }
    })

    if (this.namoEnabled) {
      this.detector = new NamoDetector({
        modelId: this.caps.VOIP_NAMO_MODEL_ID || DEFAULT_MODEL_ID,
        revision: this.caps.VOIP_NAMO_MODEL_REVISION || DEFAULT_MODEL_REVISION,
        modelPath: this.caps.VOIP_NAMO_MODEL_PATH || null,
        cacheDir: this.caps.VOIP_NAMO_CACHE_DIR || process.env.BOTIUM_NAMO_CACHE_DIR,
        log: info
      })
      info('namo_mode_enabled', {
        threshold: this.threshold,
        minWaitMs: this.minWaitMs,
        maxWaitMs: this.maxWaitMs,
        modelId: this.caps.VOIP_NAMO_MODEL_ID || DEFAULT_MODEL_ID,
        baseHandling: 'ORIGINAL'
      })
    }
  }

  async Validate () {
    return this.base.Validate()
  }

  async Start () {
    if (this.namoEnabled) {
      try {
        await this.detector.init()
      } catch (err) {
        const fallbackHandling = this.caps.VOIP_NAMO_FALLBACK_HANDLING || 'PSST'
        this.namoEnabled = false
        this.base.caps.VOIP_STT_MESSAGE_HANDLING = fallbackHandling
        info('namo_model_error', {
          phase: 'initialization',
          error: err.message,
          action: 'fallback',
          fallbackHandling
        })
      }
    }
    return this.base.Start()
  }

  async UserSays (message) {
    return this.base.UserSays(message)
  }

  async Stop () {
    await this.base.Stop()
    await new Promise(resolve => setTimeout(resolve, 0))
    await this.decisionChain
    this._flushPending('connector_stop')
  }

  _clearDecisionTimer () {
    if (this.decisionTimer) {
      clearTimeout(this.decisionTimer)
      this.decisionTimer = null
    }
  }

  _clearMaxWaitTimer () {
    if (this.maxWaitTimer) {
      clearTimeout(this.maxWaitTimer)
      this.maxWaitTimer = null
    }
  }

  _handleBaseMessage (message) {
    if (!this.namoEnabled || !message || message instanceof Error) {
      if (message instanceof Error) this._flushPending('base_error')
      this.queueBotSays(message)
      return
    }

    if (typeof message.messageText !== 'string' || !message.messageText.trim()) {
      this.queueBotSays(message)
      return
    }

    this.pendingMessages.push(message)
    this.candidateVersion++
    if (this.pendingStartedAt == null) {
      this.pendingStartedAt = Date.now()
      this._armMaxWaitTimer()
    }

    const text = this._joinedText()
    info('namo_candidate_received', {
      candidateVersion: this.candidateVersion,
      bufferedChunks: this.pendingMessages.length,
      textLength: text.length,
      preview: text.substring(0, 160)
    })
    this._scheduleDecision()
  }

  _joinedText () {
    return this.pendingMessages.map(message => message.messageText).join(this.delimiter)
  }

  _armMaxWaitTimer () {
    this._clearMaxWaitTimer()
    const armedAt = Date.now()
    this.maxWaitTimer = setTimeout(() => {
      info('namo_max_wait_fired', {
        maxWaitMs: this.maxWaitMs,
        actualWaitMs: Date.now() - armedAt,
        bufferedChunks: this.pendingMessages.length,
        action: 'flush'
      })
      this._flushPending('max_wait')
    }, this.maxWaitMs)

    info('namo_max_wait_armed', {
      maxWaitMs: this.maxWaitMs,
      bufferedChunks: this.pendingMessages.length,
      armedAt
    })
    if (this.eventEmitter) {
      this.eventEmitter.emit('voip.namoTimerArmed', {
        maxWaitMs: this.maxWaitMs,
        bufferedChunks: this.pendingMessages.length,
        armedAt
      })
      // Preserve compatibility with the existing VoipWaitTracker until it
      // learns the semantic gate event explicitly.
      this.eventEmitter.emit('voip.psstTimerArmed', {
        joinTimeoutMs: this.maxWaitMs,
        bufferedChunks: this.pendingMessages.length,
        armedAt,
        strategy: HANDLING_NAMO
      })
    }
  }

  _scheduleDecision () {
    this._clearDecisionTimer()
    const version = this.candidateVersion
    this.decisionTimer = setTimeout(() => {
      this.decisionTimer = null
      this.decisionChain = this.decisionChain
        .then(() => this._evaluateCandidate(version))
        .catch(err => this._handleInferenceError(err))
    }, this.minWaitMs)
  }

  async _evaluateCandidate (version) {
    if (!this.pendingMessages.length || version !== this.candidateVersion) return
    const text = this._joinedText()
    const result = await this.detector.predict(text)

    if (version !== this.candidateVersion) {
      info('namo_decision_stale', {
        evaluatedVersion: version,
        currentVersion: this.candidateVersion,
        inferenceMs: result.inferenceMs
      })
      return
    }

    const complete = result.eouProbability >= this.threshold
    const decision = complete ? 'complete' : 'incomplete'
    info('namo_decision', {
      candidateVersion: version,
      decision,
      eouProbability: Number(result.eouProbability.toFixed(6)),
      incompleteProbability: Number(result.incompleteProbability.toFixed(6)),
      threshold: this.threshold,
      inferenceMs: result.inferenceMs,
      bufferedChunks: this.pendingMessages.length,
      bufferedMs: this.pendingStartedAt == null ? 0 : Date.now() - this.pendingStartedAt,
      action: complete ? 'flush' : 'hold',
      preview: text.substring(0, 160)
    })
    if (this.eventEmitter) {
      this.eventEmitter.emit('voip.namoDecision', {
        decision,
        eouProbability: result.eouProbability,
        incompleteProbability: result.incompleteProbability,
        threshold: this.threshold,
        inferenceMs: result.inferenceMs,
        bufferedChunks: this.pendingMessages.length
      })
    }
    if (complete) {
      this._flushPending('model_complete', result)
    }
  }

  _handleInferenceError (err) {
    info('namo_model_error', {
      phase: 'inference',
      error: err.message,
      action: 'fail_open'
    })
    this.namoEnabled = false
    this._flushPending('inference_error')
  }

  _joinedMessage (reason, result) {
    const first = this.pendingMessages[0]
    const sourceData = this.pendingMessages.flatMap(sourceDataParts)
    const message = {
      ...first,
      messageText: this._joinedText(),
      sourceData: sourceData.length === 1 ? sourceData[0] : sourceData,
      namoGate: {
        reason,
        bufferedChunks: this.pendingMessages.length,
        bufferedMs: this.pendingStartedAt == null ? 0 : Date.now() - this.pendingStartedAt,
        threshold: this.threshold,
        ...(result
          ? {
              eouProbability: result.eouProbability,
              incompleteProbability: result.incompleteProbability,
              inferenceMs: result.inferenceMs
            }
          : {})
      }
    }
    return message
  }

  _flushPending (reason, result) {
    if (!this.pendingMessages.length) return
    this._clearDecisionTimer()
    this._clearMaxWaitTimer()
    const message = this._joinedMessage(reason, result)
    info('namo_flush', {
      reason,
      bufferedChunks: this.pendingMessages.length,
      bufferedMs: message.namoGate.bufferedMs,
      textLength: message.messageText.length,
      eouProbability: result && Number(result.eouProbability.toFixed(6)),
      preview: message.messageText.substring(0, 160)
    })
    this.pendingMessages = []
    this.pendingStartedAt = null
    this.candidateVersion++
    this.queueBotSays(message)
  }
}

module.exports = BotiumConnectorVoipWithNamo
