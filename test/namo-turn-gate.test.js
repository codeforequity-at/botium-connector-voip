const { test } = require('node:test')
const assert = require('node:assert/strict')
const { NamoTurnGate, TURN_SOFT_ENDED } = require('../src/turn-handlers/namo-turn-gate')

const Capabilities = {
  VOIP_NAMO_EOU_THRESHOLD: 'VOIP_NAMO_EOU_THRESHOLD',
  VOIP_NAMO_MIN_WAIT_MS: 'VOIP_NAMO_MIN_WAIT_MS',
  VOIP_NAMO_MAX_WAIT_MS: 'VOIP_NAMO_MAX_WAIT_MS',
  VOIP_NAMO_QUESTION_FLUSH_MS: 'VOIP_NAMO_QUESTION_FLUSH_MS',
  VOIP_NAMO_QUESTION_MIN_CHARS: 'VOIP_NAMO_QUESTION_MIN_CHARS',
  VOIP_NAMO_EMIT_STABLE_MS: 'VOIP_NAMO_EMIT_STABLE_MS',
  VOIP_NAMO_GAP_OUTLIER_FACTOR: 'VOIP_NAMO_GAP_OUTLIER_FACTOR',
  VOIP_NAMO_REOPEN_MS: 'VOIP_NAMO_REOPEN_MS',
  VOIP_NAMO_REOPEN_FAST_MS: 'VOIP_NAMO_REOPEN_FAST_MS',
  VOIP_NAMO_REOPEN_FAST_EOU: 'VOIP_NAMO_REOPEN_FAST_EOU',
  VOIP_NAMO_MIN_SEGMENT_CHARS: 'VOIP_NAMO_MIN_SEGMENT_CHARS',
  VOIP_NAMO_MAX_WAIT_VAD_EXTENSION_MS: 'VOIP_NAMO_MAX_WAIT_VAD_EXTENSION_MS',
  VOIP_NAMO_JOINED_QUESTION_FLUSH_ENABLE: 'VOIP_NAMO_JOINED_QUESTION_FLUSH_ENABLE',
  VOIP_NAMO_JOINED_QUESTION_FLUSH_MIN_CONFIDENCE: 'VOIP_NAMO_JOINED_QUESTION_FLUSH_MIN_CONFIDENCE',
  VOIP_NAMO_JOINED_QUESTION_MAX_CHUNKS: 'VOIP_NAMO_JOINED_QUESTION_MAX_CHUNKS',
  VOIP_NAMO_DTMF_ECHO_MS: 'VOIP_NAMO_DTMF_ECHO_MS',
  VOIP_NAMO_VAD_SHORT_SEGMENT_MERGE_MS: 'VOIP_NAMO_VAD_SHORT_SEGMENT_MERGE_MS',
  VOIP_NAMO_VAD_ENABLE: 'VOIP_NAMO_VAD_ENABLE',
  VOIP_STT_MESSAGE_HANDLING_DELIMITER: 'VOIP_STT_MESSAGE_HANDLING_DELIMITER'
}

const makeGate = (overrides = {}) => {
  const messages = [{ messageText: 'Hello', sourceData: { data: { start: 0, end: 1 } } }]
  const commits = []
  const detector = {
    predict: async (text) => ({
      eouProbability: text.includes('complete') ? 0.95 : 0.2,
      incompleteProbability: 0.8,
      inferenceMs: 1
    })
  }
  let vadActive = false
  const gate = new NamoTurnGate({
    caps: {
      [Capabilities.VOIP_NAMO_EOU_THRESHOLD]: 0.85,
      [Capabilities.VOIP_NAMO_MIN_WAIT_MS]: 0,
      [Capabilities.VOIP_NAMO_MAX_WAIT_MS]: 30000,
      [Capabilities.VOIP_NAMO_REOPEN_MS]: 50,
      [Capabilities.VOIP_NAMO_REOPEN_FAST_MS]: 10,
      [Capabilities.VOIP_NAMO_REOPEN_FAST_EOU]: 0.99,
      [Capabilities.VOIP_NAMO_EMIT_STABLE_MS]: 0,
      [Capabilities.VOIP_NAMO_VAD_ENABLE]: true,
      [Capabilities.VOIP_STT_MESSAGE_HANDLING_DELIMITER]: ' '
    },
    Capabilities,
    detector,
    getMessages: () => messages,
    commitFlush: (msg) => commits.push(msg),
    getVadSpeechActive: () => vadActive,
    getVadEnabled: () => true,
    getLastVadSpeechEndAt: () => null,
    setLastVadSpeechEndAt: () => {},
    eventEmitter: null,
    sessionId: 'test',
    _info: () => {},
    onInferenceError: () => {},
    ...overrides
  })
  return { gate, messages, commits, setVadActive: (v) => { vadActive = v } }
}

test('reopen during soft_ended keeps a single commit after speech resumes', async () => {
  const { gate, messages, commits, setVadActive } = makeGate()
  gate.onFinalBuffered()
  setVadActive(false)
  gate._softEndTurn('model_complete', { eouProbability: 0.9, incompleteProbability: 0.1, inferenceMs: 1 })
  assert.equal(gate.turnState, TURN_SOFT_ENDED)
  messages.push({ messageText: 'world', sourceData: { data: { start: 1, end: 2 } } })
  setVadActive(true)
  gate.onVadSpeechStart()
  assert.equal(gate.turnState, 'buffering')
  await new Promise((r) => setTimeout(r, 80))
  assert.equal(commits.length, 0)
})

test('shouldSuppressSttText drops digit-only echo after agent DTMF', () => {
  const { gate } = makeGate()
  gate.noteAgentDtmf()
  assert.equal(gate.shouldSuppressSttText('4012'), true)
  assert.equal(gate.shouldSuppressSttText('Please wait'), false)
})

test('_looksLikeQuestionCandidate rejects short How can I fragment', () => {
  const { gate } = makeGate({
    caps: {
      [Capabilities.VOIP_NAMO_EOU_THRESHOLD]: 0.85,
      [Capabilities.VOIP_NAMO_QUESTION_MIN_CHARS]: 28,
      [Capabilities.VOIP_NAMO_VAD_ENABLE]: true
    }
  })
  assert.equal(gate._looksLikeQuestionCandidate('How can I?', 'How can I?'), false)
  assert.equal(
    gate._looksLikeQuestionCandidate(
      'What is the issue you are calling about today?',
      'What is the issue you are calling about today?'
    ),
    true
  )
})

test('_resolveReopenDelay uses fast reopen for high EOU', () => {
  const { gate } = makeGate()
  assert.equal(gate._resolveReopenDelay({ eouProbability: 0.995 }), 10)
  assert.equal(gate._resolveReopenDelay({ eouProbability: 0.5 }), 50)
})

test('short first final defers max-wait clock until second final', () => {
  const messages = [{ messageText: 'Sorry.', sourceData: {} }]
  const { gate, commits } = makeGate({
    caps: {
      [Capabilities.VOIP_NAMO_EOU_THRESHOLD]: 0.85,
      [Capabilities.VOIP_NAMO_MIN_SEGMENT_CHARS]: 12,
      [Capabilities.VOIP_NAMO_MIN_WAIT_MS]: 0,
      [Capabilities.VOIP_NAMO_MAX_WAIT_MS]: 5000,
      [Capabilities.VOIP_NAMO_VAD_ENABLE]: true
    },
    getMessages: () => messages
  })
  gate.onFinalBuffered()
  assert.equal(gate.pendingStartedAt, null)
  messages.push({ messageText: 'Please wait while I connect you.', sourceData: {} })
  gate.onFinalBuffered()
  assert.notEqual(gate.pendingStartedAt, null)
  assert.equal(commits.length, 0)
})

test('max_wait does not extend when VAD quiet but decision incomplete', async () => {
  const messages = [{ messageText: 'still talking here', sourceData: {} }]
  const { gate, commits } = makeGate({
    caps: {
      [Capabilities.VOIP_NAMO_EOU_THRESHOLD]: 0.85,
      [Capabilities.VOIP_NAMO_MIN_WAIT_MS]: 0,
      [Capabilities.VOIP_NAMO_MAX_WAIT_MS]: 80,
      [Capabilities.VOIP_NAMO_MAX_WAIT_VAD_EXTENSION_MS]: 120,
      [Capabilities.VOIP_NAMO_EMIT_STABLE_MS]: 0,
      [Capabilities.VOIP_NAMO_VAD_ENABLE]: true
    },
    getMessages: () => messages,
    getVadSpeechActive: () => false
  })
  gate.onFinalBuffered()
  gate.lastDecisionComplete = false
  await new Promise((r) => setTimeout(r, 100))
  assert.equal(gate.maxWaitExtended, false)
  assert.equal(commits.length, 1)
  assert.equal(commits[0].namoGate.reason, 'max_wait')
})

test('max_wait extends once when VAD active at primary deadline', async () => {
  const messages = [{ messageText: 'still talking here', sourceData: {} }]
  let vadActive = true
  const { gate, commits } = makeGate({
    caps: {
      [Capabilities.VOIP_NAMO_EOU_THRESHOLD]: 0.85,
      [Capabilities.VOIP_NAMO_MIN_WAIT_MS]: 0,
      [Capabilities.VOIP_NAMO_MAX_WAIT_MS]: 80,
      [Capabilities.VOIP_NAMO_MAX_WAIT_VAD_EXTENSION_MS]: 120,
      [Capabilities.VOIP_NAMO_VAD_ENABLE]: true
    },
    getMessages: () => messages,
    getVadSpeechActive: () => vadActive
  })
  gate.onFinalBuffered()
  gate.lastDecisionComplete = false
  await new Promise((r) => setTimeout(r, 100))
  assert.equal(gate.maxWaitExtended, true)
  assert.equal(commits.length, 0)
  vadActive = false
  gate.lastDecisionComplete = false
  await new Promise((r) => setTimeout(r, 150))
  assert.equal(commits.length, 1)
  assert.equal(commits[0].namoGate.reason, 'max_wait')
})

test('joined question STT flush commits two-chunk IVR menu without max_wait', async () => {
  const messages = [
    { messageText: 'Para espanol O prima El ocho.', sourceData: { sttConfidence: 0.32 } }
  ]
  const detector = {
    predict: async (text) => ({
      eouProbability: text.includes('espan') && text.includes('issue') ? 0.00003 : 0.2,
      incompleteProbability: 0.99,
      inferenceMs: 1
    })
  }
  const { gate, commits } = makeGate({
    caps: {
      [Capabilities.VOIP_NAMO_EOU_THRESHOLD]: 0.85,
      [Capabilities.VOIP_NAMO_MIN_WAIT_MS]: 0,
      [Capabilities.VOIP_NAMO_MAX_WAIT_MS]: 60000,
      [Capabilities.VOIP_NAMO_EMIT_STABLE_MS]: 25,
      [Capabilities.VOIP_NAMO_JOINED_QUESTION_FLUSH_ENABLE]: true,
      [Capabilities.VOIP_NAMO_JOINED_QUESTION_FLUSH_MIN_CONFIDENCE]: 0.85,
      [Capabilities.VOIP_STT_MESSAGE_HANDLING_DELIMITER]: '.. ',
      [Capabilities.VOIP_NAMO_VAD_ENABLE]: true
    },
    getMessages: () => messages,
    detector,
    getVadSpeechActive: () => false
  })
  gate.onFinalBuffered()
  messages.push({ messageText: "What's the issue you're calling about?", sourceData: { sttConfidence: 0.91 } })
  gate.onFinalBuffered()
  await new Promise((r) => setTimeout(r, 80))
  assert.equal(commits.length, 1)
  assert.match(commits[0].messageText, /issue you're calling about/i)
  assert.match(commits[0].messageText, /espan/i)
  assert.equal(commits[0].namoGate.reason, 'model_complete')
})

test('max_wait clock anchors and re-anchors to speech end ms', () => {
  const messages = [{ messageText: 'Para espanol prima el ocho.', sourceData: {} }]
  const t0 = Date.now()
  let speechEndAtMs = t0 - 200
  const { gate } = makeGate({
    caps: {
      [Capabilities.VOIP_NAMO_MIN_SEGMENT_CHARS]: 4,
      [Capabilities.VOIP_NAMO_MAX_WAIT_MS]: 60000,
      [Capabilities.VOIP_NAMO_VAD_ENABLE]: true
    },
    getMessages: () => messages,
    getSpeechEndAtMs: () => speechEndAtMs
  })
  gate.onFinalBuffered()
  assert.equal(gate.pendingStartedAt, speechEndAtMs)
  speechEndAtMs = t0 + 800
  messages.push({ messageText: 'What is the issue you are calling about?', sourceData: {} })
  gate.onFinalBuffered()
  assert.equal(gate.pendingStartedAt, speechEndAtMs)
})

test('max_wait flush respects reply connector deadline cap', async () => {
  const messages = [{ messageText: 'still talking here', sourceData: {} }]
  const armAt = Date.now()
  const { gate, commits } = makeGate({
    caps: {
      [Capabilities.VOIP_NAMO_EOU_THRESHOLD]: 0.85,
      [Capabilities.VOIP_NAMO_MIN_WAIT_MS]: 0,
      [Capabilities.VOIP_NAMO_MAX_WAIT_MS]: 5000,
      [Capabilities.VOIP_NAMO_EMIT_STABLE_MS]: 0,
      [Capabilities.VOIP_NAMO_VAD_ENABLE]: true
    },
    getMessages: () => messages,
    getVadSpeechActive: () => false,
    getSpeechEndAtMs: () => armAt - 4900,
    getReplyConnectorDeadlineAtMs: () => armAt + 40
  })
  gate.onFinalBuffered()
  await new Promise((r) => setTimeout(r, 80))
  assert.equal(commits.length, 1)
  assert.equal(commits[0].namoGate.reason, 'max_wait')
})
