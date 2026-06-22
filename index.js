const BotiumConnectorVoip = require('./src/connector')
const IgnoreSilenceDurationLogicHook = require('./src/ignore-sd-logichook')
const JoinLogicHook = require('./src/join-logichook')
const ConfidenceThresholdLogicHook = require('./src/confidence-threshold-logichook')

module.exports = {
  PluginVersion: 1,
  PluginClass: BotiumConnectorVoip,
  PluginDesc: {
    name: 'Voice over IP',
    provider: 'Botium',
    features: {
      audioInput: true,
      e2eTesting: true
    },
    capabilities: [
      {
        name: 'VOIP_STT',
        label: 'Speech Recognition Profile',
        type: 'speechrecognitionprofile',
        required: true
      },
      {
        name: 'VOIP_TTS',
        label: 'Speech Synthesis Profile',
        type: 'speechsynthesisprofile',
        required: true
      },
      {
        name: 'VOIP_USER_INPUT_PREFER_VOICE',
        label: 'Prefer voice media when both text and voice present',
        type: 'boolean',
        required: false,
        advanced: true
      },
      {
        name: 'VOIP_JOIN_SILENCE_DURATION_BY_SUBSTRING',
        label: 'Join silence timeout rules by bot message substring',
        type: 'json',
        required: false,
        advanced: true
      },
      {
        name: 'VOIP_STT_DICTIONARY_REPLACEMENTS',
        label: 'STT dictionary replacements',
        type: 'json',
        required: false,
        advanced: true
      },
      {
        name: 'VOIP_SDP_MEDIA_TYPE_TEXT_ENABLE',
        label: 'Enable SDP media type text',
        type: 'boolean',
        required: false,
        advanced: true
      },
      {
        name: 'VOIP_TURN_AUDIO_ENABLE',
        label: 'Attach per-turn audio to each transcript message',
        type: 'boolean',
        required: false,
        advanced: true
      },
      {
        name: 'VOIP_TURN_AUDIO_PADDING_MS',
        label: 'Extra milliseconds appended after each turn audio slice (absorbs STT boundary jitter)',
        type: 'int',
        required: false,
        advanced: true
      },
      {
        name: 'VOIP_TURN_AUDIO_OFFSET_MS',
        label: 'Millisecond offset applied to every turn audio start time (positive = shift right)',
        type: 'int',
        required: false,
        advanced: true
      }
    ]
  },
  PluginLogicHooks: {
    VOIP_IGNORE_SILENCE_DURATION: IgnoreSilenceDurationLogicHook,
    VOIP_JOIN_SILENCE_DURATION: JoinLogicHook,
    VOIP_CONFIDENCE_THRESHOLD: ConfidenceThresholdLogicHook
  }
}
