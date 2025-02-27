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
      }
    ]
  },
  PluginLogicHooks: {
    VOIP_IGNORE_SILENCE_DURATION: IgnoreSilenceDurationLogicHook,
    VOIP_JOIN_SILENCE_DURATION: JoinLogicHook,
    VOIP_CONFIDENCE_THRESHOLD: ConfidenceThresholdLogicHook
  }
}
