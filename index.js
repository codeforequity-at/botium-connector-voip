const BotiumConnectorVoip = require('./src/connector')

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
  }
}
