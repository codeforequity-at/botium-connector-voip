const assert = require('node:assert/strict')
const { EventEmitter } = require('node:events')
const test = require('node:test')

const BotiumConnectorVoip = require('../src/connector')

const createConnector = () => new BotiumConnectorVoip({
  queueBotSays: () => {},
  eventEmitter: new EventEmitter(),
  caps: {}
})

test('applies configured effects after TTS and returns the converted audio', async () => {
  const connector = createConnector()
  const requests = []
  connector._axios = async (request) => {
    requests.push(request)
    if (request.url === 'https://speech.example/tts') {
      return {
        data: Buffer.from('plain-audio'),
        headers: { 'content-type': 'audio/wav', 'content-duration': '1' }
      }
    }
    assert.equal(request.url, 'https://speech.example/effect')
    assert.equal(request.data.toString(), 'plain-audio')
    assert.equal(request.headers['Content-Type'], 'audio/wav')
    return {
      data: Buffer.from('converted-audio'),
      headers: { 'content-type': 'audio/wav', 'content-duration': '2' }
    }
  }
  connector.axiosTtsEffectsParams = {
    url: 'https://speech.example/effect',
    method: 'POST',
    responseType: 'arraybuffer'
  }

  const result = await connector._fetchTts({
    url: 'https://speech.example/tts',
    method: 'GET',
    responseType: 'arraybuffer'
  }, 'hello')

  assert.equal(requests.length, 2)
  assert.equal(result.buffer.toString(), 'converted-audio')
  assert.equal(result.duration, '2')
})

test('returns original TTS audio when no effects are configured', async () => {
  const connector = createConnector()
  connector._axios = async () => ({
    data: Buffer.from('plain-audio'),
    headers: { 'content-type': 'audio/wav', 'content-duration': '1' }
  })
  connector.axiosTtsEffectsParams = null

  const result = await connector._fetchTts({
    url: 'https://speech.example/tts',
    method: 'GET',
    responseType: 'arraybuffer'
  }, 'hello')

  assert.equal(result.buffer.toString(), 'plain-audio')
  assert.equal(result.duration, '1')
})

test('fails closed when effects conversion fails', async () => {
  const connector = createConnector()
  let requestNumber = 0
  connector._axios = async () => {
    requestNumber += 1
    if (requestNumber === 1) return { data: Buffer.from('plain-audio'), headers: { 'content-type': 'audio/wav' } }
    throw new Error('converter unavailable')
  }
  connector.axiosTtsEffectsParams = { url: 'https://speech.example/effect', method: 'POST' }

  await assert.rejects(
    connector._fetchTts({ url: 'https://speech.example/tts', method: 'GET' }, 'hello'),
    /TTS effects failed.*converter unavailable/
  )
})
