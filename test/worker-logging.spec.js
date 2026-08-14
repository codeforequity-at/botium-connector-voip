const assert = require('assert')

const {
  formatConnectorInfoLine,
  formatWorkerLogLine,
  parseBoolean,
  redactWorkerLogMessage
} = require('../src/worker-logging')

const callIdLine = formatConnectorInfoLine('sip_call_id', {
  sessionId: 'worker-session',
  sipCallId: 'dialog-id-123',
  available: true
})
assert.strictEqual(
  callIdLine,
  '[botium-connector-voip] event="sip_call_id" sessionId="worker-session" sipCallId="dialog-id-123" available=true'
)

assert.strictEqual(parseBoolean(true), true)
assert.strictEqual(parseBoolean('true'), true)
assert.strictEqual(parseBoolean('1'), true)
assert.strictEqual(parseBoolean('false'), false)
assert.strictEqual(parseBoolean(false), false)
assert.strictEqual(parseBoolean(undefined), false)

const rawSecrets = [
  'Authorization: Digest username="alice", response="secret-digest"',
  "{'sipCallerPassword': 'super-secret', 'apiKey': 'key-123', 'private_key': 'private-key-123'}",
  'endpoint=https://alice:password123@example.org/path',
  'proxy=sip:alice:sip-password@example.org',
  'a=ice-pwd:ice-password',
  'a=crypto:1 AES_CM_128_HMAC_SHA1_80 inline:srtp-key',
  'Ocp-Apim-Subscription-Key: azure-secret'
].join('\n')
const redacted = redactWorkerLogMessage(rawSecrets)
assert.ok(!redacted.includes('secret-digest'))
assert.ok(!redacted.includes('super-secret'))
assert.ok(!redacted.includes('key-123'))
assert.ok(!redacted.includes('private-key-123'))
assert.ok(!redacted.includes('password123'))
assert.ok(!redacted.includes('sip-password'))
assert.ok(!redacted.includes('ice-password'))
assert.ok(!redacted.includes('srtp-key'))
assert.ok(!redacted.includes('azure-secret'))
assert.ok(redacted.includes('[REDACTED]'))

const formatted = formatWorkerLogLine({
  timestampMs: Date.UTC(2026, 7, 14, 10, 11, 12),
  level: 'debug',
  source: 'PJSUA',
  message: 'Call-ID: abc-123\nAuthorization: Bearer token-123'
}, 'worker-session')
assert.ok(formatted.startsWith('[botium-voip-worker] '))
assert.ok(formatted.includes('sessionId="worker-session"'))
assert.ok(formatted.includes('level="DEBUG"'))
assert.ok(formatted.includes('source="PJSUA"'))
assert.ok(formatted.includes('Call-ID: abc-123'))
assert.ok(!formatted.includes('token-123'))

const invalidTimestamp = formatWorkerLogLine({
  timestampMs: Number.MAX_VALUE,
  level: 'info',
  message: 'still formatted'
}, 'worker-session')
assert.ok(invalidTimestamp.includes('still formatted'))
assert.ok(!invalidTimestamp.includes('timestamp='))

console.log('worker logging tests passed')
