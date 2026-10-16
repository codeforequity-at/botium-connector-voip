const { test } = require('node:test')
const assert = require('node:assert/strict')
const { waitForOutboundAllowed } = require('../src/inbound/outbound-gate')

const silentLane = (silentForMs = 500) => ({
  speechActive: false,
  msSinceSpeechSilent: () => silentForMs,
  blocksOutbound: ({ hasSttPartials, musicDominant }) => {
    if (hasSttPartials) return true
    if (musicDominant) return false
    return false
  }
})

const speechLane = () => ({
  speechActive: true,
  msSinceSpeechSilent: () => 0,
  blocksOutbound: ({ hasSttPartials }) => hasSttPartials || true
})

test('waitForOutboundAllowed returns inbound_quiet when lane is silent and no partials', async () => {
  const start = Date.now()
  const gate = await waitForOutboundAllowed({
    inboundLane: silentLane(500),
    quietMs: 100,
    softMaxWaitMs: 2000,
    hardMaxWaitMs: 5000,
    hasRecentSttPartial: () => false,
    pollMs: 20
  })
  assert.equal(gate.reason, 'inbound_quiet')
  assert.ok(gate.waitedMs < 200)
  assert.ok(Date.now() - start < 300)
})

test('waitForOutboundAllowed aborts at hard cap when STT partials block', async () => {
  const start = Date.now()
  const gate = await waitForOutboundAllowed({
    inboundLane: silentLane(0),
    quietMs: 50,
    softMaxWaitMs: 80,
    hardMaxWaitMs: 220,
    hasRecentSttPartial: () => true,
    pollMs: 15
  })
  const elapsed = Date.now() - start
  assert.equal(gate.reason, 'superseded_by_inbound')
  assert.equal(gate.aborted, true)
  assert.equal(gate.allowed, false)
  assert.equal(gate.sttOrSpeechActive, true)
  assert.ok(elapsed >= 180, `expected hard cap wait, got ${elapsed}ms`)
  assert.ok(gate.waitedMs >= 180)
})

test('waitForOutboundAllowed aborts while TEN VAD reports speech active at hard cap', async () => {
  const gate = await waitForOutboundAllowed({
    inboundLane: speechLane(),
    quietMs: 50,
    softMaxWaitMs: 60,
    hardMaxWaitMs: 180,
    hasRecentSttPartial: () => false,
    pollMs: 15
  })
  assert.equal(gate.reason, 'superseded_by_inbound')
  assert.equal(gate.aborted, true)
  assert.equal(gate.sttOrSpeechActive, true)
  assert.ok(gate.waitedMs >= 150)
})

test('waitForOutboundAllowed aborts early when shouldAbort fires', async () => {
  let aborted = false
  const gate = await waitForOutboundAllowed({
    inboundLane: speechLane(),
    quietMs: 50,
    softMaxWaitMs: 5000,
    hardMaxWaitMs: 5000,
    hasRecentSttPartial: () => false,
    shouldAbort: () => {
      if (!aborted) {
        aborted = true
        return false
      }
      return true
    },
    pollMs: 10
  })
  assert.equal(gate.reason, 'superseded_by_inbound')
  assert.equal(gate.aborted, true)
  assert.ok(gate.waitedMs < 200)
})
