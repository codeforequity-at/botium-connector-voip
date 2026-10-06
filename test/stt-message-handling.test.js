const { test } = require('node:test')
const assert = require('node:assert/strict')
const {
  HANDLING_EOU,
  isEouHandling,
  isBufferedSttHandling
} = require('../src/stt-message-handling')

test('isEouHandling recognizes EOU and legacy NAMO', () => {
  assert.equal(HANDLING_EOU, 'EOU')
  assert.equal(isEouHandling('EOU'), true)
  assert.equal(isEouHandling('eou'), true)
  assert.equal(isEouHandling('NAMO'), true)
  assert.equal(isEouHandling('namo'), true)
  assert.equal(isEouHandling('PSST'), false)
})

test('isBufferedSttHandling includes EOU and legacy join modes', () => {
  assert.equal(isBufferedSttHandling('EOU'), true)
  assert.equal(isBufferedSttHandling('NAMO'), true)
  assert.equal(isBufferedSttHandling('PSST'), true)
  assert.equal(isBufferedSttHandling('JOIN'), true)
  assert.equal(isBufferedSttHandling('CONCAT'), true)
  assert.equal(isBufferedSttHandling('ORIGINAL'), false)
  assert.equal(isBufferedSttHandling('SPLIT'), false)
})
