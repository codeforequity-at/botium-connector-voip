const { test } = require('node:test')
const assert = require('node:assert/strict')
const { createSttEndRecovery } = require('../src/stt-end-recovery')

const partial = {
  messageText: "we're sorry but we are experiencing technical difficulties",
  sourceData: { data: { start: 1.01, end: 6.28 }, partialRecovery: true }
}
const final = {
  messageText: "We're sorry, but we are experiencing technical difficulties with our systems. Please try your call again later.",
  data: { start: 1.01, end: 7.01 }
}

const harness = () => {
  const delivered = []
  const tasks = new Map()
  let now = 0
  let nextId = 0
  const recovery = createSttEndRecovery({
    recover: () => {
      recovery.markRecovered(partial)
      delivered.push(partial)
    },
    schedule: (callback, delay) => {
      const id = nextId++
      tasks.set(id, { callback, at: now + delay })
      return id
    },
    cancel: id => tasks.delete(id)
  })
  const advance = ms => {
    now += ms
    for (const [id, task] of tasks) {
      if (task.at <= now) {
        tasks.delete(id)
        task.callback()
      }
    }
  }
  const receiveFinal = frame => {
    if (recovery.acceptFinal(frame)) delivered.push(frame)
  }
  return { recovery, delivered, tasks, advance, receiveFinal }
}

test('disconnect followed 134ms later by final delivers the announcement once', () => {
  const h = harness()
  h.recovery.defer()
  h.advance(134)
  h.receiveFinal(final)
  h.advance(2000)
  assert.deepEqual(h.delivered, [final])
  assert.equal(h.tasks.size, 0)
})

test('missing final recovers the partial once after the bounded drain window', () => {
  const h = harness()
  h.recovery.defer()
  h.advance(1499)
  assert.deepEqual(h.delivered, [])
  h.advance(1)
  h.recovery.clear()
  h.advance(2000)
  assert.deepEqual(h.delivered, [partial])
})

test('a final for already recovered audio is not emitted as another bot turn', () => {
  const h = harness()
  h.recovery.defer()
  h.advance(1500)
  h.receiveFinal(final)
  assert.deepEqual(h.delivered, [partial])
})

test('distinct subsequent speech, even with the same text, is retained', () => {
  const h = harness()
  h.recovery.defer()
  h.advance(1500)
  const next = { ...final, data: { start: 8, end: 14 } }
  h.receiveFinal(next)
  assert.deepEqual(h.delivered, [partial, next])
})

test('recording-end or Stop flush cancels the delayed fallback', () => {
  const h = harness()
  h.recovery.defer()
  h.recovery.clear()
  h.recovery.markRecovered(partial)
  h.delivered.push(partial)
  h.advance(2000)
  h.receiveFinal(final)
  assert.deepEqual(h.delivered, [partial])
})

test('repeated disconnect does not postpone recovery and cleanup cancels old session work', () => {
  const h = harness()
  h.recovery.defer()
  h.advance(1000)
  h.recovery.defer()
  assert.equal(h.tasks.size, 1)
  h.advance(500)
  assert.deepEqual(h.delivered, [partial])
  h.recovery.defer()
  h.recovery.clear()
  h.advance(2000)
  assert.deepEqual(h.delivered, [partial])
})

test('without reliable audio bounds finals are retained', () => {
  const h = harness()
  h.recovery.markRecovered({ sourceData: {} })
  h.receiveFinal(final)
  assert.deepEqual(h.delivered, [final])
})
