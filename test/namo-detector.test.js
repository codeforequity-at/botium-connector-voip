const test = require('node:test')
const assert = require('node:assert/strict')
const { NamoDetector } = require('../src/namo-detector')

test('converts Namo logits into end-of-utterance probabilities', async () => {
  class FakeTensor {
    constructor (type, data, dims) {
      this.type = type
      this.data = data
      this.dims = dims
    }
  }

  const detector = new NamoDetector()
  detector.model = {
    tokenizer: async () => ({
      input_ids: { data: BigInt64Array.from([101n, 102n]), dims: [1, 2] },
      attention_mask: { data: BigInt64Array.from([1n, 1n]), dims: [1, 2] }
    }),
    session: {
      outputNames: ['logits'],
      run: async feeds => {
        assert.deepEqual(feeds.input_ids.dims, [1, 2])
        assert.deepEqual(feeds.attention_mask.dims, [1, 2])
        return { logits: { data: Float32Array.from([-1, 2]) } }
      }
    },
    ort: { Tensor: FakeTensor }
  }

  const result = await detector.predict('How can I help you?')

  assert.equal(result.predictedComplete, true)
  assert.ok(result.eouProbability > 0.95)
  assert.ok(result.incompleteProbability < 0.05)
})
