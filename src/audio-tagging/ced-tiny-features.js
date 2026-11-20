/**
 * Log-mel features for sherpa/CED-Tiny ONNX (matches torchaudio MelSpectrogram + AmplitudeToDB).
 * feats shape: [1, 64, num_frames] (sherpa/CED ONNX layout; model transposes internally)
 */

const SAMPLE_RATE = 16000
const N_FFT = 512
const WIN_LENGTH = 512
const HOP_LENGTH = 160
const N_MELS = 64
const F_MIN = 0
const F_MAX = 8000
const TOP_DB = 120

let melFilters = null
let cachedWindow = null

const hannWindow = () => {
  if (cachedWindow) return cachedWindow
  const w = new Float32Array(WIN_LENGTH)
  for (let i = 0; i < WIN_LENGTH; i++) {
    w[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / (WIN_LENGTH - 1)))
  }
  cachedWindow = w
  return cachedWindow
}

const buildMelFilters = () => {
  if (melFilters) return melFilters
  const numFreqs = N_FFT / 2 + 1
  const fftFreqs = new Float32Array(numFreqs)
  for (let i = 0; i < numFreqs; i++) fftFreqs[i] = (i * SAMPLE_RATE) / N_FFT

  const melMin = 2595 * Math.log10(1 + F_MIN / 700)
  const melMax = 2595 * Math.log10(1 + F_MAX / 700)
  const melPoints = new Float32Array(N_MELS + 2)
  for (let i = 0; i < N_MELS + 2; i++) {
    melPoints[i] = melMin + (i * (melMax - melMin)) / (N_MELS + 1)
  }
  const hzPoints = new Float32Array(N_MELS + 2)
  for (let i = 0; i < N_MELS + 2; i++) {
    hzPoints[i] = 700 * (Math.pow(10, melPoints[i] / 2595) - 1)
  }

  const filters = []
  for (let m = 0; m < N_MELS; m++) {
    const f = new Float32Array(numFreqs)
    const left = hzPoints[m]
    const center = hzPoints[m + 1]
    const right = hzPoints[m + 2]
    for (let k = 0; k < numFreqs; k++) {
      const freq = fftFreqs[k]
      if (freq < left || freq > right) continue
      if (freq <= center) {
        f[k] = (freq - left) / (center - left)
      } else {
        f[k] = (right - freq) / (right - center)
      }
    }
    filters.push(f)
  }
  melFilters = filters
  return melFilters
}

// Iterative radix-2 FFT tables for N_FFT (power of two). This runs on the event loop for
// every CED window (~97 frames), so a direct O(N^2) DFT is too slow for live calls.
let fftTables = null

const buildFftTables = () => {
  if (fftTables) return fftTables
  const bits = Math.log2(N_FFT)
  const bitRev = new Uint16Array(N_FFT)
  for (let i = 0; i < N_FFT; i++) {
    let r = 0
    for (let b = 0; b < bits; b++) r |= ((i >> b) & 1) << (bits - 1 - b)
    bitRev[i] = r
  }
  const cos = new Float64Array(N_FFT / 2)
  const sin = new Float64Array(N_FFT / 2)
  for (let k = 0; k < N_FFT / 2; k++) {
    cos[k] = Math.cos((2 * Math.PI * k) / N_FFT)
    sin[k] = Math.sin((2 * Math.PI * k) / N_FFT)
  }
  fftTables = { bitRev, cos, sin, re: new Float64Array(N_FFT), im: new Float64Array(N_FFT) }
  return fftTables
}

const rfft512Power = (frame, window, outPower) => {
  const { bitRev, cos, sin, re, im } = buildFftTables()
  for (let n = 0; n < N_FFT; n++) {
    const src = bitRev[n]
    re[n] = src < WIN_LENGTH ? frame[src] * window[src] : 0
    im[n] = 0
  }
  for (let size = 2; size <= N_FFT; size <<= 1) {
    const half = size >> 1
    const step = N_FFT / size
    for (let start = 0; start < N_FFT; start += size) {
      for (let j = 0; j < half; j++) {
        const wr = cos[j * step]
        const wi = -sin[j * step]
        const a = start + j
        const b = a + half
        const tr = re[b] * wr - im[b] * wi
        const ti = re[b] * wi + im[b] * wr
        re[b] = re[a] - tr
        im[b] = im[a] - ti
        re[a] += tr
        im[a] += ti
      }
    }
  }
  const numFreqs = N_FFT / 2 + 1
  for (let k = 0; k < numFreqs; k++) {
    outPower[k] = re[k] * re[k] + im[k] * im[k]
  }
}

const ampToDb = (melFrame) => {
  let maxDb = -Infinity
  for (let i = 0; i < melFrame.length; i++) {
    const x = Math.max(melFrame[i], 1e-10)
    const db = 10 * Math.log10(x)
    melFrame[i] = db
    if (db > maxDb) maxDb = db
  }
  const floor = maxDb - TOP_DB
  for (let i = 0; i < melFrame.length; i++) {
    if (melFrame[i] < floor) melFrame[i] = floor
  }
}

/**
 * @param {Float32Array} waveform mono 16 kHz (typically 1s window, zero-padded at start)
 * @returns {{ data: Float32Array, dims: [number, number, number] } | null}
 */
const computeCedFeats = (waveform) => {
  if (!waveform || waveform.length < HOP_LENGTH) return null
  const filters = buildMelFilters()
  const window = hannWindow()
  const numFrames = 1 + Math.floor((waveform.length - WIN_LENGTH) / HOP_LENGTH)
  if (numFrames <= 0) return null

  const power = new Float32Array(N_FFT / 2 + 1)
  const melFrame = new Float32Array(N_MELS)
  const data = new Float32Array(N_MELS * numFrames)
  const frame = new Float32Array(WIN_LENGTH)

  for (let t = 0; t < numFrames; t++) {
    const start = t * HOP_LENGTH
    frame.fill(0)
    const copyLen = Math.min(WIN_LENGTH, waveform.length - start)
    for (let i = 0; i < copyLen; i++) frame[i] = waveform[start + i]

    rfft512Power(frame, window, power)
    for (let m = 0; m < N_MELS; m++) {
      let sum = 0
      const f = filters[m]
      for (let k = 0; k < power.length; k++) sum += power[k] * f[k]
      melFrame[m] = sum
    }
    ampToDb(melFrame)
    for (let m = 0; m < N_MELS; m++) {
      data[m * numFrames + t] = melFrame[m]
    }
  }

  return { data, dims: [1, N_MELS, numFrames] }
}

module.exports = {
  SAMPLE_RATE,
  computeCedFeats,
  rfft512Power
}
