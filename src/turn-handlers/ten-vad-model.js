const fs = require('fs')
const os = require('os')
const path = require('path')
const { Readable } = require('stream')
const { pipeline } = require('stream/promises')

// Sherpa-packaged TEN VAD (metadata + int8 ONNX). See assets/models/TEN-VAD.md.
const TEN_VAD_SHERPA_RELEASE_TAG = 'asr-models'
const TEN_VAD_DOWNLOAD_URL =
  'https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/ten-vad.int8.onnx'
const TEN_VAD_UPSTREAM_REPO = 'https://github.com/TEN-framework/ten-vad'
const TEN_VAD_SHERPA_REPO = 'https://github.com/k2-fsa/sherpa-onnx'
const ONNX_FILENAME = 'ten-vad.int8.onnx'
const { bundledModelPath: packageBundledModelPath, assetsModelsDir } = require('../package-assets')

const TEN_VAD_METADATA_HINT =
  'Expected sherpa asr-models ten-vad.int8.onnx with ONNX metadata; see assets/models/TEN-VAD.md'

let downloadPromise = null

const bundledModelPath = () => packageBundledModelPath(ONNX_FILENAME)

const defaultCacheDir = () => {
  if (process.env.BOTIUM_TEN_VAD_CACHE_DIR) {
    return process.env.BOTIUM_TEN_VAD_CACHE_DIR
  }
  return path.join(os.homedir(), '.cache', 'botium', 'ten-vad')
}

const defaultCachePath = () => path.join(defaultCacheDir(), ONNX_FILENAME)

/** Preferred ONNX path: vendored asset, else user cache (legacy download). */
const defaultModelPath = () => {
  const bundled = bundledModelPath()
  if (fs.existsSync(bundled)) return bundled
  return defaultCachePath()
}

const configuredOverridePath = (caps, Capabilities) => {
  const keys = Capabilities && [
    Capabilities.VOIP_TEN_VAD_MODEL_PATH,
    Capabilities.VOIP_NAMO_VAD_MODEL_PATH,
    Capabilities.VOIP_SMART_TURN_VAD_MODEL_PATH
  ].filter(Boolean)
  for (const key of keys) {
    const fromCap = caps && caps[key]
    if (fromCap && String(fromCap).trim()) return String(fromCap).trim()
  }
  if (process.env.BOTIUM_TEN_VAD_MODEL_PATH) return String(process.env.BOTIUM_TEN_VAD_MODEL_PATH).trim()
  if (process.env.VOIP_TEN_VAD_MODEL_PATH) return String(process.env.VOIP_TEN_VAD_MODEL_PATH).trim()
  if (process.env.VOIP_SMART_TURN_VAD_MODEL_PATH) return String(process.env.VOIP_SMART_TURN_VAD_MODEL_PATH).trim()
  return null
}

/** Sherpa asr-models TEN VAD uses a 3-byte length prefix 0xc2 after the value tag for long CSV metadata. */
const readOnnxMetadataValueLength = (buf, i) => {
  if (buf[i] === 0xc2 && i + 2 < buf.length) {
    const len = (buf[i + 1] & 0x7f) + (buf[i + 2] << 7)
    return { len, next: i + 3 }
  }
  let len = 0
  let shift = 0
  let j = i
  for (;;) {
    const b = buf[j]
    j += 1
    len |= (b & 0x7f) << shift
    if ((b & 0x80) === 0) break
    shift += 7
  }
  return { len, next: j }
}

const findMetadataKeyEntry = (buf, key) => {
  const keyBuf = Buffer.from(key, 'utf8')
  if (keyBuf.length > 127) throw new Error(`ONNX metadata key too long: ${key}`)
  const pattern = Buffer.concat([Buffer.from([0x0a, keyBuf.length]), keyBuf, Buffer.from([0x12])])
  let idx = -1
  let pos = 0
  while (pos < buf.length) {
    const hit = buf.indexOf(pattern, pos)
    if (hit < 0) break
    idx = hit
    pos = hit + 1
  }
  if (idx < 0) throw new Error(`ONNX metadata key missing: ${key}`)
  return idx + pattern.length
}

/**
 * Read ONNX StringStringEntryProto metadata without extra dependencies.
 */
const readOnnxMetadataString = (buf, key) => {
  const valueTagPos = findMetadataKeyEntry(buf, key)
  const { len, next } = readOnnxMetadataValueLength(buf, valueTagPos)
  if (len <= 0 || next + len > buf.length) {
    throw new Error(`ONNX metadata value length invalid for: ${key}`)
  }
  return buf.slice(next, next + len).toString('utf8')
}

const parseTenVadMetadata = (modelPath) => {
  const buf = fs.readFileSync(modelPath)
  const modelType = readOnnxMetadataString(buf, 'model_type')
  if (modelType !== 'ten-vad') {
    throw new Error(`Expected ten-vad model_type, got: ${modelType}`)
  }
  const mean = readOnnxMetadataString(buf, 'mean').split(',').map(Number)
  const invStddev = readOnnxMetadataString(buf, 'inv_stddev').split(',').map(Number)
  const window = readOnnxMetadataString(buf, 'window').split(',').map(Number)
  if (mean.length !== 41 || invStddev.length !== 41 || window.length !== 768) {
    throw new Error('ten-vad metadata vector sizes invalid')
  }
  return { mean, invStddev, window }
}

const assertValidTenVadModelFile = (modelPath) => {
  try {
    return parseTenVadMetadata(modelPath)
  } catch (err) {
    throw new Error(`${err.message} — ${TEN_VAD_METADATA_HINT} (path: ${modelPath})`)
  }
}

const downloadOnnx = async ({ destPath, log }) => {
  if (fs.existsSync(destPath)) return destPath
  await fs.promises.mkdir(path.dirname(destPath), { recursive: true })
  const tempPath = `${destPath}.${process.pid}.${Date.now()}.tmp`
  log('ten_vad_model_download_started', {
    url: TEN_VAD_DOWNLOAD_URL,
    destination: destPath,
    attribution: TEN_VAD_SHERPA_REPO
  })
  try {
    const response = await fetch(TEN_VAD_DOWNLOAD_URL)
    if (!response.ok || !response.body) {
      throw new Error(`HTTP ${response.status} ${response.statusText}`)
    }
    await pipeline(Readable.fromWeb(response.body), fs.createWriteStream(tempPath))
    try {
      await fs.promises.rename(tempPath, destPath)
    } catch (err) {
      if (!fs.existsSync(destPath)) throw err
      await fs.promises.rm(tempPath, { force: true })
    }
    const stat = await fs.promises.stat(destPath)
    log('ten_vad_model_download_finished', { bytes: stat.size, destination: destPath })
    return destPath
  } catch (err) {
    await fs.promises.rm(tempPath, { force: true })
    throw new Error(`Downloading ${ONNX_FILENAME} failed: ${err.message}`)
  }
}

const resolveTenVadModelPath = async ({ caps, Capabilities, log = () => {} }) => {
  const override = configuredOverridePath(caps, Capabilities)
  if (override) {
    if (!fs.existsSync(override)) {
      throw new Error(`TEN VAD model not found at configured path: ${override}`)
    }
    assertValidTenVadModelFile(override)
    return override
  }
  const bundled = bundledModelPath()
  if (fs.existsSync(bundled)) {
    assertValidTenVadModelFile(bundled)
    return bundled
  }

  log('ten_vad_bundled_missing', {
    bundledPath: bundled,
    assetsModelsDir: assetsModelsDir(),
    hint: TEN_VAD_METADATA_HINT
  })

  const destPath = defaultCachePath()
  if (fs.existsSync(destPath)) {
    try {
      assertValidTenVadModelFile(destPath)
      return destPath
    } catch (err) {
      log('ten_vad_cache_invalid', { path: destPath, error: err.message })
      await fs.promises.rm(destPath, { force: true })
    }
  }
  if (!downloadPromise) {
    downloadPromise = downloadOnnx({ destPath, log })
      .then((downloadedPath) => {
        assertValidTenVadModelFile(downloadedPath)
        return downloadedPath
      })
      .finally(() => {
        downloadPromise = null
      })
  }
  return downloadPromise
}

module.exports = {
  TEN_VAD_SHERPA_RELEASE_TAG,
  TEN_VAD_DOWNLOAD_URL,
  TEN_VAD_UPSTREAM_REPO,
  TEN_VAD_SHERPA_REPO,
  ONNX_FILENAME,
  bundledModelPath,
  defaultCacheDir,
  defaultCachePath,
  defaultModelPath,
  configuredOverridePath,
  readOnnxMetadataString,
  parseTenVadMetadata,
  assertValidTenVadModelFile,
  TEN_VAD_METADATA_HINT,
  resolveTenVadModelPath
}
