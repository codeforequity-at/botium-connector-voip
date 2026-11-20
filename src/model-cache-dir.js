const fs = require('fs')
const os = require('os')
const path = require('path')

const resolved = new Map()

const isWritableDir = (dir) => {
  try {
    fs.mkdirSync(dir, { recursive: true })
    fs.accessSync(dir, fs.constants.W_OK)
    return true
  } catch (err) {
    return false
  }
}

const safeHomedir = () => {
  try {
    return os.homedir()
  } catch (err) {
    return null
  }
}

/**
 * Cache directory for downloaded models. An explicit env var wins as-is; otherwise
 * ~/.cache/botium/<name>, falling back to <tmpdir>/botium-cache/<name> when the home
 * directory is missing or read-only (e.g. box-server runs as a user without a home).
 */
const resolveModelCacheDir = (name, envVar, { env = process.env, homedir = safeHomedir, tmpdir = os.tmpdir } = {}) => {
  if (envVar && env[envVar]) return env[envVar]
  if (resolved.has(name)) return resolved.get(name)
  const home = homedir()
  const candidates = [
    home && path.join(home, '.cache', 'botium', name),
    path.join(tmpdir(), 'botium-cache', name)
  ].filter(Boolean)
  const dir = candidates.find(isWritableDir) || candidates[0]
  resolved.set(name, dir)
  return dir
}

const _resetModelCacheDirs = () => resolved.clear()

module.exports = {
  resolveModelCacheDir,
  _resetModelCacheDirs
}
