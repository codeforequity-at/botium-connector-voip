const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { resolveModelCacheDir, _resetModelCacheDirs } = require('../src/model-cache-dir')

beforeEach(() => _resetModelCacheDirs())

test('explicit env var wins', () => {
  assert.equal(resolveModelCacheDir('namo', 'BOTIUM_NAMO_CACHE_DIR', { env: { BOTIUM_NAMO_CACHE_DIR: '/custom/namo' } }), '/custom/namo')
})

test('uses ~/.cache/botium/<name> when home is writable', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'voip-home-'))
  const dir = resolveModelCacheDir('namo', 'BOTIUM_NAMO_CACHE_DIR', { env: {}, homedir: () => home })
  assert.equal(dir, path.join(home, '.cache', 'botium', 'namo'))
  assert.ok(fs.existsSync(dir))
  fs.rmSync(home, { recursive: true, force: true })
})

test('falls back to tmpdir when home cannot be created (box-server ENOENT case)', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'voip-tmp-'))
  // A path below a regular file can't be created even as root (ENOTDIR), unlike a missing /home.
  const blocker = path.join(tmp, 'not-a-dir')
  fs.writeFileSync(blocker, '')
  const dir = resolveModelCacheDir('namo', 'BOTIUM_NAMO_CACHE_DIR', {
    env: {},
    homedir: () => path.join(blocker, 'home', 'node'),
    tmpdir: () => tmp
  })
  assert.equal(dir, path.join(tmp, 'botium-cache', 'namo'))
  assert.ok(fs.existsSync(dir))
  fs.rmSync(tmp, { recursive: true, force: true })
})

test('falls back to tmpdir when the home directory is unknown', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'voip-tmp-'))
  const dir = resolveModelCacheDir('ten-vad', 'BOTIUM_TEN_VAD_CACHE_DIR', { env: {}, homedir: () => null, tmpdir: () => tmp })
  assert.equal(dir, path.join(tmp, 'botium-cache', 'ten-vad'))
  fs.rmSync(tmp, { recursive: true, force: true })
})
