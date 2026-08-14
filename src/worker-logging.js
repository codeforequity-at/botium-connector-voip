const WORKER_LOG_LEVELS = new Set(['DEBUG', 'INFO', 'WARNING', 'WARN', 'ERROR', 'CRITICAL', 'FATAL'])

const formatConnectorInfoLine = (event, data = {}) => {
  const parts = Object.entries({ event, ...data })
    .filter(([, value]) => value != null && value !== '')
    .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
  return `[botium-connector-voip] ${parts.join(' ')}`
}

const parseBoolean = (value) => {
  if (value === true || value === 1) return true
  if (value === false || value === 0 || value == null) return false
  if (typeof value !== 'string') return false
  return ['1', 'true', 'yes', 'y', 'on'].includes(value.trim().toLowerCase())
}

const redactWorkerLogMessage = (value) => {
  let message = value == null ? '' : String(value)

  // SIP/HTTP authentication headers can contain reusable credentials or digest material.
  message = message.replace(
    /(\b(?:Proxy-Authorization|Authorization|Cookie|Set-Cookie|X-API-Key|API-Key|Ocp-Apim-Subscription-Key)\s*:\s*)[^\r\n]*/gi,
    '$1[REDACTED]'
  )
  message = message.replace(/\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+/gi, '$1 [REDACTED]')

  const secretKey = '(?:[a-z0-9_-]*password|[a-z0-9_-]*passwd|[a-z0-9_-]*pwd|[a-z0-9_-]*credential|[a-z0-9_-]*secret|secret(?:[_-]?access)?[_-]?key|private[_-]?key|client[_-]?secret|[a-z0-9_-]*token|api[_-]?key|apikey|access[_-]?key(?:[_-]?id)?|account[_-]?key|subscription[_-]?key)'
  const quotedValue = new RegExp(`((?:["']?${secretKey}["']?)\\s*[:=]\\s*)(["'])(.*?)\\2`, 'gi')
  message = message.replace(quotedValue, (match, prefix, quote) => `${prefix}${quote}[REDACTED]${quote}`)

  const unquotedValue = new RegExp(`(\\b${secretKey}\\b\\s*[:=]\\s*)([^,\\s}\\]]+)`, 'gi')
  message = message.replace(unquotedValue, '$1[REDACTED]')

  // Generic URI user-info (for example https://user:password@host).
  message = message.replace(/([a-z][a-z0-9+.-]*:\/\/[^:\s/@]+:)([^@\s/]+)(@)/gi, '$1[REDACTED]$3')
  message = message.replace(/(\bsips?:[^\s:@;>]+:)([^@\s;>]+)(@)/gi, '$1[REDACTED]$3')
  message = message.replace(/(\ba=crypto:[^\r\n]*?\s+inline:)[^\s|;]+/gi, '$1[REDACTED]')
  return message
}

const formatWorkerLogLine = (entry, sessionId) => {
  if (!entry || typeof entry !== 'object') return null

  const rawLevel = typeof entry.level === 'string' ? entry.level.trim().toUpperCase() : 'INFO'
  const level = WORKER_LOG_LEVELS.has(rawLevel) ? rawLevel : 'INFO'
  const source = typeof entry.source === 'string' && entry.source.trim()
    ? entry.source.trim().replace(/[\r\n]+/g, ' ')
    : 'VoipWorker'
  const timestampMs = Number(entry.timestampMs)
  const timestampDate = Number.isFinite(timestampMs) && timestampMs > 0 ? new Date(timestampMs) : null
  const timestamp = timestampDate && Number.isFinite(timestampDate.getTime())
    ? timestampDate.toISOString()
    : null
  const message = redactWorkerLogMessage(entry.message).split('\u0000').join('')
  const parts = Object.entries({
    sessionId: sessionId || null,
    timestamp,
    level: level === 'WARN' ? 'WARNING' : (level === 'FATAL' ? 'CRITICAL' : level),
    source,
    message
  })
    .filter(([, value]) => value != null && value !== '')
    .map(([key, value]) => `${key}=${JSON.stringify(value)}`)

  return `[botium-voip-worker] ${parts.join(' ')}`
}

module.exports = {
  formatConnectorInfoLine,
  formatWorkerLogLine,
  parseBoolean,
  redactWorkerLogMessage
}
