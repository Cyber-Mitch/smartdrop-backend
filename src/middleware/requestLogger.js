'use strict';

/**
 * Structured logging for every HTTP request/response cycle (issue #148).
 *
 * Complements `routes/metrics.js`'s `requestMetricsMiddleware` (which only
 * increments in-memory counters) with an actual log line per request —
 * method, path, status code, and duration — so requests are debuggable and
 * analyzable outside of the /metrics snapshot. `req.id`/requestId is already
 * attached to every log line automatically by `logger.js`'s AsyncLocalStorage
 * context (see requestId.js), so it isn't repeated here explicitly.
 *
 * Uses `req.path` (not `req.originalUrl`) so query strings — which can carry
 * an API key on some endpoints — are never logged, consistent with
 * logger.js's redaction of sensitive fields elsewhere.
 */

const logger = require('../logger');

function requestLoggerMiddleware(req, res, next) {
  const startedAt = process.hrtime.bigint();

  res.on('finish', () => {
    const durationMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
    const level = res.statusCode >= 500 ? 'error' : res.statusCode >= 400 ? 'warn' : 'info';

    logger[level]('HTTP request', {
      method: req.method,
      path: req.path,
      statusCode: res.statusCode,
      durationMs: Math.round(durationMs * 100) / 100,
    });
  });

  next();
}

module.exports = requestLoggerMiddleware;
