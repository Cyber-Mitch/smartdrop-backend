'use strict';

const cache = require('../services/cache');
const logger = require('../logger');
const AppError = require('../errors/AppError');

/**
 * Fixed-window rate limiter backed by Redis INCR + EXPIRE.
 * Fails open if Redis is unreachable so a cache outage cannot lock out users.
 */
// Track consecutive Redis failures per keyPrefix to escalate log severity.
const consecutiveFailures = new Map();

function buildRateLimit({ windowSeconds, max, keyPrefix }) {
  if (!Number.isFinite(windowSeconds) || windowSeconds <= 0) {
    throw new Error('windowSeconds must be a positive number');
  }
  if (!Number.isFinite(max) || max <= 0) {
    throw new Error('max must be a positive number');
  }
  if (!keyPrefix || typeof keyPrefix !== 'string') {
    throw new Error('keyPrefix is required');
  }

  return async function rateLimit(req, res, next) {
    const identifier = req.ip || req.connection?.remoteAddress || 'unknown';
    const bucket = Math.floor(Date.now() / 1000 / windowSeconds);
    const key = `ratelimit:${keyPrefix}:${identifier}:${bucket}`;

    try {
      const redis = cache.getClient();
      const count = await redis.incr(key);
      if (count === 1) {
        await redis.expire(key, windowSeconds);
      }
      // Reset failure counter on success.
      consecutiveFailures.delete(keyPrefix);
      const remaining = Math.max(0, max - count);
      const resetAt = (bucket + 1) * windowSeconds;
      const retryAfterSeconds = Math.max(1, resetAt - Math.floor(Date.now() / 1000));
      res.setHeader('X-RateLimit-Limit', String(max));
      res.setHeader('X-RateLimit-Remaining', String(remaining));
      res.setHeader('X-RateLimit-Reset', String(resetAt));
      if (count > max) {
        res.setHeader('Retry-After', String(retryAfterSeconds));
        return next(new AppError(
          'RATE_LIMITED',
          `Rate limit of ${max} requests per ${windowSeconds}s exceeded`,
          429,
          { limit: max, window_seconds: windowSeconds, retry_after_seconds: retryAfterSeconds },
        ));
      }
      return next();
    } catch (err) {
      const failures = (consecutiveFailures.get(keyPrefix) || 0) + 1;
      consecutiveFailures.set(keyPrefix, failures);
      // First failure is a warning; 3+ consecutive failures escalate to error
      // so operators see persistent Redis issues in alerting.
      if (failures >= 3) {
        logger.error('Rate limit disabled — Redis error persists', {
          keyPrefix,
          consecutive_failures: failures,
          error: err.message,
        });
      } else {
        logger.warn('Rate limit fail-open due to cache error', { keyPrefix, error: err.message });
      }
      return next();
    }
  };
}

module.exports = buildRateLimit;
