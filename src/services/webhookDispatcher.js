'use strict';

const crypto = require('crypto');
const axios = require('axios');
const config = require('../config');
const logger = require('../logger');
const cache = require('./cache');
const signature = require('./webhookSignature');
const events = require('./webhookEvents');
const webhookRepo = require('../repositories/webhookRepository');
const deliveryRepo = require('../repositories/deliveryRepository');
const { requestContext } = require('../middleware/requestId');

const USER_AGENT = 'SmartDrop-Webhooks/1.0';

/**
 * Computes the retry delay for a webhook delivery that has completed
 * `attemptsCompleted` attempts, using exponential backoff with "equal
 * jitter": half of the deterministic delay is fixed, the other half is
 * randomized within [0, half). This spreads out deliveries that fail at
 * the same attempt count around the same wall-clock moment — preventing
 * the synchronized-retry thundering-herd burst described in #128 — while
 * keeping the result always within [deterministic/2, deterministic):
 * never zero or negative, and never reaching or exceeding the original
 * deterministic delay, so worst-case retry latency stays predictable for
 * operators. "Full jitter" (uniformly random in [0, deterministic)) was
 * considered and rejected: it can produce near-immediate retries, and —
 * with the default 2x factor — its range for one attempt overlaps the
 * next attempt's range, which would make delays non-monotonic across
 * attempts.
 *
 * The random source is injectable via `options.random` (mirroring
 * CircuitBreaker's `options.now`/`options.logger` pattern in
 * `utils/circuitBreaker.js`) so tests can assert exact min/max bounds
 * rather than only "looks random".
 */
function backoffMs(attemptsCompleted, options = {}) {
  const random = options.random || Math.random;
  const base = config.webhooks.retryBaseMs;
  const factor = config.webhooks.retryFactor;
  const deterministicDelay = base * factor ** (attemptsCompleted - 1);
  const half = deterministicDelay / 2;
  return half + random() * half;
}

function shouldRetry(responseStatus, networkError) {
  if (networkError) return true;
  if (responseStatus == null) return true;
  if (responseStatus >= 500 && responseStatus < 600) return true;
  if (responseStatus === 408 || responseStatus === 429) return true;
  return false;
}

function buildHeaders(secret, body, eventType, deliveryId) {
  return {
    'Content-Type': 'application/json',
    'User-Agent': USER_AGENT,
    'X-SmartDrop-Event': eventType,
    'X-SmartDrop-Delivery': deliveryId,
    'X-SmartDrop-Signature': signature.sign(secret, body),
  };
}

function generateDeliveryTraceId() {
  return `trace_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
}

function matchesWebhookFilters(filters, data) {
  if (!filters) return true;
  if (!data || typeof data !== 'object') return false;

  if (filters.asset !== undefined) {
    const asset = typeof data.asset === 'string' ? data.asset.toUpperCase() : null;
    if (asset !== filters.asset) return false;
  }

  if (filters.pool_id !== undefined && data.pool_id !== filters.pool_id) {
    return false;
  }

  return true;
}

function withDeliveryTrace(traceId, fn) {
  const currentRequestId = requestContext.getStore()?.requestId;
  if (currentRequestId && currentRequestId !== 'system') {
    return fn();
  }
  return requestContext.run({ requestId: traceId }, fn);
}

async function postOnce(url, headers, body, timeoutMs) {
  return axios.post(url, body, {
    headers,
    timeout: timeoutMs ?? config.webhooks.timeoutMs,
    transformRequest: [(data) => data],
    validateStatus: () => true,
  });
}

async function attempt(deliveryId) {
  const delivery = await deliveryRepo.findById(deliveryId);
  if (!delivery) {
    logger.warn('Delivery missing, dropping retry', { delivery_id: deliveryId });
    return null;
  }
  if (delivery.status === 'success') return delivery;

  const traceId = delivery.trace_id || generateDeliveryTraceId();
  if (!delivery.trace_id) {
    await deliveryRepo.update(deliveryId, { trace_id: traceId });
  }

  return withDeliveryTrace(traceId, async () => {
    const webhook = await webhookRepo.findById(delivery.webhook_id);
    if (!webhook || !webhook.active) {
      return deliveryRepo.update(deliveryId, {
        status: 'failed',
        last_error: 'webhook missing or inactive',
        last_attempt_at: new Date().toISOString(),
        next_retry_at: null,
      });
    }

    const payload = delivery.payload || {
      event: delivery.event_type,
      event_id: delivery.event_id,
      delivery_id: delivery.id,
      occurred_at: delivery.created_at,
    };
    const body = JSON.stringify(payload);
    const headers = buildHeaders(webhook.secret, body, delivery.event_type, delivery.id);

    const attempts = delivery.attempts + 1;
    let responseStatus = null;
    let networkError = null;

    try {
      const res = await postOnce(webhook.url, headers, body, webhook.timeoutMs);
      responseStatus = res.status;
    } catch (err) {
      networkError = err.message || 'network error';
    }

    const succeeded = responseStatus != null && responseStatus >= 200 && responseStatus < 300;
    const nowIso = new Date().toISOString();

    if (succeeded) {
      logger.info('Webhook delivered', {
        delivery_id: delivery.id,
        trace_id: traceId,
        webhook_id: webhook.id,
        attempts,
        status: responseStatus,
      });
      return deliveryRepo.update(deliveryId, {
        status: 'success',
        attempts,
        last_attempt_at: nowIso,
        next_retry_at: null,
        last_error: null,
        response_status: responseStatus,
      });
    }

    const errorMessage = networkError || `HTTP ${responseStatus}`;
    const retryable = shouldRetry(responseStatus, Boolean(networkError));
    const hasAttemptsLeft = attempts < config.webhooks.maxAttempts;

    if (retryable && hasAttemptsLeft) {
      const delayMs = backoffMs(attempts);
      const nextRetryAt = new Date(Date.now() + delayMs).toISOString();
      await deliveryRepo.scheduleRetry(delivery.id, Date.now() + delayMs);
      logger.warn('Webhook delivery failed, retry scheduled', {
        delivery_id: delivery.id,
        trace_id: traceId,
        webhook_id: webhook.id,
        attempts,
        error: errorMessage,
        next_retry_at: nextRetryAt,
      });
      return deliveryRepo.update(deliveryId, {
        status: 'pending',
        attempts,
        last_attempt_at: nowIso,
        next_retry_at: nextRetryAt,
        last_error: errorMessage,
        response_status: responseStatus,
      });
    }

    logger.error('Webhook delivery failed permanently', {
      delivery_id: delivery.id,
      trace_id: traceId,
      webhook_id: webhook.id,
      attempts,
      error: errorMessage,
    });
    return deliveryRepo.update(deliveryId, {
      status: 'failed',
      attempts,
      last_attempt_at: nowIso,
      next_retry_at: null,
      last_error: errorMessage,
      response_status: responseStatus,
    });
  });
}

async function deliverToWebhook(webhook, eventType, eventId, payload) {
  const delivery = await deliveryRepo.create({
    webhook_id: webhook.id,
    event_id: eventId,
    event_type: eventType,
  });
  await deliveryRepo.update(delivery.id, { payload });
  return attempt(delivery.id);
}

const DISPATCH_CONCURRENCY = parseInt(process.env.WEBHOOK_DISPATCH_CONCURRENCY, 10) || 10;

async function processBatch(batch, eventType, eventId, payload) {
  return Promise.allSettled(
    batch.map((webhook) => deliverToWebhook(webhook, eventType, eventId, payload))
  );
}

async function dispatch({ event_type: eventType, event_id: eventId, data }) {
  if (!events.isKnownEvent(eventType)) {
    logger.warn('Dispatch skipped, unknown event type', { event_type: eventType });
    return [];
  }
  if (!eventId || typeof eventId !== 'string') {
    throw new Error('event_id is required to dispatch a webhook event');
  }

  const dedupKey = `webhook:dispatched:${eventId}`;
  const alreadyDispatched = await cache.get(dedupKey);
  if (alreadyDispatched) {
    logger.info('Skipping duplicate webhook dispatch', { event_id: eventId, event_type: eventType });
    return [];
  }

  const targets = (await webhookRepo.listActiveForEvent(eventType, events.matchesSubscription))
    .filter((webhook) => matchesWebhookFilters(webhook.filters, data));
  if (targets.length === 0) return [];

  const resourceId = data?.pool_id || data?.asset || eventType;
  const redis = cache.getClient();
  const sequence = await redis.incr(`seq:${resourceId}`);

  const occurredAt = new Date().toISOString();
  const payload = {
    event: eventType,
    event_id: eventId,
    occurred_at: occurredAt,
    sequence,
    data: data || {},
  };

  const allResults = [];
  for (let i = 0; i < targets.length; i += DISPATCH_CONCURRENCY) {
    const batch = targets.slice(i, i + DISPATCH_CONCURRENCY);
    const batchResults = await processBatch(batch, eventType, eventId, payload);
    allResults.push(...batchResults);
  }

  await cache.set(dedupKey, Date.now(), 86400);

  return allResults.map((result, i) => {
    const webhook_id = targets[i].id;
    if (result.status === 'fulfilled') {
      return { webhook_id, delivery: result.value, error: null };
    }
    logger.error('Webhook delivery failed', { webhook_id, error: result.reason?.message || String(result.reason) });
    return { webhook_id, delivery: null, error: result.reason?.message || String(result.reason) };
  });
}

async function sendTest(webhookId) {
  const webhook = await webhookRepo.findById(webhookId);
  if (!webhook) return null;
  const eventType = 'pool.assets_locked';
  const payload = {
    event: eventType,
    event_id: `evt_test_${Date.now()}`,
    occurred_at: new Date().toISOString(),
    data: { test: true, message: 'This is a test delivery from SmartDrop' },
  };
  return deliverToWebhook(webhook, eventType, payload.event_id, payload);
}

module.exports = { dispatch, attempt, sendTest, backoffMs, shouldRetry };
