const crypto = require('crypto');
const cache = require('./cache');
const webhook = require('./webhook');
const logger = require('../logger');

const IDS_KEY = 'alerts:ids';
const COOLDOWN_MS = 5 * 60 * 1000;

function alertKey(id) {
  return `alert:${id}`;
}

function generateId() {
  return `alrt_${crypto.randomUUID().replace(/-/g, '').slice(0, 16)}`;
}

function isTriggered(alert, priceUsd) {
  if (alert.type === 'above') return priceUsd > alert.threshold_usd;
  if (alert.type === 'below') return priceUsd < alert.threshold_usd;
  if (alert.type === 'change_pct') {
    if (alert.baseline_price === null) return false;
    const pct = Math.abs((priceUsd - alert.baseline_price) / alert.baseline_price) * 100;
    return pct >= alert.threshold_usd;
  }
  return false;
}

async function create(data) {
  const { asset, type, threshold_usd, webhook_url, webhook_secret, repeat } = data;

  const id = generateId();

  let baselinePrice = null;
  if (type === 'change_pct') {
    const cached = await cache.get(`price:${asset.toUpperCase()}`);
    if (cached && cached.price) baselinePrice = cached.price;
  }

  const alert = {
    id,
    asset: asset.toUpperCase(),
    type,
    threshold_usd,
    webhook_url,
    webhook_secret,
    repeat: repeat === true,
    created_at: new Date().toISOString(),
    last_fired_at: null,
    baseline_price: baselinePrice,
  };

  const redis = cache.getClient();
  await cache.set(alertKey(id), alert);
  await redis.zadd(IDS_KEY, Date.now(), id);

  return alert;
}

async function list() {
  const redis = cache.getClient();
  const ids = await redis.zrevrange(IDS_KEY, 0, -1);
  const alerts = await Promise.all(ids.map((id) => cache.get(alertKey(id))));
  return alerts.filter(Boolean);
}

async function listPaginated({ offset = 0, limit = 20 } = {}) {
  const redis = cache.getClient();
  const total = await redis.zcard(IDS_KEY);
  const paginatedIds = await redis.zrevrange(IDS_KEY, offset, offset + limit - 1);
  const alerts = await Promise.all(
    paginatedIds.map((id) => cache.get(alertKey(id)))
  );
  return {
    alerts: alerts.filter(Boolean),
    total
  };
}

async function remove(id) {
  const redis = cache.getClient();
  const existing = await cache.get(alertKey(id));
  if (!existing) return null;
  await cache.del(alertKey(id));
  await redis.zrem(IDS_KEY, id);
  return existing;
}

async function fire(alert, priceUsd) {
  const payload = {
    event: 'price.alert',
    alert_id: alert.id,
    asset: alert.asset,
    type: alert.type,
    threshold_usd: alert.threshold_usd,
    actual_price_usd: priceUsd,
    triggered_at: new Date().toISOString(),
  };

  logger.info('Price alert triggered', { alert_id: alert.id, asset: alert.asset, price: priceUsd });
  await webhook.deliver(alert.webhook_url, alert.webhook_secret, payload);
}

// Evaluates an already-fetched list of alerts against a price. Extracted out
// of evaluateForAsset's per-id loop so the trigger/cooldown/fire/persist
// logic has one implementation, usable against any array of alert objects
// regardless of how they were fetched.
async function evaluateAlertList(alerts, priceUsd) {
  for (const alert of alerts) {
    if (!alert) continue;
    if (!isTriggered(alert, priceUsd)) continue;

    if (alert.repeat && alert.last_fired_at) {
      const elapsed = Date.now() - new Date(alert.last_fired_at).getTime();
      if (elapsed < COOLDOWN_MS) continue;
    }

    await fire(alert, priceUsd);

    if (!alert.repeat) {
      await remove(alert.id);
    } else {
      alert.last_fired_at = new Date().toISOString();
      await cache.set(alertKey(alert.id), alert);
    }
  }
}

// Standalone entry point for evaluating a single asset. Reads the full alert
// list fresh from Redis on every call (rather than reusing any snapshot),
// which matters for callers invoking this directly for one asset right
// after an alert may have been created — evaluateAll does not call this
// function; see its own comment below for why it takes one upfront
// snapshot instead.
async function evaluateForAsset(asset, priceUsd) {
  const redis = cache.getClient();
  const ids = await redis.zrevrange(IDS_KEY, 0, -1);
  const alerts = await Promise.all(ids.map((id) => cache.get(alertKey(id))));
  const matching = alerts.filter((alert) => alert && alert.asset === asset.toUpperCase());
  await evaluateAlertList(matching, priceUsd);
}

// Evaluates every configured alert against the current cached price for its
// asset, once per price-refresh cycle (see src/jobs/priceRefresh.js).
//
// Takes a single upfront snapshot via list() and groups it by asset in
// memory, rather than re-reading the full alert set from Redis once per
// distinct asset. There is no correctness reason to prefer a fresh
// per-asset read here: an alert created concurrently mid-cycle simply gets
// picked up on the *next* cycle (default every 30s, see
// PRICE_REFRESH_INTERVAL_SECONDS), the same way it would if it had been
// created a few seconds earlier and missed this cycle's snapshot entirely.
// Re-reading per asset bought no additional correctness, only an O(assets *
// alerts) multiplier on Redis round-trips — see issue #132.
//
// This still pulls every configured alert into the process on every cycle,
// which is O(alerts) rather than O(assets * alerts) but not free at very
// large alert counts. A secondary index (e.g. a per-asset
// `alerts:by_asset:{asset}` Set, maintained incrementally on create/remove)
// would let this touch only the alerts for assets whose price actually
// changed this cycle. Worth revisiting if alert counts grow large enough
// for the single list() fetch itself to matter; out of scope here since the
// issue's acceptance criteria only call for eliminating the redundant
// per-asset re-fetch.
async function evaluateAll() {
  const allAlerts = await list();

  const alertsByAsset = new Map();
  for (const alert of allAlerts) {
    const bucket = alertsByAsset.get(alert.asset);
    if (bucket) {
      bucket.push(alert);
    } else {
      alertsByAsset.set(alert.asset, [alert]);
    }
  }

  for (const [asset, alerts] of alertsByAsset) {
    const cached = await cache.get(`price:${asset}`);
    if (!cached || cached.price == null) continue;
    await evaluateAlertList(alerts, cached.price);
  }
}

module.exports = { create, list, listPaginated, remove, evaluateForAsset, evaluateAll };
