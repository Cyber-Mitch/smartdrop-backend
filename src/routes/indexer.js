const express = require('express');
const eventStore = require('../indexer/eventStore');
const indexerPoller = require('../indexer/runtime');
const logger = require('../logger');
const buildRateLimit = require('../middleware/rateLimit');
const { parsePagination, paginateResponse } = require('../utils/paginate');

const router = express.Router();

function isValidId(value) {
  return typeof value === 'string' && /^[A-Za-z0-9:_-]{1,128}$/.test(value);
}

function isValidAddress(value) {
  return typeof value === 'string' && /^[A-Z0-9]{10,80}$/.test(value);
}

router.get('/airdrops/:id/status', async (req, res) => {
  try {
    if (!isValidId(req.params.id)) {
      return res.status(400).json({ error: 'Invalid airdrop id' });
    }

    const status = await eventStore.getAirdropStatus(req.params.id);
    if (!status) {
      return res.status(404).json({ error: 'Airdrop not indexed' });
    }

    return res.json(status);
  } catch (err) {
    logger.error('Airdrop status lookup failed', { error: err.message });
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Named distinctly from airdrops.js's own `/airdrops/:id/recipients` (the
// stored/intended recipient list): this returns recipients derived from
// indexed on-chain claim events, a different source of truth. The two
// routers previously registered the exact same path, and since this
// router is mounted first in src/index.js, it silently shadowed the real
// listRecipients handler in airdrops.js on every request.
// getAirdropRecipients/getRecipientClaims below fully materialize their
// list from a single Redis key regardless (see eventStore.js's
// getJsonList) — there's no server-side "fetch only a page" available at
// the storage layer, so pagination here is a slice of the already-fetched
// array plus the canonical envelope, not a more efficient query (#131).
router.get('/airdrops/:id/onchain-recipients', async (req, res) => {
  try {
    if (!isValidId(req.params.id)) {
      return res.status(400).json({ error: 'Invalid airdrop id' });
    }

    const allRecipients = await eventStore.getAirdropRecipients(req.params.id);
    const { page, limit } = parsePagination(req.query);
    const start = (page - 1) * limit;
    const pageRecipients = allRecipients.slice(start, start + limit);
    return res.json(paginateResponse(pageRecipients, allRecipients.length, { page, limit }));
  } catch (err) {
    logger.error('Airdrop recipients lookup failed', { error: err.message });
    return res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/recipients/:address/claims', async (req, res) => {
  try {
    if (!isValidAddress(req.params.address)) {
      return res.status(400).json({ error: 'Invalid recipient address' });
    }

    const allClaims = await eventStore.getRecipientClaims(req.params.address);
    const { page, limit } = parsePagination(req.query);
    const start = (page - 1) * limit;
    const pageClaims = allClaims.slice(start, start + limit);
    return res.json(paginateResponse(pageClaims, allClaims.length, { page, limit }));
  } catch (err) {
    logger.error('Recipient claims lookup failed', { error: err.message });
    return res.status(500).json({ error: 'Internal server error' });
  }
});

const indexerStatusLimit = buildRateLimit({
  windowSeconds: 1,
  max: 1,
  keyPrefix: 'indexer-status',
});

router.get('/indexer/status', indexerStatusLimit, async (_req, res) => {
  try {
    const stats = await eventStore.getStats();
    const poller = indexerPoller.getStatus();
    const hasLatestLedger = poller.latest_ledger !== null && poller.latest_ledger !== undefined;
    const latestLedger = Number(poller.latest_ledger);
    const lastLedger = Number(stats.last_ledger);
    const ledgerLag = hasLatestLedger && Number.isFinite(latestLedger) && Number.isFinite(lastLedger)
      ? Math.max(0, latestLedger - lastLedger)
      : null;

    return res.json({
      ...poller,
      last_ledger: stats.last_ledger,
      events_count: stats.events_count,
      lag: ledgerLag,
      ledger_lag: ledgerLag,
    });
  } catch (err) {
    logger.error('Indexer status lookup failed', { error: err.message });
    return res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
