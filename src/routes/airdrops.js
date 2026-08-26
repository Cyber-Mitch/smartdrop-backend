const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');
const config = require('../config');
const airdropsService = require('../services/airdrops');
const logger = require('../logger');
const AppError = require('../errors/AppError');
const { flattenZodIssues, validate } = require('../middleware/validate');
const {
  airdropCreateBodySchema,
  airdropRecipientsBodySchema,
  airdropUpdateBodySchema,
  paginationQuerySchema,
  recipientsSchema,
  routeIdParamsSchema,
} = require('../validation/schemas');
const buildRateLimit = require('../middleware/rateLimit');
const { StrKey } = require('stellar-sdk');
const { paginateResponse } = require('../utils/paginate');

// Stellar Int64 max in stroops (1 unit = 10_000_000 stroops for XLM/USDC)
const INT64_MAX_STROOPS = 9223372036854775807n;
const STROOPS_PER_UNIT = 10_000_000n;

const router = express.Router();
const CSV_PARSE_CHUNK_BYTES = 64 * 1024;
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: config.airdrops.csvMaxBytes },
});
const validateRouteIdParams = validate(routeIdParamsSchema, 'params');
const validatePaginationQuery = validate(paginationQuerySchema, 'query');
const validateRecipientBody = validate(airdropRecipientsBodySchema);

function validateWithCurrentLedger(schemaFactory) {
  return async (req, res, next) => {
    try {
      const currentLedger = await airdropsService.getCurrentLedger();
      return validate(schemaFactory(currentLedger))(req, res, next);
    } catch (err) {
      logger.error('Airdrop validation error', { error: err.message });
      return next(err);
    }
  };
}

const createAirdropLimit = buildRateLimit({
  windowSeconds: config.airdrops.rateLimit.windowSeconds,
  max: config.airdrops.rateLimit.max,
  keyPrefix: 'airdrops_create',
});

const addRecipientsLimit = buildRateLimit({
  windowSeconds: config.airdrops.rateLimit.windowSeconds,
  max: config.airdrops.rateLimit.max,
  keyPrefix: 'airdrops_recipients',
});

function uploadRecipientsFile(req, res, next) {
  upload.single('file')(req, res, (err) => {
    if (err instanceof multer.MulterError && err.code === 'LIMIT_FILE_SIZE') {
      return next(new AppError(
        'PAYLOAD_TOO_LARGE',
        `CSV file cannot exceed ${config.airdrops.csvMaxBytes} bytes`,
        413,
        { max_bytes: config.airdrops.csvMaxBytes }
      ));
    }
    return next(err);
  });
}

function isValidStellarAddress(address) {
  try {
    return StrKey.isValidEd25519PublicKey(address);
  } catch {
    return false;
  }
}

function toStroops(amount) {
  return BigInt(Math.round(amount * Number(STROOPS_PER_UNIT)));
}

function assertWithinCeiling(stroops, label) {
  if (stroops > INT64_MAX_STROOPS) {
    throw new AppError('VALIDATION_ERROR', `${label} exceeds Stellar Int64 ceiling`, 400);
  }
}

function parseRecipients(recipients, next) {
  const result = recipientsSchema.safeParse(recipients);
  if (!result.success) {
    return next(new AppError('VALIDATION_ERROR', 'Validation failed', 400, {
      fields: flattenZodIssues(result.error),
    }));
  }
  return result.data;
}

function validateUtf8(buffer) {
  try {
    const decoder = new TextDecoder('utf-8', { fatal: true });
    decoder.decode(buffer);
  } catch {
    throw new AppError('VALIDATION_ERROR', 'CSV file must be valid UTF-8 encoded', 400);
  }
}

async function parseCSV(buffer) {
  validateUtf8(buffer);
  const results = [];
  let rowCount = 0;
  const chunks = (function* chunkBuffer() {
    for (let offset = 0; offset < buffer.length; offset += CSV_PARSE_CHUNK_BYTES) {
      yield buffer.subarray(offset, offset + CSV_PARSE_CHUNK_BYTES);
    }
  }());

  await pipeline(Readable.from(chunks), csv(), async (rows) => {
    for await (const data of rows) {
      rowCount += 1;
      if (rowCount > config.airdrops.maxRecipients) {
        throw new AppError('VALIDATION_ERROR', 'recipients cannot exceed 10,000', 400);
      }

      const address = data.address || data.Address || data.ADDRESS;
      const amount = parseFloat(data.amount || data.Amount || data.AMOUNT);
      if (address && Number.isFinite(amount) && amount > 0) {
        results.push({ address, amount });
      }
    }
  });

  return results;
}

router.post('/airdrops', createAirdropLimit, validateWithCurrentLedger(airdropCreateBodySchema), async (req, res, next) => {
  try {
    const airdrop = await airdropsService.create(req.validated.body);
    return res.status(201).json(airdrop);
  } catch (err) {
    logger.error('Create airdrop error', { error: err.message });
    return next(err);
  }
});

router.get('/airdrops', validatePaginationQuery, async (req, res, next) => {
  try {
    const { page, limit } = req.validated.query;
    const result = await airdropsService.list(page, limit);
    return res.json(paginateResponse(result.airdrops, result.total, { page, limit }));
  } catch (err) {
    logger.error('List airdrops error', { error: err.message });
    return next(err);
  }
});

router.get('/airdrops/:id', validateRouteIdParams, async (req, res, next) => {
  try {
    const airdrop = await airdropsService.get(req.params.id);
    if (!airdrop) {
      return next(new AppError('NOT_FOUND', 'Airdrop not found', 404));
    }
    return res.json(airdrop);
  } catch (err) {
    logger.error('Get airdrop error', { error: err.message });
    return next(err);
  }
});

router.patch('/airdrops/:id', validateRouteIdParams, validateWithCurrentLedger(airdropUpdateBodySchema), async (req, res, next) => {
  try {
    const airdrop = await airdropsService.update(req.params.id, req.validated.body);
    if (!airdrop) {
      return next(new AppError('NOT_FOUND', 'Airdrop not found', 404));
    }
    return res.json(airdrop);
  } catch (err) {
    logger.error('Update airdrop error', { error: err.message });
    return next(err);
  }
});

router.delete('/airdrops/:id', validateRouteIdParams, async (req, res, next) => {
  try {
    const deleted = await airdropsService.remove(req.params.id);
    if (!deleted) {
      return next(new AppError('NOT_FOUND', 'Airdrop not found', 404));
    }
    return res.json({ deleted: true, id: req.params.id });
  } catch (err) {
    logger.error('Delete airdrop error', { error: err.message });
    return next(err);
  }
});

router.post('/airdrops/:id/cancel', validateRouteIdParams, async (req, res, next) => {
  try {
    const airdrop = await airdropsService.cancel(req.params.id);
    if (!airdrop) {
      return next(new AppError('NOT_FOUND', 'Airdrop not found', 404));
    }
    return res.json(airdrop);
  } catch (err) {
    logger.error('Cancel airdrop error', { error: err.message });
    return next(err);
  }
});

router.post('/airdrops/:id/recipients', validateRouteIdParams, addRecipientsLimit, uploadRecipientsFile, validateRecipientBody, async (req, res, next) => {
  try {
    const airdrop = await airdropsService.get(req.params.id);
    if (!airdrop) {
      return next(new AppError('NOT_FOUND', 'Airdrop not found', 404));
    }

    let recipients = [];
    if (req.file) {
      recipients = await parseCSV(req.file.buffer);
      recipients = parseRecipients(recipients, next);
      if (!recipients) return undefined;
    } else if (req.validated.body.recipients) {
      recipients = req.validated.body.recipients;
    } else {
      return next(new AppError('VALIDATION_ERROR', 'recipients or file is required', 400));
    }

    if (recipients.length > config.airdrops.maxRecipients) {
      return next(new AppError('VALIDATION_ERROR', 'recipients cannot exceed 10,000', 400));
    }

    const recipientSet = new Set();
    let sum = 0n;
    for (let i = 0; i < recipients.length; i++) {
      const r = recipients[i];
      if (!r.address || !isValidStellarAddress(r.address)) {
        return next(new AppError('VALIDATION_ERROR', `recipient ${i}: invalid Stellar address`, 400));
      }
      if (recipientSet.has(r.address)) {
        return next(new AppError('VALIDATION_ERROR', `recipient ${i}: duplicate address ${r.address}`, 400));
      }
      recipientSet.add(r.address);
      if (typeof r.amount !== 'number' || r.amount <= 0 || !Number.isFinite(r.amount)) {
        return next(new AppError('VALIDATION_ERROR', `recipient ${i}: amount must be a positive number`, 400));
      }
      const stroops = toStroops(r.amount);
      assertWithinCeiling(stroops, `recipient ${i} amount`);
      sum += stroops;
    }
    assertWithinCeiling(sum, 'total recipient amount');

    const duplicates = await airdropsService.addRecipients(req.params.id, recipients);
    if (duplicates.length > 0) {
      return next(new AppError(
        'CONFLICT',
        'One or more recipient addresses are already registered for this airdrop',
        409,
        { duplicate_addresses: duplicates },
      ));
    }
    return res.status(201).json({ added: recipients.length });
  } catch (err) {
    logger.error('Add recipients error', { error: err.message });
    return next(err);
  }
});

router.get('/airdrops/:id/recipients', validateRouteIdParams, validatePaginationQuery, async (req, res, next) => {
  try {
    const airdrop = await airdropsService.get(req.params.id);
    if (!airdrop) {
      return next(new AppError('NOT_FOUND', 'Airdrop not found', 404));
    }

    const { page, limit } = req.validated.query;
    const result = await airdropsService.listRecipients(req.params.id, page, limit);
    return res.json(paginateResponse(result.recipients, result.total, { page, limit }));
  } catch (err) {
    logger.error('List recipients error', { error: err.message });
    return next(err);
  }
});

module.exports = router;
