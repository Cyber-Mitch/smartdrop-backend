const Redis = require('ioredis');
const config = require('../config');
const logger = require('../logger');

const MAX_RETRIES = 10;
const RETRY_DELAY_MS = 1000;
const CONNECT_TIMEOUT_MS = 5000;
const COMMAND_TIMEOUT_MS = 3000;
const COMMAND_QUEUE_WARN_THRESHOLD = parseInt(process.env.REDIS_COMMAND_QUEUE_WARN_THRESHOLD, 10) || 100;

let client = null;
let reconnectAttempts = 0;

function getClient() {
  if (!client) {
    client = new Redis(config.redis.url, {
      lazyConnect: true,
      enableOfflineQueue: true,
      connectTimeout: CONNECT_TIMEOUT_MS,
      commandTimeout: COMMAND_TIMEOUT_MS,
      retryStrategy(times) {
        if (times > MAX_RETRIES) {
          logger.error('Redis max reconnection attempts reached', { attempts: times });
          return null;
        }
        const delay = Math.min(times * RETRY_DELAY_MS, 30000);
        logger.warn('Redis reconnecting', { attempt: times, delayMs: delay });
        return delay;
      },
      maxRetriesPerRequest: 3,
    });

    client.on('error', (err) => {
      reconnectAttempts++;
      logger.error('Redis connection error', { error: err.message, reconnectAttempts });
    });
    client.on('connect', () => {
      reconnectAttempts = 0;
      logger.info('Redis connected');
    });
    client.on('ready', () => {
      reconnectAttempts = 0;
      logger.info('Redis ready');
    });
    client.on('close', () => {
      logger.warn('Redis connection closed');
    });
    client.connect().catch(() => {});
  }
  return client;
}

function isConnected() {
  return client !== null && client.status === 'ready';
}

function getCommandQueueLength() {
  if (!client) return 0;
  return client.commandQueue ? client.commandQueue.length : 0;
}

async function get(key) {
  const redis = getClient();
  const queueLen = getCommandQueueLength();
  if (queueLen > COMMAND_QUEUE_WARN_THRESHOLD) {
    logger.warn('Redis command queue depth high', { queue_length: queueLen, threshold: COMMAND_QUEUE_WARN_THRESHOLD });
  }
  const data = await redis.get(key);
  if (!data) return null;
  try {
    return JSON.parse(data);
  } catch {
    return data;
  }
}

async function set(key, value, ttlSeconds) {
  const redis = getClient();
  const queueLen = getCommandQueueLength();
  if (queueLen > COMMAND_QUEUE_WARN_THRESHOLD) {
    logger.warn('Redis command queue depth high', { queue_length: queueLen, threshold: COMMAND_QUEUE_WARN_THRESHOLD });
  }
  const serialized = JSON.stringify(value);
  if (ttlSeconds) {
    await redis.setex(key, ttlSeconds, serialized);
  } else {
    await redis.set(key, serialized);
  }
}

async function del(key) {
  const redis = getClient();
  await redis.del(key);
}

async function disconnect() {
  if (client) {
    await client.quit();
    client = null;
  }
}

module.exports = { get, set, del, disconnect, getClient, isConnected, getCommandQueueLength };
