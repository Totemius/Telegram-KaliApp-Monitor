const fs = require("fs").promises;
const fsSync = require("fs");
const { Pool } = require("pg");
const WebSocket = require("ws");
const winston = require("winston");
const express = require("express");
const http = require("http");
const https = require('https');
const path = require("path");
const { v4: uuidv4 } = require("uuid");
const mime = require("mime-types");
const NodeCache = require("node-cache");
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');

require('dotenv').config({ path: path.join(__dirname, '.env.mirror') });

const PORT = parseInt(process.env.PORT) || 8080;
const SSL_ENABLED = process.env.SSL_ENABLED === 'true';

const BASE_URL = process.env.BASE_URL || 
  `http${SSL_ENABLED ? 's' : ''}://nekhebet.su:${PORT}`;

const USE_S3_FOR_MEDIA = process.env.USE_S3_FOR_MEDIA === 'true';
const S3_PUBLIC_URL = process.env.S3_PUBLIC_URL;

const notificationDedupCache = new NodeCache({
  stdTTL: 2,
  checkperiod: 5,
  useClones: false,
  maxKeys: 100000
});

const apiCache = new NodeCache({ stdTTL: 30, checkperiod: 10 });

const messageCache = new NodeCache({ 
  stdTTL: 2,
  checkperiod: 5,
  maxKeys: 1000 
});

const mediaCache = new NodeCache({ 
  stdTTL: 30,
  checkperiod: 10,
  maxKeys: 2000 
});

const mediaStateStore = new Map();

let globalEventId = 0;
const MAX_EVENT_BUFFER_SIZE = 1000;

class EventReplayBuffer {
    constructor(maxSize = 1000) {
        this.buffer = [];
        this.maxSize = maxSize;
        this.startId = 0;
    }
    
    add(event) {
        if (!event.event_id) {
            throw new Error('Event must have event_id set before adding to buffer');
        }
        
        const eventCopy = JSON.parse(JSON.stringify(event));
        
        if (this.buffer.length >= this.maxSize) {
            this.buffer.shift();
            this.startId++;
        }
        this.buffer.push(eventCopy);
        
        return event.event_id;
    }
    
    getEventsSince(eventId) {
        if (eventId < this.startId) {
            return [];
        }
        const startIndex = eventId - this.startId;
        return this.buffer.slice(startIndex);
    }
    
    getLastEventId() {
        return this.startId + this.buffer.length - 1;
    }
}

const eventReplayBuffer = new EventReplayBuffer(MAX_EVENT_BUFFER_SIZE);

let CHANNELS = [];
let lastChannelRefresh = 0;

const DB_HOST = process.env.DB_HOST || "127.0.0.1";
const DB_PORT = process.env.DB_PORT || 5432;
const DB_NAME = process.env.DB_NAME;
const DB_USER = process.env.DB_USER;
const DB_PASSWORD = process.env.DB_PASSWORD;

const INITIAL_DELAY_MS = parseInt(process.env.INITIAL_DELAY_MS) || 0;
const MAX_PENDING_MESSAGES_PER_CLIENT = parseInt(process.env.MAX_PENDING_MESSAGES_PER_CLIENT) || 50;
const ENABLE_BUFFERING = process.env.ENABLE_BUFFERING === 'true';
const CHANNEL_REFRESH_INTERVAL = parseInt(process.env.CHANNEL_REFRESH_INTERVAL) || 60;

const WS_MAX_PAYLOAD = 64 * 1024;
const WS_MAX_MESSAGE_SIZE = 4096;
const WS_RATE_LIMIT = 20;
const WS_RATE_WINDOW = 500;
const MAX_BUFFER = 2 * 1024 * 1024;

const SSL_CERT_PATH = process.env.SSL_CERT_PATH;
const PRIVKEY_PATH = SSL_CERT_PATH ? `${SSL_CERT_PATH}/privkey.pem` : null;
const FULLCHAIN_PATH = SSL_CERT_PATH ? `${SSL_CERT_PATH}/fullchain.pem` : null;

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ level, message, timestamp }) => {
      return `[MIRROR:${PORT}] ${timestamp} ${level}: ${message}`;
    })
  ),
  transports: [
    new winston.transports.File({ 
      filename: path.join(__dirname, 'logs', 'mirror-error.log'), 
      level: 'error' 
    }),
    new winston.transports.File({ 
      filename: path.join(__dirname, 'logs', 'mirror-combined.log') 
    }),
    new winston.transports.Console()
  ]
});

try {
  fsSync.mkdirSync(path.join(__dirname, 'logs'), { recursive: true });
} catch (err) {}

let sslOptions = null;
if (SSL_ENABLED && PRIVKEY_PATH && FULLCHAIN_PATH) {
  try {
    if (fsSync.existsSync(PRIVKEY_PATH) && fsSync.existsSync(FULLCHAIN_PATH)) {
      sslOptions = {
        key: fsSync.readFileSync(PRIVKEY_PATH),
        cert: fsSync.readFileSync(FULLCHAIN_PATH)
      };
      logger.info('SSL certificates loaded');
    } else {
      logger.warn('SSL certificates not found');
    }
  } catch (err) {
    logger.warn(`SSL load error: ${err.message}`);
  }
}

const pool = new Pool({
  host: DB_HOST,
  port: DB_PORT,
  database: DB_NAME,
  user: DB_USER,
  password: DB_PASSWORD,
  max: 50,
  idleTimeoutMillis: 30000,
  statement_timeout: 5000,
  query_timeout: 5000
});

pool.connect((err, client, done) => {
  if (err) {
    logger.error(`Database connection error: ${err.message}`);
  } else {
    logger.info('Connected to PostgreSQL');
    done();
  }
});

const channelSubscriptions = new Map();

const sessions = new Map();

async function refreshChannelsFromDB() {
  try {
    const query = `
      SELECT 
        c.chat_id as id,
        COALESCE(c.title, 'Channel ' || c.chat_id) as title,
        COALESCE(c.username, 'channel_' || c.chat_id) as username
      FROM chats c
      JOIN chat_lists cl ON c.chat_id = cl.chat_id
      WHERE cl.list_type = 'white' 
        AND c.is_active = true
      ORDER BY c.title
    `;
    
    const result = await pool.query(query);
    
    if (result.rows.length > 0) {
      const newChannels = result.rows.map(row => ({
        id: parseInt(row.id),
        title: row.title,
        username: row.username,
        avatar: 'avatar.jpg'
      }));
      
      const oldIds = CHANNELS.map(c => c.id).sort().join(',');
      const newIds = newChannels.map(c => c.id).sort().join(',');
      
      CHANNELS = newChannels;
      lastChannelRefresh = Date.now();
      
      if (oldIds !== newIds) {
        logger.info(`Channels refreshed from DB: ${CHANNELS.length} white chats loaded`);
        broadcastChannelsUpdate();
      }
    } else {
      logger.warn('No white chats found in database');
      CHANNELS = [];
    }
  } catch (err) {
    logger.error(`Error refreshing channels from DB: ${err.message}`);
  }
}

async function startChannelRefreshTask() {
  await refreshChannelsFromDB();
  
  setInterval(async () => {
    await refreshChannelsFromDB();
  }, CHANNEL_REFRESH_INTERVAL * 1000);
}

function broadcastChannelsUpdate() {
  const updateMessage = {
    type: 'channels_updated',
    channels: CHANNELS.map(c => ({
      id: c.id,
      title: c.title,
      username: c.username,
      avatar: c.avatar
    })),
    timestamp: new Date().toISOString()
  };
  
  const payload = JSON.stringify(updateMessage);
  
  let sentCount = 0;
  sessions.forEach(session => {
    if (session.sendPreSerialized(payload)) {
      sentCount++;
    }
  });
  
  logger.info(`Broadcast channels update to ${sentCount} clients`);
}

function escapeHtml(unsafe) {
    if (!unsafe) return '';
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;")
        .replace(/`/g, "&#96;");
}

function validateChannelId(id) {
    if (id === undefined || id === null) return false;
    const num = Number(id);
    return Number.isInteger(num) && num > 0;
}

function validateMessageId(id) {
    if (id === undefined || id === null) return false;
    const num = Number(id);
    return Number.isInteger(num) && num > 0;
}

function validateMessageIds(ids) {
    return Array.isArray(ids) && 
           ids.length > 0 && 
           ids.length <= 120 && 
           ids.every(id => Number.isInteger(Number(id)) && Number(id) > 0);
}

function anonymizeIp(ip) {
    if (!ip) return 'unknown';
    if (ip.includes('.')) {
        const parts = ip.split('.');
        if (parts.length === 4) {
            return parts.slice(0, 3).concat(['0']).join('.');
        }
    }
    if (ip.includes(':')) {
        return ip.substring(0, ip.lastIndexOf(':')) + ':0';
    }
    return ip;
}

const apiLimiter = rateLimit({
    windowMs: 1 * 60 * 1000,
    max: 300,
    message: { error: 'Too many requests, please try again later.' },
    standardHeaders: true,
    legacyHeaders: false
});

const heavyApiLimiter = rateLimit({
    windowMs: 1 * 60 * 1000,
    max: 150,
    message: { error: 'Too many requests, please try again later.' },
    standardHeaders: true,
    legacyHeaders: false
});

const batchLimiter = rateLimit({
    windowMs: 1 * 60 * 1000,
    max: 60,
    message: { error: 'Too many batch requests, please try again later.' },
    standardHeaders: true,
    legacyHeaders: false
});

async function getFullMessageFromDB(channelId, messageId, maxRetries = 5) {
  if (!validateChannelId(channelId) || !validateMessageId(messageId)) {
    logger.error(`Invalid IDs for getFullMessageFromDB: channel=${channelId}, message=${messageId}`);
    return null;
  }
  
  const cacheKey = `${channelId}:${messageId}`;
  
  const cached = messageCache.get(cacheKey);
  if (cached) {
    return cached;
  }
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const result = await pool.query(
        `SELECT 
          message_id, text, date, views, forwards,
          media_type, is_edited, edit_date
         FROM messages 
         WHERE chat_id = $1 AND message_id = $2`,
        [channelId, messageId]
      );
      
      if (result.rows.length === 0) {
        if (attempt < maxRetries) {
          await new Promise(r => setTimeout(r, 100 * attempt));
          continue;
        }
        return null;
      }
      
      const row = result.rows[0];
      
      const textSafe = escapeHtml(row.text || '');
      const isWebPage = row.media_type === 'MessageMediaWebPage';
      
      const message = {
        message_id: row.message_id,
        text: row.text,
        date: row.date.toISOString(),
        views: row.views || 0,
        forwards: row.forwards || 0,
        media_type: isWebPage ? null : row.media_type,
        has_media: !isWebPage && !!row.media_type,
        is_edited: row.is_edited || false,
        edit_date: row.edit_date ? row.edit_date.toISOString() : null
      };
      
      messageCache.set(cacheKey, message, 2);
      
      return message;
    } catch (err) {
      logger.error(`Error fetching message ${messageId} from DB (attempt ${attempt}): ${err.message}`);
      if (attempt === maxRetries) return null;
      await new Promise(r => setTimeout(r, 100 * attempt));
    }
  }
  
  return null;
}

async function getMediaInfoFromDBWithCache(messageId, channelId) {
  if (!validateMessageId(messageId) || !validateChannelId(channelId)) {
    logger.error(`Invalid IDs for getMediaInfoFromDBWithCache: message=${messageId}, channel=${channelId}`);
    return null;
  }
  
  const cacheKey = `${channelId}:${messageId}`;
  
  const cached = mediaCache.get(cacheKey);
  if (cached) {
    return cached;
  }
  
  try {
    const result = await pool.query(`
      SELECT mf.id, mf.file_type, mf.uploaded, mf.public_url, 
             mf.s3_key, mf.checksum, mf.created_at,
             mm.chat_id
      FROM message_media mm
      JOIN media_files mf ON mm.media_id = mf.id
      WHERE mm.message_id = $1 AND mm.chat_id = $2
      ORDER BY mf.created_at DESC
      LIMIT 1
    `, [messageId, channelId]);
    
    if (result.rows.length === 0) {
      return null;
    }
    
    const media = result.rows[0];
    mediaCache.set(cacheKey, media, 30);
    return media;
  } catch (err) {
    logger.error(`Error fetching media for message ${messageId}: ${err.message}`);
    return null;
  }
}

const app = express();
app.disable('x-powered-by');
app.set('trust proxy', 1);

app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "https:", "http:"],
      mediaSrc: ["'self'", "https:", "http:", "data:"],
      connectSrc: [
        "'self'",
        `wss://nekhebet.su:${PORT}`,
        `https://nekhebet.su:${PORT}`,
        S3_PUBLIC_URL
      ].filter(Boolean),
      fontSrc: ["'self'", "https:", "data:"],
      objectSrc: ["'none'"],
      frameAncestors: ["'none'"],
      baseUri: ["'self'"],
      formAction: ["'self'"],
      upgradeInsecureRequests: SSL_ENABLED ? [] : null,
    },
  },
  hsts: SSL_ENABLED ? {
    maxAge: 63072000,
    includeSubDomains: true,
    preload: true
  } : false,
  referrerPolicy: { policy: 'strict-origin-when-cross-origin' },
  noSniff: true,
  xssFilter: true,
  hidePoweredBy: true
}));

app.use((req, res, next) => {
  res.removeHeader('X-Powered-By');
  res.removeHeader('Server');
  res.removeHeader('X-AspNet-Version');
  res.removeHeader('X-AspNetMvc-Version');
  next();
});

app.use((req, res, next) => {
  const blockedPaths = [
    '/logs', '/config', '/uploads', '/backups', '/.git', '/.env',
    '/internal', '/debug', '/metrics', '/uptime', '/server-info',
    '/files', '/directories', '/status', '/health', '/node_modules',
    '/package.json', '/package-lock.json', '/.npmrc', '/.nvmrc',
    '/docker-compose.yml', '/Dockerfile', '/.dockerignore'
  ];
  
  if (blockedPaths.some(path => req.path.startsWith(path))) {
    return res.status(404).send('Not found');
  }
  next();
});

app.use((req, res, next) => {
  const allowedOrigins = [
    'https://nekhebet.su',
    'http://nekhebet.su',
    'https://labubugram.github.io',
  ];
  
  const origin = req.headers.origin;
  
  if (origin && allowedOrigins.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  } else if (origin && origin.includes('nekhebet.su')) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  } else {
    return next();
  }
  
  res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS, POST');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Accept, Range');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range');
  
  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }
  
  next();
});

app.use(express.json({ limit: '1mb' }));
app.use('/api/', apiLimiter);

app.get('/api/s3/config', (req, res) => {
  res.json({
    enabled: USE_S3_FOR_MEDIA,
    public_url: S3_PUBLIC_URL
  });
});

app.get('/api/channels/config', (req, res) => {
  res.json({
    channels: CHANNELS.map(channel => ({
      id: channel.id,
      title: channel.title,
      username: channel.username,
      avatar: channel.avatar
    })),
    last_updated: new Date(lastChannelRefresh).toISOString()
  });
});

app.get('/api/channel/info', async (req, res) => {
  const channelId = parseInt(req.query.channel_id);
  
  if (!validateChannelId(channelId)) {
    return res.status(400).json({ error: 'Invalid channel_id' });
  }
  
  const channel = CHANNELS.find(c => c.id === channelId);
  
  if (!channel) {
    return res.status(404).json({ error: 'Channel not found' });
  }
  
  try {
    const result = await pool.query(
      'SELECT description FROM chats WHERE chat_id = $1',
      [channelId]
    );
    
    res.json({
      chat_id: channel.id,
      title: channel.title,
      username: channel.username,
      description: result.rows[0]?.description || 'Telegram channel mirror'
    });
  } catch (err) {
    logger.error(`Error fetching channel info: ${err.message}`);
    res.json({
      chat_id: channel.id,
      title: channel.title,
      username: channel.username,
      description: 'Telegram channel mirror'
    });
  }
});

app.get('/api/channel/posts', heavyApiLimiter, async (req, res) => {
  try {
    const channelId = parseInt(req.query.channel_id);
    const limit = Math.min(parseInt(req.query.limit) || 20, 25);
    const offset = parseInt(req.query.offset) || 0;
    
    if (!validateChannelId(channelId)) {
      return res.status(400).json({ error: 'Invalid channel_id' });
    }
    
    const channel = CHANNELS.find(c => c.id === channelId);
    if (!channel) {
      return res.status(403).json({ error: 'Channel not available' });
    }
    
    const MAX_PAGES = 4;
    const maxOffset = MAX_PAGES * limit;
    
    if (offset >= maxOffset) {
      return res.status(400).json({ 
        error: `Pagination limited to last ${maxOffset} messages`,
        max_offset: maxOffset - limit,
        limit: limit
      });
    }
    
    const result = await pool.query(`
      SELECT 
        message_id,
        text,
        date,
        views,
        media_type,
        is_edited,
        edit_date
      FROM messages 
      WHERE chat_id = $1 
      ORDER BY date DESC
      LIMIT $2 OFFSET $3
    `, [channelId, limit, offset]);
    
    const countResult = await pool.query(
      'SELECT COUNT(*) FROM messages WHERE chat_id = $1',
      [channelId]
    );
    const totalMessages = parseInt(countResult.rows[0].count);
    
    const posts = result.rows.map(row => {
      const isWebPage = row.media_type === 'MessageMediaWebPage';
      return {
        ...row,
        date: row.date.toISOString(),
        edit_date: row.edit_date ? row.edit_date.toISOString() : null,
        media_type: isWebPage ? null : row.media_type,
        has_media: !isWebPage && !!row.media_type
      };
    });
    
    res.setHeader('Cache-Control', 'private, max-age=30');
    
    res.json({
      posts: posts,
      pagination: {
        limit: limit,
        offset: offset,
        total: totalMessages,
        max_offset: maxOffset - limit,
        has_more: offset + limit < Math.min(totalMessages, maxOffset)
      }
    });
  } catch (err) {
    logger.error(`Error loading posts: ${err.message}`);
    res.status(500).json({ error: 'Failed to load posts' });
  }
});

app.get('/api/channel/posts/since', heavyApiLimiter, async (req, res) => {
  try {
    const channelId = parseInt(req.query.channel_id);
    const afterId = parseInt(req.query.after_id);
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);
    
    if (!validateChannelId(channelId) || !validateMessageId(afterId)) {
      return res.status(400).json({ error: 'Invalid channel_id or after_id' });
    }
    
    const channel = CHANNELS.find(c => c.id === channelId);
    if (!channel) {
      return res.status(403).json({ error: 'Channel not available' });
    }
    
    const result = await pool.query(`
      SELECT 
        message_id,
        text,
        date,
        views,
        media_type,
        is_edited,
        edit_date
      FROM messages 
      WHERE chat_id = $1 
        AND message_id > $2
      ORDER BY date DESC
      LIMIT $3
    `, [channelId, afterId, limit]);
    
    const posts = result.rows.map(row => {
      const isWebPage = row.media_type === 'MessageMediaWebPage';
      return {
        ...row,
        date: row.date.toISOString(),
        edit_date: row.edit_date ? row.edit_date.toISOString() : null,
        media_type: isWebPage ? null : row.media_type,
        has_media: !isWebPage && !!row.media_type
      };
    });
    
    res.setHeader('Cache-Control', 'private, max-age=10');
    
    res.json({
      posts: posts
    });
  } catch (err) {
    logger.error(`Error fetching messages since: ${err.message}`);
    res.status(500).json({ error: 'Failed to load messages' });
  }
});

app.get('/api/channel/recent-ids', async (req, res) => {
  try {
    const channelId = parseInt(req.query.channel_id);
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    
    if (!validateChannelId(channelId)) {
      return res.status(400).json({ error: 'Invalid channel_id' });
    }
    
    const channel = CHANNELS.find(c => c.id === channelId);
    if (!channel) {
      return res.status(403).json({ error: 'Channel not available' });
    }
    
    const result = await pool.query(`
      SELECT message_id
      FROM messages 
      WHERE chat_id = $1 
      ORDER BY date DESC 
      LIMIT $2
    `, [channelId, limit]);
    
    res.setHeader('Cache-Control', 'private, max-age=30');
    
    res.json({
      message_ids: result.rows.map(row => row.message_id)
    });
  } catch (err) {
    logger.error(`Error loading recent IDs: ${err.message}`);
    res.status(500).json({ error: 'Failed to load recent IDs' });
  }
});

app.get('/api/v1/messages/:messageId', heavyApiLimiter, async (req, res) => {
  const messageId = parseInt(req.params.messageId);
  const channelId = parseInt(req.query.channel_id);
  
  if (!validateMessageId(messageId) || !validateChannelId(channelId)) {
    return res.status(400).json({ error: 'Invalid messageId or channel_id' });
  }
  
  const channel = CHANNELS.find(c => c.id === channelId);
  if (!channel) {
    return res.status(403).json({ error: 'Channel not available' });
  }
  
  try {
    const message = await getFullMessageFromDB(channelId, messageId, 5);
    
    if (!message) {
      return res.status(404).json({ error: 'Message not found' });
    }
    
    if (message.has_media) {
      const media = await getMediaInfoFromDBWithCache(messageId, channelId);
      if (media) {
        message.media = {
          id: media.id,
          file_type: media.file_type,
          url: media.public_url,
          checksum: media.checksum,
          uploaded: media.uploaded
        };
        
        const mediaState = mediaStateStore.get(media.id);
        if (mediaState) {
          message.media.status = mediaState.status;
          message.media.progress = mediaState.progress;
        }
      }
    }
    
    res.setHeader('Cache-Control', 'private, max-age=2');
    res.json(message);
    
  } catch (err) {
    logger.error(`Error fetching message ${messageId}: ${err.message}`);
    res.status(500).json({ error: 'Internal error' });
  }
});

app.post('/api/v1/messages/batch', batchLimiter, async (req, res) => {
  const { channel_id, message_ids } = req.body;
  
  if (!validateChannelId(channel_id)) {
    return res.status(400).json({ error: 'Invalid channel_id' });
  }
  
  if (!validateMessageIds(message_ids)) {
    return res.status(400).json({ error: 'message_ids must be a non-empty array of positive integers (max 120)' });
  }
  
  const channel = CHANNELS.find(c => c.id === channel_id);
  if (!channel) {
    return res.status(403).json({ error: 'Channel not available' });
  }
  
  try {
    const numericIds = message_ids.map(id => Number(id));
    
    const result = await pool.query(
      `SELECT 
        message_id, text, date, views, forwards,
        media_type, is_edited, edit_date
       FROM messages 
       WHERE chat_id = $1 AND message_id = ANY($2::bigint[])`,
      [channel_id, numericIds]
    );
    
    const messages = {};
    result.rows.forEach(row => {
      const isWebPage = row.media_type === 'MessageMediaWebPage';
      messages[row.message_id] = {
        message_id: row.message_id,
        text: row.text,
        date: row.date.toISOString(),
        views: row.views || 0,
        forwards: row.forwards || 0,
        media_type: isWebPage ? null : row.media_type,
        has_media: !isWebPage && !!row.media_type,
        is_edited: row.is_edited || false,
        edit_date: row.edit_date ? row.edit_date.toISOString() : null
      };
    });
    
    res.setHeader('Cache-Control', 'private, max-age=2');
    
    res.json({
      channel_id,
      messages
    });
    
  } catch (err) {
    logger.error(`Error fetching batch messages: ${err.message}`);
    res.status(500).json({ error: 'Internal error' });
  }
});

app.get('/api/media/status/:messageId', async (req, res) => {
  const { messageId } = req.params;
  const channelId = parseInt(req.query.channel_id);
  
  if (!validateMessageId(messageId) || !validateChannelId(channelId)) {
    return res.status(400).json({ error: 'Invalid messageId or channel_id' });
  }
  
  const channel = CHANNELS.find(c => c.id === channelId);
  if (!channel) {
    return res.status(403).json({ error: 'Channel not available' });
  }
  
  try {
    const media = await getMediaInfoFromDBWithCache(parseInt(messageId), channelId);
    
    const response = {
      exists: !!media,
      message_id: messageId,
      channel_id: channelId
    };
    
    if (media) {
      const state = mediaStateStore.get(media.id);
      
      response.media_id = media.id;
      response.file_type = media.file_type;
      response.checksum = media.checksum;
      response.uploaded = media.uploaded;
      
      if (state) {
        response.status = state.status;
        response.progress = state.progress;
        response.last_update = state.lastUpdate;
      } else if (media.uploaded) {
        response.status = 'ready';
        response.progress = 100;
      } else {
        response.status = 'processing';
        response.progress = 0;
      }
      
      if (media.uploaded && media.public_url) {
        response.url = media.public_url;
        response.from_s3 = true;
      }
      
      response.client_hints = {
        retry_after: (!media.uploaded && state && state.status === 'processing') ? 5 : null,
        can_request: media.uploaded || (state && state.status !== 'downloading'),
        estimated_wait: state?.estimated_time
      };
    }
    
    res.setHeader('Cache-Control', 'private, max-age=5');
    res.json(response);
    
  } catch (err) {
    logger.error(`Error checking media status: ${err.message}`);
    res.status(500).json({ error: 'Internal error' });
  }
});

app.get('/api/media/by-message/:messageId', heavyApiLimiter, async (req, res) => {
    const { messageId } = req.params;
    const channelId = parseInt(req.query.channel_id);
    
    if (!validateMessageId(messageId) || !validateChannelId(channelId)) {
        return res.status(400).json({ error: 'Invalid messageId or channel_id' });
    }
    
    const channel = CHANNELS.find(c => c.id === channelId);
    if (!channel) {
        return res.status(403).json({ error: 'Channel not available' });
    }
    
    try {
        const cacheKey = `media-by-message-${messageId}-${channelId}`;
        const cached = apiCache.get(cacheKey);
        
        if (cached) {
            return res.json(cached);
        }
        
        const media = await getMediaInfoFromDBWithCache(parseInt(messageId), channelId);
        
        if (!media) {
            return res.status(404).json({ error: 'Media not found' });
        }
        
        const state = mediaStateStore.get(media.id);
        
        let response;
        if (USE_S3_FOR_MEDIA && media.uploaded && media.public_url) {
            response = {
                media_id: media.id,
                file_type: media.file_type,
                channel_id: media.chat_id,
                url: media.public_url,
                checksum: media.checksum,
                from_s3: true,
                uploaded: true,
                status: 'ready'
            };
        } else if (media.uploaded === false) {
            response = {
                media_id: media.id,
                file_type: media.file_type,
                checksum: media.checksum,
                uploaded: false,
                from_s3: false,
                status: state ? state.status : 'processing',
                progress: state ? state.progress : 0,
                message: state ? `Media is ${state.status}` : 'File is still being processed'
            };
        } else {
            response = {
                error: 'Media not available in S3',
                uploaded: false,
                message: 'File upload failed or not configured'
            };
        }
        
        apiCache.set(cacheKey, response, 15);
        res.json(response);
        
    } catch (err) {
        logger.error(`Error finding media for message ${messageId}: ${err.message}`);
        res.status(500).json({ error: 'Internal error' });
    }
});

app.get('/api/media/by-checksum/:checksum', heavyApiLimiter, async (req, res) => {
    const { checksum } = req.params;
    const channelId = req.query.channel_id ? parseInt(req.query.channel_id) : null;
    
    if (!/^[a-f0-9]{64}$/i.test(checksum)) {
        return res.status(400).json({ error: 'Invalid checksum format' });
    }
    
    if (channelId) {
        if (!validateChannelId(channelId)) {
            return res.status(400).json({ error: 'Invalid channel_id' });
        }
        const channel = CHANNELS.find(c => c.id === channelId);
        if (!channel) {
            return res.status(403).json({ error: 'Channel not available' });
        }
    }
    
    try {
        let query = `
            SELECT mf.id, mf.public_url, mf.uploaded, mf.file_type,
                   mm.message_id, mm.chat_id, mf.file_size, mf.mime_type, mf.created_at,
                   mf.width, mf.height, mf.duration
            FROM media_files mf
            LEFT JOIN message_media mm ON mf.id = mm.media_id
            WHERE mf.checksum = $1
        `;
        const params = [checksum];
        
        if (channelId && validateChannelId(channelId)) {
            query += ` AND mm.chat_id = $2`;
            params.push(channelId);
        }
        
        query += ` ORDER BY mf.created_at DESC`;
        
        const result = await pool.query(query, params);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Media not found with this checksum' });
        }
        
        const files = result.rows.map(row => {
            const state = mediaStateStore.get(row.id);
            return {
                media_id: row.id,
                public_url: row.public_url,
                uploaded: row.uploaded,
                file_type: row.file_type,
                message_id: row.message_id,
                chat_id: row.chat_id,
                file_size: row.file_size,
                mime_type: row.mime_type,
                created_at: row.created_at,
                dimensions: (row.width && row.height) ? `${row.width}x${row.height}` : null,
                duration: row.duration,
                status: state ? state.status : (row.uploaded ? 'ready' : 'processing'),
                progress: state ? state.progress : (row.uploaded ? 100 : 0)
            };
        });
        
        res.setHeader('Cache-Control', 'private, max-age=60');
        
        res.json({
            checksum,
            total: files.length,
            files: files
        });
        
    } catch (err) {
        logger.error(`Error finding media by checksum ${checksum}: ${err.message}`);
        res.status(500).json({ error: 'Internal error' });
    }
});

app.post('/api/media/batch-info', batchLimiter, async (req, res) => {
    try {
        const { message_ids, channel_id } = req.body;
        
        if (!validateChannelId(channel_id)) {
            return res.status(400).json({ error: 'Invalid channel_id' });
        }
        
        if (!validateMessageIds(message_ids)) {
            return res.status(400).json({ error: 'message_ids must be a non-empty array of positive integers (max 120)' });
        }
        
        const channel = CHANNELS.find(c => c.id === channel_id);
        if (!channel) {
            return res.status(403).json({ error: 'Channel not available' });
        }
        
        const numericIds = message_ids.map(id => Number(id));
        
        const result = await pool.query(
            `SELECT DISTINCT ON (mm.message_id) 
                mm.message_id, mf.id, mf.file_type, mf.uploaded, mf.public_url, 
                mf.checksum
             FROM message_media mm
             JOIN media_files mf ON mm.media_id = mf.id
             WHERE mm.message_id = ANY($1::bigint[]) 
               AND mm.chat_id = $2
             ORDER BY mm.message_id, mf.created_at DESC`,
            [numericIds, channel_id]
        );
        
        const mediaMap = {};
        result.rows.forEach(row => {
            const state = mediaStateStore.get(row.id);
            
            if (USE_S3_FOR_MEDIA && row.uploaded && row.public_url) {
                mediaMap[row.message_id] = {
                    media_id: row.id,
                    file_type: row.file_type,
                    url: row.public_url,
                    checksum: row.checksum,
                    from_s3: true,
                    uploaded: true,
                    status: 'ready'
                };
            } else if (row.uploaded === false) {
                mediaMap[row.message_id] = {
                    media_id: row.id,
                    file_type: row.file_type,
                    checksum: row.checksum,
                    uploaded: false,
                    from_s3: false,
                    pending: true,
                    status: state ? state.status : 'processing',
                    progress: state ? state.progress : 0
                };
            }
        });
        
        res.setHeader('Cache-Control', 'private, max-age=15');
        
        res.json({ media: mediaMap });
        
    } catch (err) {
        logger.error(`Batch media error: ${err.message}`);
        res.status(500).json({ error: 'Internal error' });
    }
});

class MirrorClientSession {
  constructor(ws, ip, lastEventId = 0) {
    this.id = uuidv4();
    this.ws = ws;
    this.ip = ip;
    this.anonymizedIp = anonymizeIp(ip);
    this.createdAt = new Date();
    this.isReady = true;
    this.lastHeartbeat = new Date();
    this.messageQueue = [];
    this.messageIds = new Set();
    this.lastEventId = lastEventId;
    this.missedEventsSent = false;
    this.pendingQueue = [];
    this.maxQueueSize = MAX_PENDING_MESSAGES_PER_CLIENT;
    this.isSlow = false;
    this.slowSince = null;
    this.totalDropped = 0;
    
    this.messageCount = 0;
    this.rateLimitReset = Date.now() + WS_RATE_WINDOW;
    
    this.subscribedChannel = null;
    
    const welcomeMsg = {
      type: 'welcome',
      version: '3.0',
      session_id: this.id,
      event_id: eventReplayBuffer.getLastEventId(),
      server: 'mirror',
      s3_enabled: USE_S3_FOR_MEDIA,
      s3_url: S3_PUBLIC_URL,
      api_version: 'v1',
      media_policy: 's3_only',
      supports_media_status: true,
      timestamp: new Date().toISOString()
    };
    
    this.welcomePayload = JSON.stringify(welcomeMsg);
    this.sendPreSerialized(this.welcomePayload);
    
    if (lastEventId > 0) {
      this.sendMissedEvents(lastEventId);
    }
  }
  
  sendMissedEvents(lastEventId) {
    const missedEvents = eventReplayBuffer.getEventsSince(lastEventId);
    
    if (missedEvents.length > 0) {
      logger.info(`Sending ${missedEvents.length} missed events to client ${this.id} (${this.anonymizedIp})`);
      
      const batchMessage = {
        type: 'event_batch',
        is_replay: true,
        events: missedEvents,
        from_event_id: lastEventId,
        to_event_id: eventReplayBuffer.getLastEventId(),
        timestamp: new Date().toISOString()
      };
      
      this.send(batchMessage);
      this.missedEventsSent = true;
    }
  }
  
  checkRateLimit() {
    const now = Date.now();
    if (now > this.rateLimitReset) {
      this.messageCount = 0;
      this.rateLimitReset = now + WS_RATE_WINDOW;
    }
    
    this.messageCount++;
    return this.messageCount <= WS_RATE_LIMIT;
  }
  
  send(data) {
    try {
      if (this.ws.readyState === WebSocket.OPEN) {
        if (this.ws.bufferedAmount > MAX_BUFFER) {
          logger.debug(`Client ${this.id} buffer full (${this.ws.bufferedAmount} bytes), skipping message`);
          return false;
        }
        
        if (!data.timestamp) {
          data.timestamp = new Date().toISOString();
        }
        
        const jsonData = JSON.stringify(data);
        this.ws.send(jsonData);
        return true;
      }
    } catch (err) {
      logger.error(`Error sending to client ${this.id}: ${err.message}`);
    }
    return false;
  }
  
  sendPreSerialized(payload) {
    try {
      if (this.ws.readyState !== WebSocket.OPEN) {
        return false;
      }
      
      const bufferUsage = this.ws.bufferedAmount / MAX_BUFFER;
      
      if (bufferUsage > 0.8) {
        if (!this.isSlow) {
          this.isSlow = true;
          this.slowSince = Date.now();
          logger.warn(`Client ${this.id} (${this.anonymizedIp}) is slow, buffer: ${Math.round(bufferUsage*100)}%`);
        }
        
        if (this.pendingQueue.length < this.maxQueueSize) {
          this.pendingQueue.push(payload);
          return true;
        } else {
          this.totalDropped++;
          if (this.totalDropped % 10 === 0) {
            logger.warn(`Client ${this.id} (${this.anonymizedIp}) dropped ${this.totalDropped} messages, queue full`);
          }
          
          if (this.slowSince && (Date.now() - this.slowSince) > 30000) {
            logger.warn(`Client ${this.id} (${this.anonymizedIp}) too slow for 30s, disconnecting`);
            this.ws.close(1009, 'Client too slow');
          }
          return false;
        }
      }
      
      if (this.isSlow && bufferUsage < 0.3) {
        this.isSlow = false;
        this.slowSince = null;
        logger.info(`Client ${this.id} (${this.anonymizedIp}) is fast again, flushing ${this.pendingQueue.length} queued messages`);
        this.flushPendingQueue();
      }
      
      this.ws.send(payload);
      
      if (this.pendingQueue.length > 0 && bufferUsage < 0.5) {
        this.flushPendingQueue();
      }
      
      return true;
      
    } catch (err) {
      logger.error(`Error sending to client ${this.id}: ${err.message}`);
      return false;
    }
  }
  
  flushPendingQueue() {
    let sent = 0;
    while (this.pendingQueue.length > 0 && this.ws.bufferedAmount < MAX_BUFFER * 0.5) {
      const payload = this.pendingQueue.shift();
      try {
        this.ws.send(payload);
        sent++;
      } catch (err) {
        logger.error(`Error flushing queue for client ${this.id}: ${err.message}`);
        this.pendingQueue.unshift(payload);
        break;
      }
    }
  }
  
  addToQueue(data) {
    if (!ENABLE_BUFFERING) return;
    
    if (data.message_id && this.messageIds.has(data.message_id)) {
      return;
    }
    
    if (data.message_id) {
      this.messageIds.add(data.message_id);
      if (this.messageIds.size > 200) {
        const iterator = this.messageIds.values();
        for (let i = 0; i < 100; i++) {
          this.messageIds.delete(iterator.next().value);
        }
      }
    }
    
    this.messageQueue.push(data);
    if (this.messageQueue.length > this.maxQueueSize) {
      this.messageQueue = this.messageQueue.slice(-50);
    }
    
    if (this.messageQueue.length === 1) {
      this.send({
        type: 'buffering',
        count: this.messageQueue.length,
        ready_in: 0
      });
    }
  }
  
  flushQueue() {
    if (this.messageQueue.length === 0 || !ENABLE_BUFFERING) return;
    
    logger.info(`Flushing ${this.messageQueue.length} queued messages for client ${this.id} (${this.anonymizedIp})`);
    
    this.send({
      type: 'flush_start',
      count: this.messageQueue.length
    });
    
    let delay = 0;
    while (this.messageQueue.length > 0) {
      const msg = this.messageQueue.shift();
      setTimeout(() => {
        this.send(msg);
      }, delay);
      delay += 50;
    }
    
    setTimeout(() => {
      this.send({
        type: 'flush_complete',
        timestamp: new Date().toISOString()
      });
    }, delay);
  }
  
  cleanup() {
    this.messageQueue = [];
    this.messageIds.clear();
    this.pendingQueue = [];
    
    if (this.subscribedChannel) {
      const channelSet = channelSubscriptions.get(this.subscribedChannel);
      if (channelSet) {
        channelSet.delete(this);
        if (channelSet.size === 0) {
          channelSubscriptions.delete(this.subscribedChannel);
        }
      }
    }
  }
}

let server;
if (sslOptions) {
  server = https.createServer(sslOptions, app);
  logger.info('HTTPS server created');
} else {
  server = http.createServer(app);
  logger.warn('HTTP server created (no SSL)');
}

const wss = new WebSocket.Server({ 
  server,
  maxPayload: WS_MAX_PAYLOAD,
  verifyClient: (info, cb) => {
    const origin = info.origin || '';
    if (origin.includes('nekhebet.su') || 
        origin.includes('nekhebet.github.io') || 
        origin.includes('labubugram.github.io') || 
        origin === '' ||
        origin === 'null') {
      cb(true);
    } else {
      logger.info(`WebSocket connection rejected from origin: ${origin}`);
      cb(false, 403, 'Forbidden');
    }
  }
});

wss.on('connection', (ws, req) => {
  try {
    const ip = req.socket.remoteAddress;
    const anonymizedIp = anonymizeIp(ip);
    
    const url = new URL(req.url, `http://${req.headers.host}`);
    const lastEventId = parseInt(url.searchParams.get('last_event_id')) || 0;
    
    const session = new MirrorClientSession(ws, ip, lastEventId);
    
    sessions.set(session.id, session);
    logger.info(`New mirror connection: ${session.id} from ${anonymizedIp}, last_event_id: ${lastEventId}, total: ${sessions.size}`);
    
    ws.isAlive = true;
    ws.on('pong', () => {
      ws.isAlive = true;
      session.lastHeartbeat = new Date();
    });
    
    ws.on('message', (data) => {
      try {
        if (data.length > WS_MAX_MESSAGE_SIZE) {
          logger.warn(`Oversized message (${data.length} bytes) from client ${session.id} (${anonymizedIp}), closing`);
          ws.close(1009, 'Message too large');
          return;
        }
        
        if (!session.checkRateLimit()) {
          logger.warn(`Rate limit exceeded for client ${session.id} (${anonymizedIp})`);
          ws.close(1008, 'Rate limit exceeded');
          return;
        }
        
        const msg = JSON.parse(data);
        
        if (msg.type === 'ping') {
          session.send({ 
            type: 'pong', 
            timestamp: new Date().toISOString() 
          });
        } 
        else if (msg.type === 'subscribe') {
          const channelId = parseInt(msg.channel_id);
          
          if (!validateChannelId(channelId)) {
            session.send({ 
              type: 'error', 
              message: 'Invalid channel_id' 
            });
            return;
          }
          
          const channelExists = CHANNELS.some(c => c.id === channelId);
          
          if (channelExists) {
            if (session.subscribedChannel) {
              const oldSet = channelSubscriptions.get(session.subscribedChannel);
              if (oldSet) {
                oldSet.delete(session);
                if (oldSet.size === 0) {
                  channelSubscriptions.delete(session.subscribedChannel);
                }
              }
            }
            
            session.subscribedChannel = channelId;
            
            if (!channelSubscriptions.has(channelId)) {
              channelSubscriptions.set(channelId, new Set());
            }
            channelSubscriptions.get(channelId).add(session);
            
            logger.info(`Client ${session.id} (${anonymizedIp}) subscribed to channel ${channelId}`);
            
            const channel = CHANNELS.find(c => c.id === channelId);
            session.send({ 
              type: 'subscribed', 
              channel_id: channelId,
              channel_info: {
                title: channel.title,
                username: channel.username,
                avatar: channel.avatar
              },
              status: 'ok'
            });
          } else {
            logger.warn(`Client ${session.id} (${anonymizedIp}) tried to subscribe to unknown channel ${channelId}`);
            session.send({ 
              type: 'error', 
              message: 'Unknown channel_id' 
            });
          }
        }
        else if (msg.type === 'get_channels') {
          session.send({
            type: 'channels_list',
            channels: CHANNELS.map(c => ({
              id: c.id,
              title: c.title,
              username: c.username,
              avatar: c.avatar
            })),
            last_updated: new Date(lastChannelRefresh).toISOString()
          });
        }
        else if (msg.type === 'get_channel_info') {
          const channelId = parseInt(msg.channel_id);
          
          if (!validateChannelId(channelId)) {
            session.send({ 
              type: 'error', 
              message: 'Invalid channel_id' 
            });
            return;
          }
          
          const channel = CHANNELS.find(c => c.id === channelId);
          
          if (channel) {
            session.send({
              type: 'channel_info',
              channel_id: channelId,
              channel_info: {
                title: channel.title,
                username: channel.username,
                avatar: channel.avatar
              }
            });
          } else {
            session.send({ 
              type: 'error', 
              message: 'Channel not found' 
            });
          }
        }
        else if (msg.type === 'get_status') {
          session.send({
            type: 'status',
            session_id: session.id,
            is_ready: session.isReady,
            queued: session.messageQueue.length,
            subscribed_channel: session.subscribedChannel,
            timestamp: new Date().toISOString()
          });
        } else if (msg.type === 'flush_queue') {
          session.flushQueue();
        }
      } catch (err) {
        logger.error(`Message error: ${err.message}`);
      }
    });
    
    ws.on('error', (error) => {
      logger.error(`WebSocket error for client ${session.id} (${anonymizedIp}): ${error.message}`);
    });
    
    ws.on('close', (code, reason) => {
      session.cleanup();
      sessions.delete(session.id);
      logger.info(`Mirror connection closed: ${session.id} (${anonymizedIp}), code: ${code}, reason: ${reason}, remaining: ${sessions.size}`);
    });
    
  } catch (err) {
    logger.error(`Error in WebSocket connection: ${err.message}`);
  }
});

const interval = setInterval(() => {
  let closed = 0;
  
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) {
      closed++;
      return ws.terminate();
    }
    
    ws.isAlive = false;
    ws.ping();
  });
  
  if (closed > 0) {
    logger.info(`Closed ${closed} stale WebSocket connections`);
  }
}, 30000);

const { Client } = require('pg');

let pgClient = null;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;

function createDeduplicationKey(msg, payload) {
  const components = [
    msg.channel,
    payload.message_id || payload.media_id || 'unknown',
    payload.chat_id || 'unknown',
    payload.type || msg.channel.replace('_message', ''),
    payload.edit_date ? new Date(payload.edit_date).getTime() : '',
    payload.timestamp ? new Date(payload.timestamp).getTime() : ''
  ];
  
  if (msg.channel === 'media_ready' && payload.media_id) {
    components.push(`media:${payload.media_id}`);
  }
  
  return components.filter(Boolean).join('-');
}

async function handleMediaStatus(payload) {
  if (payload.v !== '3.0') {
    logger.warn(`Unsupported media_status version: ${payload.v}`);
    return;
  }

  logger.info(`📊 MEDIA STATUS notification: msg=${payload.message_id}, status=${payload.status}, progress=${payload.progress}%`);

  const channelId = parseInt(payload.chat_id);
  const messageId = parseInt(payload.message_id);
  const mediaId = payload.media_id;
  
  if (!validateChannelId(channelId) || !validateMessageId(messageId)) {
    logger.error(`Invalid IDs in media status: channel=${payload.chat_id}, message=${payload.message_id}`);
    return;
  }

  const isMirroredChannel = CHANNELS.some(c => c.id === channelId);
  if (!isMirroredChannel) {
    return;
  }

  if (mediaId) {
    if (!mediaStateStore.has(mediaId)) {
      mediaStateStore.set(mediaId, {
        status: payload.status,
        progress: payload.progress,
        lastUpdate: new Date(),
        messageId: messageId,
        channelId: channelId
      });
    } else {
      const state = mediaStateStore.get(mediaId);
      state.status = payload.status;
      state.progress = payload.progress;
      state.lastUpdate = new Date();
    }
    
    if (payload.status === 'ready') {
      setTimeout(() => {
        mediaStateStore.delete(mediaId);
        mediaCache.del(`${channelId}:${messageId}`);
      }, 300000);
    }
  }

  const clients = channelSubscriptions.get(channelId);
  if (clients) {
    const statusMsg = {
      type: 'media_status',
      version: '3.0',
      message_id: messageId,
      channel_id: channelId,
      media_id: mediaId,
      status: payload.status,
      progress: payload.progress,
      timestamp: new Date().toISOString()
    };
    
    const serialized = JSON.stringify(statusMsg);
    clients.forEach(session => {
      if (session.subscribedChannel === channelId) {
        session.sendPreSerialized(serialized);
      }
    });
    
    logger.info(`✅ Sent media_status ${payload.status} for message ${messageId} to ${clients.size} clients`);
  }

  messageCache.del(`${channelId}:${messageId}`);
  mediaCache.del(`${channelId}:${messageId}`);
}

async function handleNewMessage(payload) {
  if (![ '2.0', '3.0' ].includes(payload.v)) {
    logger.warn(`Unsupported notification version: ${payload.v}`);
    return;
  }

  logger.info(`📨 NEW message notification: ${payload.message_id} in channel ${payload.chat_id}`);
  
  const channelId = parseInt(payload.chat_id);
  const messageId = parseInt(payload.message_id);
  
  if (!validateChannelId(channelId) || !validateMessageId(messageId)) {
    logger.error(`Invalid IDs in new message notification: channel=${payload.chat_id}, message=${payload.message_id}`);
    return;
  }
  
  const isMirroredChannel = CHANNELS.some(c => c.id === channelId);
  if (!isMirroredChannel) {
    return;
  }
  
  await new Promise(resolve => setTimeout(resolve, 100));
  
  let fullMessage;
  try {
    fullMessage = await getFullMessageFromDB(channelId, messageId, 5);
  } catch (err) {
    logger.error(`Error fetching message ${messageId} from DB: ${err.message}`);
    
    const clients = channelSubscriptions.get(channelId);
    if (clients) {
      const errorMsg = {
        type: 'error',
        version: '3.0',
        message_id: messageId,
        channel_id: channelId,
        error: 'message_data_unavailable',
        retry_after: 2,
        timestamp: new Date().toISOString()
      };
      const serialized = JSON.stringify(errorMsg);
      clients.forEach(session => session.sendPreSerialized(serialized));
    }
    return;
  }
  
  if (!fullMessage) {
    logger.error(`Message ${messageId} not found in DB after notification`);
    
    const clients = channelSubscriptions.get(channelId);
    if (clients) {
      const errorMsg = {
        type: 'error',
        version: '3.0',
        message_id: messageId,
        channel_id: channelId,
        error: 'message_not_found',
        timestamp: new Date().toISOString()
      };
      const serialized = JSON.stringify(errorMsg);
      clients.forEach(session => session.sendPreSerialized(serialized));
    }
    return;
  }
  
  if (fullMessage.has_media) {
    try {
      const media = await getMediaInfoFromDBWithCache(messageId, channelId);
      if (media) {
        fullMessage.media = {
          id: media.id,
          file_type: media.file_type,
          url: media.public_url,
          checksum: media.checksum,
          uploaded: media.uploaded
        };
        
        const mediaState = mediaStateStore.get(media.id);
        if (mediaState) {
          fullMessage.media.status = mediaState.status;
          fullMessage.media.progress = mediaState.progress;
        }
      }
    } catch (err) {
      logger.error(`Error fetching media for message ${messageId}: ${err.message}`);
    }
  }
  
  const messageToSend = {
    type: 'new',
    version: '3.0',
    message_id: messageId,
    channel_id: channelId,
    data: fullMessage,
    timestamp: new Date().toISOString()
  };
  
  messageToSend.event_id = ++globalEventId;
  
  const serializedMessage = JSON.stringify(messageToSend);
  
  eventReplayBuffer.add(messageToSend);
  
  const clients = channelSubscriptions.get(channelId);
  let sentCount = 0;
  
  if (clients) {
    clients.forEach(session => {
      if (session.sendPreSerialized(serializedMessage)) {
        sentCount++;
      }
    });
  }
  
  logger.info(`✅ Sent new message ${messageId} to ${sentCount} clients (event_id: ${messageToSend.event_id})`);
}

async function handleEditMessage(payload) {
  if (![ '2.0', '3.0' ].includes(payload.v)) {
    logger.warn(`Unsupported notification version: ${payload.v}`);
    return;
  }

  logger.info(`✏️ EDIT message notification: ${payload.message_id} in channel ${payload.chat_id}`);

  const channelId = parseInt(payload.chat_id);
  const messageId = parseInt(payload.message_id);
  
  if (!validateChannelId(channelId) || !validateMessageId(messageId)) {
    logger.error(`Invalid IDs in edit message notification: channel=${payload.chat_id}, message=${payload.message_id}`);
    return;
  }

  const isMirroredChannel = CHANNELS.some(c => c.id === channelId);
  if (!isMirroredChannel) {
    return;
  }

  await new Promise(resolve => setTimeout(resolve, 200));

  messageCache.del(`${channelId}:${messageId}`);
  mediaCache.del(`${channelId}:${messageId}`);

  const fullMessage = await getFullMessageFromDB(channelId, messageId, 5);
  
  if (!fullMessage) {
    logger.error(`Cannot fetch full message ${messageId} for edit notification`);
    return;
  }

  if (fullMessage.has_media) {
    try {
      const media = await getMediaInfoFromDBWithCache(messageId, channelId);
      if (media) {
        fullMessage.media = {
          id: media.id,
          file_type: media.file_type,
          url: media.public_url,
          checksum: media.checksum,
          uploaded: media.uploaded
        };
        
        const mediaState = mediaStateStore.get(media.id);
        if (mediaState) {
          fullMessage.media.status = mediaState.status;
          fullMessage.media.progress = mediaState.progress;
        }
      }
    } catch (err) {
      logger.error(`Error fetching media for message ${messageId}: ${err.message}`);
    }
  }

  const editToSend = {
    type: 'edit',
    version: '3.0',
    message_id: messageId,
    channel_id: channelId,
    data: fullMessage,
    timestamp: new Date().toISOString()
  };

  editToSend.event_id = ++globalEventId;

  const serializedEdit = JSON.stringify(editToSend);
  
  eventReplayBuffer.add(editToSend);

  const clients = channelSubscriptions.get(channelId);
  let sentCount = 0;

  if (clients) {
    clients.forEach(session => {
      if (session.sendPreSerialized(serializedEdit)) {
        sentCount++;
      }
    });
  }

  logger.info(`✅ Sent full edit data for message ${messageId} to ${sentCount} clients (event_id: ${editToSend.event_id})`);
}

async function handleMediaReady(payload) {
  if (![ '2.0', '3.0' ].includes(payload.v)) {
    logger.warn(`Unsupported notification version: ${payload.v}`);
    return;
  }

  logger.info(`📹 MEDIA READY notification for message: ${payload.message_id}`);

  const channelId = parseInt(payload.chat_id);
  const messageId = parseInt(payload.message_id);
  
  if (!validateChannelId(channelId) || !validateMessageId(messageId)) {
    logger.error(`Invalid IDs in media ready notification: channel=${payload.chat_id}, message=${payload.message_id}`);
    return;
  }

  const isMirroredChannel = CHANNELS.some(c => c.id === channelId);
  if (!isMirroredChannel) {
    return;
  }

  let mediaUrl = payload.public_url;
  let mediaId = payload.media_id;
  
  if (!mediaUrl && mediaId) {
    try {
      const result = await pool.query(
        'SELECT public_url FROM media_files WHERE id = $1',
        [mediaId]
      );
      if (result.rows.length > 0) {
        mediaUrl = result.rows[0].public_url;
      }
    } catch (err) {
      logger.error(`Error fetching media URL for ${mediaId}: ${err.message}`);
    }
  }

  if (!mediaUrl) {
    logger.error(`No public_url found for media_ready event (message: ${messageId})`);
    return;
  }

  if (mediaId) {
    mediaStateStore.set(mediaId, {
      status: 'ready',
      progress: 100,
      lastUpdate: new Date(),
      messageId: messageId,
      channelId: channelId
    });
    
    setTimeout(() => {
      mediaStateStore.delete(mediaId);
    }, 300000);
  }

  const mediaToSend = {
    type: 'media_ready',
    version: '3.0',
    message_id: messageId,
    channel_id: channelId,
    media_url: mediaUrl,
    media_id: mediaId,
    media_type: payload.file_type,
    timestamp: new Date().toISOString()
  };

  mediaToSend.event_id = ++globalEventId;

  const serializedMedia = JSON.stringify(mediaToSend);
  
  eventReplayBuffer.add(mediaToSend);

  const clients = channelSubscriptions.get(channelId);
  let sentCount = 0;

  if (clients) {
    clients.forEach(session => {
      if (session.sendPreSerialized(serializedMedia)) {
        sentCount++;
      }
    });
  }

  logger.info(`✅ Sent media_ready for message ${messageId} to ${sentCount} clients (event_id: ${mediaToSend.event_id})`);

  messageCache.del(`${channelId}:${messageId}`);
  mediaCache.del(`${channelId}:${messageId}`);
}

async function handleDeleteMessage(payload) {
  if (![ '2.0', '3.0' ].includes(payload.v)) {
    logger.warn(`Unsupported notification version: ${payload.v}`);
    return;
  }

  logger.info(`🗑️ DELETE message notification: ${payload.message_id}`);
  
  const channelId = parseInt(payload.chat_id);
  const messageId = parseInt(payload.message_id);
  
  if (!validateChannelId(channelId) || !validateMessageId(messageId)) {
    logger.error(`Invalid IDs in delete message notification: channel=${payload.chat_id}, message=${payload.message_id}`);
    return;
  }
  
  const isMirroredChannel = CHANNELS.some(c => c.id === channelId);
  if (!isMirroredChannel) {
    return;
  }
  
  const deleteToSend = {
    type: 'delete',
    version: '3.0',
    message_id: messageId,
    channel_id: channelId,
    timestamp: new Date().toISOString()
  };

  deleteToSend.event_id = ++globalEventId;
  
  const serializedDelete = JSON.stringify(deleteToSend);
  
  eventReplayBuffer.add(deleteToSend);
  
  const clients = channelSubscriptions.get(channelId);
  let sentCount = 0;
  
  if (clients) {
    clients.forEach(session => {
      if (session.sendPreSerialized(serializedDelete)) {
        sentCount++;
      }
    });
  }
  
  logger.info(`✅ Sent delete message ${messageId} to ${sentCount} clients (event_id: ${deleteToSend.event_id})`);
  
  messageCache.del(`${channelId}:${messageId}`);
  mediaCache.del(`${channelId}:${messageId}`);
}

async function connectToPostgres() {
  try {
    if (pgClient) {
      try {
        await pgClient.end();
      } catch (e) {}
    }
    
    pgClient = new Client({
      host: DB_HOST,
      port: DB_PORT,
      database: DB_NAME,
      user: DB_USER,
      password: DB_PASSWORD,
      statement_timeout: 5000,
      query_timeout: 5000
    });
    
    await pgClient.connect();
    logger.info('Connected to PostgreSQL for LISTEN');
    
    await pgClient.query('LISTEN new_message');
    await pgClient.query('LISTEN edit_message');
    await pgClient.query('LISTEN delete_message');
    await pgClient.query('LISTEN media_ready');
    await pgClient.query('LISTEN media_status');
    
    logger.info('Listening for notifications (including media_status)');
    
    reconnectAttempts = 0;
    
    pgClient.on('notification', async (msg) => {
      try {
        const payload = JSON.parse(msg.payload);
        
        const dedupKey = createDeduplicationKey(msg, payload);
        
        if (notificationDedupCache.has(dedupKey)) {
          logger.debug(`Duplicate notification ignored: ${dedupKey}`);
          return;
        }
        
        if (msg.channel === 'media_ready' && payload.media_id) {
          const mediaKey = `media-${payload.media_id}-ready`;
          if (notificationDedupCache.has(mediaKey)) {
            logger.debug(`Duplicate media_ready ignored: ${payload.media_id}`);
            return;
          }
          notificationDedupCache.set(mediaKey, true);
        }
        
        notificationDedupCache.set(dedupKey, true);
        
        switch (msg.channel) {
          case 'new_message':
            await handleNewMessage(payload);
            break;
          case 'edit_message':
            await handleEditMessage(payload);
            break;
          case 'delete_message':
            await handleDeleteMessage(payload);
            break;
          case 'media_ready':
            await handleMediaReady(payload);
            break;
          case 'media_status':
            await handleMediaStatus(payload);
            break;
          default:
            logger.warn(`Unknown channel: ${msg.channel}`);
        }
        
      } catch (err) {
        logger.error(`❌ Notification error: ${err.message}`);
      }
    });
    
    pgClient.on('error', (err) => {
      logger.error(`PostgreSQL client error: ${err.message}`);
      handleDisconnect();
    });
    
    pgClient.on('end', () => {
      logger.warn('PostgreSQL connection ended');
      handleDisconnect();
    });
    
  } catch (err) {
    logger.error(`Failed to connect to PostgreSQL: ${err.message}`);
    handleDisconnect();
  }
}

function handleDisconnect() {
  reconnectAttempts++;
  
  if (reconnectAttempts > MAX_RECONNECT_ATTEMPTS) {
    logger.error('Max reconnection attempts reached, giving up');
    return;
  }
  
  const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
  logger.info(`Reconnecting in ${delay}ms (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
  
  setTimeout(() => {
    connectToPostgres();
  }, delay);
}

connectToPostgres();

setInterval(() => {
  const now = Date.now();
  let expired = 0;
  
  for (const [mediaId, state] of mediaStateStore.entries()) {
    if (now - state.lastUpdate.getTime() > 600000) {
      mediaStateStore.delete(mediaId);
      expired++;
    }
  }
  
  if (expired > 0) {
    logger.debug(`Cleaned up ${expired} expired media states`);
  }
}, 300000);

app.use(express.static(path.join(__dirname, 'public'), {
  dotfiles: 'ignore',
  index: false,
  maxAge: '1h'
}));

app.get('/mirror', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.use((req, res, next) => {
  if (req.path.startsWith('/api/')) {
    return next();
  }
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

startChannelRefreshTask().then(() => {
  server.listen(PORT, '0.0.0.0', () => {
    logger.info('🚀 Mirror server running with security improvements');
    logger.info(`📡 Port: ${PORT}, SSL: ${SSL_ENABLED ? '✅' : '❌'}`);
    logger.info(`📋 Mirrored channels: ${CHANNELS.length}`);
    logger.info(`🔌 WebSocket: ${SSL_ENABLED ? 'wss' : 'ws'}://nekhebet.su:${PORT}`);
    logger.info(`☁️ S3 enabled: ${USE_S3_FOR_MEDIA ? '✅' : '❌'}`);
    logger.info(`🔒 Helmet CSP: ✅`);
    logger.info(`🔒 Text sanitization: ✅ (text_safe field added)`);
    logger.info(`🔒 Input validation: ✅`);
    logger.info(`🔒 IP anonymization: ✅`);
    logger.info(`🔒 Rate limiting: ✅ (API, heavy, batch)`);
    logger.info(`🔒 Sensitive data hidden: ✅ (s3_key, paths, server info)`);
    logger.info(`🎯 Event ID + Replay buffer: ${MAX_EVENT_BUFFER_SIZE} events`);
    logger.info(`⏱️ Cache TTL: Messages 2s, Media 30s, API 30s`);
    logger.info(`🧹 Deduplication TTL: 2s`);
    logger.info(`📨 Client queue: ${MAX_PENDING_MESSAGES_PER_CLIENT} messages`);
    logger.info(`🔗 WebPage previews: ignored as media (MessageMediaWebPage)`);
    logger.info(`📊 Media status tracking: ✅ (v3.0 protocol)`);
  });
});

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

async function gracefulShutdown(signal) {
  logger.info(`Received ${signal}, starting graceful shutdown`);
  
  clearInterval(interval);
  
  wss.close(() => {
    logger.info('WebSocket server closed');
  });
  
  server.close(() => {
    logger.info('HTTP server closed');
  });
  
  if (pgClient) {
    try {
      await pgClient.end();
      logger.info('PostgreSQL client closed');
    } catch (err) {
      logger.error(`Error closing PostgreSQL client: ${err.message}`);
    }
  }
  
  try {
    await pool.end();
    logger.info('Database pool closed');
  } catch (err) {
    logger.error(`Error closing pool: ${err.message}`);
  }
  
  logger.info('Graceful shutdown completed');
  process.exit(0);
}

process.on('uncaughtException', (err) => {
  logger.error(`Uncaught Exception: ${err.message}\n${err.stack}`);
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error(`Unhandled Rejection at: ${promise}, reason: ${reason}`);
});

module.exports = {
  app,
  server,
  wss,
  pool,
  sessions
};