/**
 * ZapSpy.ai Backend API
 * Lead capture and admin panel API
 * With Facebook Conversions API integration
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;

// Trust proxy for Railway/Heroku (needed for rate limiting and IP detection)
app.set('trust proxy', 1);

// ==================== FACEBOOK CONVERSIONS API ====================

// Pixel configurations by funnel language
const FB_PIXELS_BY_LANGUAGE = {
    // English funnel pixels - ONLY 726299943423075
    en: [
        {
            id: '726299943423075',
            token: process.env.FB_PIXEL_TOKEN_EN || 'EAALZCphpZCmcIBQodgl2fJ81kKfOWRmhYmJPBVQSfOuBBbxfjOxg3HH6y03bqp8fAbZCoghz8d9HglfpbBeZBl7wTaBGvIWRqtNgoJCFz5lts434LKD5EhF26KZCFjICN9jwsEdDu4afDUYH8Ld5ZC9D8gRFq3Y884qotjlqIszrQAzZAju7qkt9OgMhX7X093PNQZDZD',
            name: '[PABLO NOVO] - [SPY INGLES] - [2025]'
        }
    ],
    // Spanish funnel pixels - ONLY 534495082571779
    es: [
        {
            id: '534495082571779',
            token: process.env.FB_PIXEL_TOKEN_ES || 'EAALZCphpZCmcIBQh5zHSNNj666RUi8XybMe3ZBRE31J9czSE04LBY4nZC9PBNG8SFNL4yCJf6zb9V88JkjNz55nTaIZC2wKSW22OhohIBY0IyYPYXTBFQTBVWUUIYDHhgZBf1CDVye724ekcSA6UbwSqJQPK8XYLEkvUfoJtXq7ktPv7qMOjloAx3jXdjUdJM3TgZDZD',
            name: 'PIXEL SPY ESPANHOL'
        }
    ]
};

// Default to English pixels for backward compatibility
const FB_PIXELS = FB_PIXELS_BY_LANGUAGE.en;

// Get pixels by language (or use provided custom pixels)
function getPixelsForLanguage(language, customPixelIds = null, customAccessToken = null) {
    // If custom pixels are provided (from frontend), use them
    if (customPixelIds && customPixelIds.length > 0 && customAccessToken) {
        return customPixelIds.map(id => ({
            id: id,
            token: customAccessToken,
            name: `Custom Pixel ${id}`
        }));
    }
    
    // Otherwise use the configured pixels for the language
    return FB_PIXELS_BY_LANGUAGE[language] || FB_PIXELS_BY_LANGUAGE.en;
}

const FB_API_VERSION = 'v21.0';

// Hash function for user data (required by Facebook)
function hashData(data) {
    if (!data) return null;
    return crypto.createHash('sha256').update(data.toLowerCase().trim()).digest('hex');
}

// Normalize phone number for Facebook (must include country code)
function normalizePhone(phone, countryCode = null) {
    if (!phone) return null;
    // Remove all non-numeric characters
    let normalized = phone.replace(/\D/g, '');
    
    // If phone doesn't start with a country code, try to add one
    // Common patterns: US/CA starts with 1, BR starts with 55, ES/MX etc.
    if (normalized.length > 0) {
        // If it's a short number (10-11 digits), it probably doesn't have country code
        if (normalized.length <= 11 && !normalized.startsWith('1') && !normalized.startsWith('55')) {
            // Try to detect based on countryCode parameter
            if (countryCode) {
                const countryPrefixes = {
                    'US': '1', 'CA': '1', 'BR': '55', 'ES': '34', 'MX': '52',
                    'AR': '54', 'CL': '56', 'CO': '57', 'PE': '51', 'VE': '58',
                    'GB': '44', 'DE': '49', 'FR': '33', 'IT': '39', 'PT': '351'
                };
                const prefix = countryPrefixes[countryCode.toUpperCase()];
                if (prefix && !normalized.startsWith(prefix)) {
                    normalized = prefix + normalized;
                }
            }
        }
    }
    
    return normalized;
}

// Normalize gender for Facebook (m or f, lowercase)
function normalizeGender(gender) {
    if (!gender) return null;
    const g = gender.toLowerCase().trim();
    if (g === 'male' || g === 'm' || g === 'masculino' || g === 'hombre') return 'm';
    if (g === 'female' || g === 'f' || g === 'feminino' || g === 'mujer') return 'f';
    return null;
}

// Send event to Facebook Conversions API
// eventId: if provided, use it for deduplication with browser pixel
// userData.externalId: visitor ID for cross-device tracking
// options.language: 'en' or 'es' to select correct pixels
// options.pixelIds: array of custom pixel IDs (from frontend)
// options.accessToken: custom access token (from frontend)
async function sendToFacebookCAPI(eventName, userData, customData = {}, eventSourceUrl = null, eventId = null, options = {}) {
    // Use provided event_time (actual purchase time) or fallback to current time
    const timestamp = options.eventTime ? Math.floor(new Date(options.eventTime).getTime() / 1000) : Math.floor(Date.now() / 1000);
    // Use provided eventId or generate one
    const finalEventId = eventId || `${eventName}_${timestamp}_${Math.random().toString(36).substr(2, 9)}`;
    
    // Build user_data object
    const user_data = {};
    
    // Email (required, hashed)
    if (userData.email) {
        user_data.em = [hashData(userData.email)];
    }
    
    // Phone (hashed, with country code)
    if (userData.phone) {
        const normalizedPhone = normalizePhone(userData.phone, userData.country);
        if (normalizedPhone) {
            user_data.ph = [hashData(normalizedPhone)];
        }
    }
    
    // First Name and Last Name (hashed)
    if (userData.firstName) {
        const names = userData.firstName.trim().split(' ');
        user_data.fn = [hashData(names[0].toLowerCase())];
        if (names.length > 1) {
            user_data.ln = [hashData(names.slice(1).join(' ').toLowerCase())];
        }
    }
    if (userData.lastName) {
        user_data.ln = [hashData(userData.lastName.toLowerCase())];
    }
    
    // Gender (hashed, m or f)
    if (userData.gender) {
        const normalizedGender = normalizeGender(userData.gender);
        if (normalizedGender) {
            user_data.ge = [hashData(normalizedGender)];
        }
    }
    
    // Client IP Address (NOT hashed - required for server events)
    if (userData.ip) {
        user_data.client_ip_address = userData.ip;
    }
    
    // Client User Agent (NOT hashed - required for server events)
    if (userData.userAgent) {
        user_data.client_user_agent = userData.userAgent;
    }
    
    // Facebook Click ID (NOT hashed)
    if (userData.fbc) {
        user_data.fbc = userData.fbc;
    }
    
    // Facebook Browser ID (NOT hashed)
    if (userData.fbp) {
        user_data.fbp = userData.fbp;
    }
    
    // Country code (2-letter ISO 3166-1 alpha-2, lowercase, hashed)
    if (userData.country) {
        user_data.country = [hashData(userData.country.toLowerCase())];
    }
    
    // City (lowercase, no spaces, no punctuation, hashed)
    if (userData.city) {
        user_data.ct = [hashData(userData.city.toLowerCase().replace(/[^a-z]/g, ''))];
    }
    
    // State/Province (lowercase, no spaces, no punctuation, hashed)
    if (userData.state) {
        user_data.st = [hashData(userData.state.toLowerCase().replace(/[^a-z]/g, ''))];
    }
    
    // External ID for cross-device tracking (hashed)
    if (userData.externalId) {
        user_data.external_id = [hashData(userData.externalId)];
    }
    
    // Build event payload
    const eventPayload = {
        event_name: eventName,
        event_time: timestamp,
        event_id: finalEventId,
        action_source: 'website',
        user_data: user_data
    };
    
    // Always include event_source_url (required for best match quality)
    // Default to English funnel domain (must match domain where pixel fires for attribution)
    eventPayload.event_source_url = eventSourceUrl || 'https://ingles.zappdetect.com/';
    
    // Add referrer URL if available (helps with attribution)
    if (userData.referrer) {
        eventPayload.referrer_url = userData.referrer;
    }
    
    if (Object.keys(customData).length > 0) {
        eventPayload.custom_data = customData;
    }
    
    // Get pixels for the correct language (or use custom pixels from frontend)
    const pixels = getPixelsForLanguage(options.language, options.pixelIds, options.accessToken);
    
    // Test event codes for Facebook Events Manager testing
    // EN: TEST23104, ES: TEST96875
    const testEventCodes = {
        'en': process.env.FB_TEST_CODE_EN || null,  // Set to 'TEST23104' to enable testing
        'es': process.env.FB_TEST_CODE_ES || null   // Set to 'TEST96875' to enable testing
    };
    
    // Send to all pixels (with retry for transient failures)
    const results = [];
    const maxRetries = 2;
    const retryDelayMs = 500;

    for (const pixel of pixels) {
        const url = `https://graph.facebook.com/${FB_API_VERSION}/${pixel.id}/events?access_token=${pixel.token}`;
        const requestBody = {
            data: [eventPayload]
        };
        const testCode = options.testEventCode || testEventCodes[options.language];
        if (testCode) {
            requestBody.test_event_code = testCode;
            console.log(`🧪 TEST MODE: Using test_event_code ${testCode} for ${pixel.name}`);
        }

        let lastError = null;
        let lastResult = null;
        let success = false;

        for (let attempt = 0; attempt <= maxRetries && !success; attempt++) {
            try {
                if (attempt > 0) {
                    await new Promise(r => setTimeout(r, retryDelayMs));
                    console.log(`🔄 CAPI [${pixel.name}] ${eventName}: retry ${attempt}/${maxRetries}`);
                }
                const response = await fetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(requestBody)
                });
                lastResult = await response.json();

                if (response.ok) {
                    console.log(`✅ CAPI [${pixel.name}] ${eventName}: success (id: ${finalEventId}, events_received: ${lastResult.events_received || 1})`);
                    results.push({ pixel: pixel.id, success: true, result: lastResult, eventId: finalEventId });
                    success = true;
                } else {
                    lastError = lastResult;
                    const isRetryable = response.status >= 500 || response.status === 429;
                    if (!isRetryable || attempt === maxRetries) {
                        console.error(`❌ CAPI [${pixel.name}] ${eventName}: error`, lastResult);
                        results.push({ pixel: pixel.id, success: false, error: lastResult });
                        break;
                    }
                }
            } catch (error) {
                lastError = error.message;
                if (attempt === maxRetries) {
                    console.error(`❌ CAPI [${pixel.name}] ${eventName}: exception`, error.message);
                    results.push({ pixel: pixel.id, success: false, error: error.message });
                }
            }
        }
        if (!success && results.filter(r => r.pixel === pixel.id).length === 0) {
            results.push({ pixel: pixel.id, success: false, error: lastError || lastResult || 'Max retries exceeded' });
        }
    }

    return results;
}

// Database connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Middleware
app.use(helmet({
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false
}));

// Gzip compression for all responses
app.use(compression({ threshold: 512 }));

// Origens permitidas para CORS (funis + admin). FRONTEND_URL sobrescreve se definido.
const ALLOWED_ORIGINS = [
    'https://ingles.zappdetect.com',
    'https://espanhol.zappdetect.com',
    'https://perfect.zappdetect.com',
    'https://afiliado.whatstalker.com',
    'https://www.afiliado.whatstalker.com',
    'http://localhost:3000',
    'http://localhost:5500',
    'http://127.0.0.1:3000',
    'http://127.0.0.1:5500'
];

// Auto-detectar URL pública do Railway para permitir admin panel
if (process.env.RAILWAY_PUBLIC_DOMAIN) {
    ALLOWED_ORIGINS.push(`https://${process.env.RAILWAY_PUBLIC_DOMAIN}`);
}
if (process.env.RAILWAY_STATIC_URL) {
    ALLOWED_ORIGINS.push(process.env.RAILWAY_STATIC_URL.startsWith('http') 
        ? process.env.RAILWAY_STATIC_URL 
        : `https://${process.env.RAILWAY_STATIC_URL}`);
}

// Cache para o domínio do próprio servidor (detectado dinamicamente via Host header)
const selfOrigins = new Set();

// Auto-detectar host do servidor ANTES do CORS (para admin panel funcionar em qualquer deploy)
app.use((req, res, next) => {
    const host = req.headers.host;
    if (host) {
        const httpsOrigin = `https://${host}`;
        const httpOrigin = `http://${host}`;
        if (!selfOrigins.has(httpsOrigin)) {
            selfOrigins.add(httpsOrigin);
            selfOrigins.add(httpOrigin);
            console.log(`🔒 Auto-detected server origin: ${httpsOrigin}`);
        }
    }
    next();
});

app.use(cors({
    origin: function(origin, callback) {
        if (!origin) return callback(null, true); // same-origin ou Postman

        // Allow same-origin requests (admin panel served from same server)
        if (selfOrigins.has(origin)) return callback(null, true);

        // Build allowed list
        let allowed = process.env.FRONTEND_URL
            ? process.env.FRONTEND_URL.split(',').map(s => s.trim())
            : [...ALLOWED_ORIGINS];

        // Always include Railway domain even when FRONTEND_URL overrides
        if (process.env.RAILWAY_PUBLIC_DOMAIN) {
            allowed.push(`https://${process.env.RAILWAY_PUBLIC_DOMAIN}`);
        }
        if (process.env.RAILWAY_STATIC_URL) {
            allowed.push(process.env.RAILWAY_STATIC_URL.startsWith('http')
                ? process.env.RAILWAY_STATIC_URL
                : `https://${process.env.RAILWAY_STATIC_URL}`);
        }

        if (allowed.includes('*') || allowed.includes(origin)) return callback(null, true);
        console.error(`CORS blocked origin: ${origin} (allowed: ${allowed.join(', ')})`);
        callback(new Error('CORS not allowed'), false);
    },
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    credentials: true
}));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// No-cache for admin panel files (JS/CSS), cache ok for funnel assets
app.use((req, res, next) => {
    if (req.path.includes('admin') || req.path.startsWith('/js/admin') || req.path.startsWith('/css/admin')) {
        res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
        res.set('Pragma', 'no-cache');
    }
    next();
});
app.use(express.static('public'));

// Serve funnel static files
// These are placed in public/ingles and public/espanhol after build
const path = require('path');

// ==================== GEOLOCATION HELPER ====================
const http = require('http');

async function getCountryFromIP(ip) {
    return new Promise((resolve) => {
        try {
            // Skip for localhost/private IPs
            if (!ip || ip === '::1' || ip.startsWith('127.') || ip.startsWith('192.168.') || ip.startsWith('10.')) {
                console.log('Geolocation: Skipping private/local IP:', ip);
                return resolve({ country: null, country_code: null, city: null });
            }
            
            // Clean IP (remove IPv6 prefix if present)
            let cleanIP = ip;
            if (ip.startsWith('::ffff:')) {
                cleanIP = ip.substring(7);
            }
            
            console.log('Geolocation: Looking up IP:', cleanIP);
            
            // Using ip-api.com (free, no key required, 45 requests/minute)
            const url = `http://ip-api.com/json/${cleanIP}?fields=status,country,countryCode,city,regionName`;
            
            http.get(url, (res) => {
                let data = '';
                
                res.on('data', (chunk) => {
                    data += chunk;
                });
                
                res.on('end', () => {
                    try {
                        const json = JSON.parse(data);
                        
                        if (json.status === 'success') {
                            console.log('Geolocation: Found -', json.country, json.countryCode, json.city, json.regionName);
                            resolve({
                                country: json.country || null,
                                country_code: json.countryCode || null,
                                city: json.city || null,
                                state: json.regionName || null
                            });
                        } else {
                            console.log('Geolocation: API returned fail status');
                            resolve({ country: null, country_code: null, city: null, state: null });
                        }
                    } catch (parseError) {
                        console.log('Geolocation parse error:', parseError.message);
                        resolve({ country: null, country_code: null, city: null, state: null });
                    }
                });
            }).on('error', (error) => {
                console.log('Geolocation request error:', error.message);
                resolve({ country: null, country_code: null, city: null, state: null });
            });
            
        } catch (error) {
            console.log('Geolocation error:', error.message);
            resolve({ country: null, country_code: null, city: null, state: null });
        }
    });
}

app.use('/ingles', express.static(path.join(__dirname, 'public', 'ingles')));
app.use('/espanhol', express.static(path.join(__dirname, 'public', 'espanhol')));
app.use('/perfectpay', express.static(path.join(__dirname, 'public', 'perfectpay')));
app.use('/en', express.static(path.join(__dirname, 'public', 'ingles')));
app.use('/es', express.static(path.join(__dirname, 'public', 'espanhol')));

// Serve funnel files at root path based on domain
// ingles.zappdetect.com/upsell/up1/ → public/ingles/upsell/up1/
// espanhol.zappdetect.com/upsell/up1/ → public/espanhol/upsell/up1/
// perfect.zappdetect.com/ingles/ → public/perfectpay/ (PerfectPay funnel)
// This is needed because checkout platforms redirect to /upsell/up1/ (without prefix)
app.use((req, res, next) => {
    const host = req.hostname || req.headers.host || '';
    if (host.includes('perfect') || host.includes('perfect.zappdetect')) {
        // perfect.zappdetect.com/ingles/... → serve from perfectpay folder
        // Also serve root paths (upsell redirects) from perfectpay
        if (req.path.startsWith('/ingles')) {
            // Rewrite /ingles/... to serve from /perfectpay/...
            const newPath = req.path.replace('/ingles', '');
            req.url = newPath || '/';
        }
        express.static(path.join(__dirname, 'public', 'perfectpay'))(req, res, next);
    } else if (host.includes('ingles') || host.includes('ingles.zappdetect')) {
        express.static(path.join(__dirname, 'public', 'ingles'))(req, res, next);
    } else if (host.includes('espanhol') || host.includes('espanhol.zappdetect')) {
        express.static(path.join(__dirname, 'public', 'espanhol'))(req, res, next);
    } else {
        next();
    }
});

// Rate limiting
const apiLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100, // limit each IP to 100 requests per windowMs
    message: { error: 'Too many requests, please try again later.' }
});

const leadLimiter = rateLimit({
    windowMs: 60 * 1000, // 1 minute
    max: 10, // limit each IP to 10 leads per minute
    message: { error: 'Too many submissions, please try again later.' }
});

// Admin rate limiter (very generous - admin panel auto-refreshes frequently)
// Dashboard makes ~14 API calls per refresh cycle (every 15s) + active users + badges
// That's ~70 calls/min = ~1050/15min, so limit must be well above that
const adminLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 5000, // 5000 req/15min - admin panel has many concurrent API calls + auto-refresh
    message: { error: 'Too many admin requests, please slow down.' },
    standardHeaders: true,
    legacyHeaders: false
});

// Bulk operations limiter
const bulkLimiter = rateLimit({
    windowMs: 60 * 60 * 1000, // 1 hour
    max: 10,
    message: { error: 'Too many bulk operations, try again later.' }
});

// ==================== IN-MEMORY CACHE ====================
const apiCache = new Map();

function getCached(key, ttlMs) {
    const entry = apiCache.get(key);
    if (entry && (Date.now() - entry.time) < ttlMs) return entry.data;
    return null;
}

function setCache(key, data) {
    apiCache.set(key, { data, time: Date.now() });
    // Cleanup old entries periodically (max 200 entries)
    if (apiCache.size > 200) {
        const oldest = [...apiCache.entries()].sort((a, b) => a[1].time - b[1].time);
        for (let i = 0; i < 50; i++) apiCache.delete(oldest[i][0]);
    }
}

function invalidateCache(prefix) {
    for (const key of apiCache.keys()) {
        if (key.startsWith(prefix)) apiCache.delete(key);
    }
}

// Apply admin rate limiter to all admin routes
app.use('/api/admin', adminLimiter);

// JWT Authentication middleware
const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    
    if (!token) {
        return res.status(401).json({ error: 'Access token required' });
    }
    
    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) {
            // Return 401 so frontend can detect and auto-logout
            return res.status(401).json({ error: 'Invalid or expired token' });
        }
        req.user = user;
        next();
    });
};

// Helper function to build date filter SQL
// Uses Brazil timezone for correct date filtering
function buildDateFilter(startDate, endDate, columnName = 'created_at') {
    if (!startDate || !endDate) return { sql: '', params: [] };
    return {
        sql: ` AND (${columnName} AT TIME ZONE 'America/Sao_Paulo')::date >= $PARAM_START::date AND (${columnName} AT TIME ZONE 'America/Sao_Paulo')::date <= $PARAM_END::date`,
        params: [startDate, endDate]
    };
}

// Helper function to parse Monetizze dates
// Handles both Brazilian format (DD/MM/YYYY HH:MM:SS) and ISO format (YYYY-MM-DD HH:MM:SS)
// IMPORTANT: Monetizze sends dates in Brazil timezone (UTC-3), so we must interpret them correctly
function parseMonetizzeDate(dateStr) {
    if (!dateStr) return null;
    try {
        // Check if it's in DD/MM/YYYY format (Brazilian postback format)
        const brDateMatch = dateStr.match(/^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2}):(\d{2})$/);
        if (brDateMatch) {
            const [, day, month, year, hour, minute, second] = brDateMatch;
            // Create ISO string with Brazil timezone offset (-03:00)
            // This ensures the date is interpreted as Brazil time, not server local time
            const isoString = `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T${hour}:${minute}:${second}-03:00`;
            const date = new Date(isoString);
            return isNaN(date.getTime()) ? null : date;
        }
        // Check if it's already in ISO format but without timezone (assume Brazil time)
        const isoNoTzMatch = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})$/);
        if (isoNoTzMatch) {
            const [, year, month, day, hour, minute, second] = isoNoTzMatch;
            const isoString = `${year}-${month}-${day}T${hour}:${minute}:${second}-03:00`;
            const date = new Date(isoString);
            return isNaN(date.getTime()) ? null : date;
        }
        // Try standard parsing (ISO format with timezone from API 2.1)
        const date = new Date(dateStr);
        return isNaN(date.getTime()) ? null : date;
    } catch (e) {
        return null;
    }
}

// ==================== PUBLIC API ROUTES ====================

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ==================== A/B TESTING API ====================

// Public endpoint: Get variant for a visitor
app.get('/api/ab/variant', async (req, res) => {
    try {
        const { funnel, visitor_id } = req.query;
        
        if (!funnel || !visitor_id) {
            return res.json({ variant: null, test_id: null, config: null });
        }
        
        // Check if visitor already has a variant assigned
        const existing = await pool.query(`
            SELECT atv.variant, atv.test_id, at.variant_a_param, at.variant_b_param,
                   at.test_type, at.config_a, at.config_b
            FROM ab_test_visitors atv
            JOIN ab_tests at ON at.id = atv.test_id
            WHERE atv.visitor_id = $1 AND at.funnel = $2 AND at.status = 'running'
            LIMIT 1
        `, [visitor_id, funnel]);
        
        if (existing.rows.length > 0) {
            const row = existing.rows[0];
            const param = row.variant === 'A' ? row.variant_a_param : row.variant_b_param;
            const config = row.variant === 'A' ? row.config_a : row.config_b;
            return res.json({ 
                variant: row.variant, 
                test_id: row.test_id,
                param: param,
                test_type: row.test_type,
                config: config || {}
            });
        }
        
        // Get active test for this funnel
        const test = await pool.query(`
            SELECT id, traffic_split, variant_a_param, variant_b_param,
                   test_type, config_a, config_b
            FROM ab_tests 
            WHERE funnel = $1 AND status = 'running'
            LIMIT 1
        `, [funnel]);
        
        if (test.rows.length === 0) {
            return res.json({ variant: null, test_id: null, config: null });
        }
        
        const activeTest = test.rows[0];
        
        // Assign variant based on traffic split
        const random = Math.random() * 100;
        const variant = random < activeTest.traffic_split ? 'A' : 'B';
        const param = variant === 'A' ? activeTest.variant_a_param : activeTest.variant_b_param;
        const config = variant === 'A' ? activeTest.config_a : activeTest.config_b;
        
        // Save assignment
        await pool.query(`
            INSERT INTO ab_test_visitors (test_id, visitor_id, variant, ip_address, user_agent)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (test_id, visitor_id) DO NOTHING
        `, [activeTest.id, visitor_id, variant, req.ip, req.headers['user-agent']]);
        
        res.json({ 
            variant, 
            test_id: activeTest.id,
            param: param,
            test_type: activeTest.test_type,
            config: config || {}
        });
        
    } catch (error) {
        console.error('A/B variant error:', error);
        res.json({ variant: null, test_id: null });
    }
});

// Public endpoint: Track conversion
app.post('/api/ab/convert', async (req, res) => {
    try {
        const { test_id, visitor_id, event_type, value, metadata } = req.body;
        
        if (!test_id || !visitor_id || !event_type) {
            return res.status(400).json({ error: 'Missing required fields' });
        }
        
        // Get visitor's variant
        const visitor = await pool.query(`
            SELECT variant FROM ab_test_visitors
            WHERE test_id = $1 AND visitor_id = $2
        `, [test_id, visitor_id]);
        
        if (visitor.rows.length === 0) {
            return res.status(404).json({ error: 'Visitor not found in test' });
        }
        
        const variant = visitor.rows[0].variant;
        
        // Record conversion
        await pool.query(`
            INSERT INTO ab_test_conversions (test_id, visitor_id, variant, event_type, value, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
        `, [test_id, visitor_id, variant, event_type, value || 0, metadata || {}]);
        
        res.json({ success: true, variant });
        
    } catch (error) {
        console.error('A/B conversion error:', error);
        res.status(500).json({ error: 'Failed to record conversion' });
    }
});

// Admin: List all A/B tests
app.get('/api/admin/ab-tests', authenticateToken, async (req, res) => {
    try {
        const tests = await pool.query(`
            SELECT 
                at.*,
                (SELECT COUNT(*) FROM ab_test_visitors WHERE test_id = at.id AND variant = 'A') as visitors_a,
                (SELECT COUNT(*) FROM ab_test_visitors WHERE test_id = at.id AND variant = 'B') as visitors_b,
                (SELECT COUNT(*) FROM ab_test_conversions WHERE test_id = at.id AND variant = 'A' AND event_type = 'lead') as leads_a,
                (SELECT COUNT(*) FROM ab_test_conversions WHERE test_id = at.id AND variant = 'B' AND event_type = 'lead') as leads_b,
                (SELECT COUNT(*) FROM ab_test_conversions WHERE test_id = at.id AND variant = 'A' AND event_type = 'purchase') as purchases_a,
                (SELECT COUNT(*) FROM ab_test_conversions WHERE test_id = at.id AND variant = 'B' AND event_type = 'purchase') as purchases_b,
                (SELECT COALESCE(SUM(value), 0) FROM ab_test_conversions WHERE test_id = at.id AND variant = 'A' AND event_type = 'purchase') as revenue_a,
                (SELECT COALESCE(SUM(value), 0) FROM ab_test_conversions WHERE test_id = at.id AND variant = 'B' AND event_type = 'purchase') as revenue_b
            FROM ab_tests at
            ORDER BY at.created_at DESC
        `);
        
        // Calculate conversion rates
        const testsWithStats = tests.rows.map(test => {
            const visitorsA = parseInt(test.visitors_a) || 0;
            const visitorsB = parseInt(test.visitors_b) || 0;
            const leadsA = parseInt(test.leads_a) || 0;
            const leadsB = parseInt(test.leads_b) || 0;
            const purchasesA = parseInt(test.purchases_a) || 0;
            const purchasesB = parseInt(test.purchases_b) || 0;
            
            return {
                ...test,
                conversion_rate_a: visitorsA > 0 ? ((leadsA / visitorsA) * 100).toFixed(2) : 0,
                conversion_rate_b: visitorsB > 0 ? ((leadsB / visitorsB) * 100).toFixed(2) : 0,
                purchase_rate_a: visitorsA > 0 ? ((purchasesA / visitorsA) * 100).toFixed(2) : 0,
                purchase_rate_b: visitorsB > 0 ? ((purchasesB / visitorsB) * 100).toFixed(2) : 0
            };
        });
        
        res.json(testsWithStats);
        
    } catch (error) {
        console.error('List A/B tests error:', error);
        res.status(500).json({ error: 'Failed to fetch A/B tests' });
    }
});

// Admin: Get single A/B test with detailed stats
app.get('/api/admin/ab-tests/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        const test = await pool.query(`SELECT * FROM ab_tests WHERE id = $1`, [id]);
        
        if (test.rows.length === 0) {
            return res.status(404).json({ error: 'Test not found' });
        }
        
        // Get daily stats
        const dailyStats = await pool.query(`
            SELECT 
                DATE(created_at) as date,
                variant,
                COUNT(*) as visitors
            FROM ab_test_visitors
            WHERE test_id = $1
            GROUP BY DATE(created_at), variant
            ORDER BY date
        `, [id]);
        
        const dailyConversions = await pool.query(`
            SELECT 
                DATE(created_at) as date,
                variant,
                event_type,
                COUNT(*) as count,
                COALESCE(SUM(value), 0) as value
            FROM ab_test_conversions
            WHERE test_id = $1
            GROUP BY DATE(created_at), variant, event_type
            ORDER BY date
        `, [id]);
        
        // Get totals
        const totals = await pool.query(`
            SELECT 
                variant,
                COUNT(*) as visitors
            FROM ab_test_visitors
            WHERE test_id = $1
            GROUP BY variant
        `, [id]);
        
        const conversionTotals = await pool.query(`
            SELECT 
                variant,
                event_type,
                COUNT(*) as count,
                COALESCE(SUM(value), 0) as value
            FROM ab_test_conversions
            WHERE test_id = $1
            GROUP BY variant, event_type
        `, [id]);
        
        res.json({
            test: test.rows[0],
            daily_visitors: dailyStats.rows,
            daily_conversions: dailyConversions.rows,
            totals: totals.rows,
            conversion_totals: conversionTotals.rows
        });
        
    } catch (error) {
        console.error('Get A/B test error:', error);
        res.status(500).json({ error: 'Failed to fetch test details' });
    }
});

// Admin: Create A/B test
app.post('/api/admin/ab-tests', authenticateToken, async (req, res) => {
    try {
        const { 
            name, 
            description, 
            funnel, 
            variant_a_name, 
            variant_a_param,
            variant_b_name, 
            variant_b_param,
            traffic_split,
            test_type,
            config_a,
            config_b
        } = req.body;
        
        if (!name || !funnel) {
            return res.status(400).json({ error: 'Name and funnel are required' });
        }
        
        const result = await pool.query(`
            INSERT INTO ab_tests (
                name, description, funnel, 
                variant_a_name, variant_a_param,
                variant_b_name, variant_b_param,
                traffic_split, status, created_by,
                test_type, config_a, config_b
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'draft', $9, $10, $11, $12)
            RETURNING *
        `, [
            name, 
            description || '', 
            funnel,
            variant_a_name || 'Controle',
            variant_a_param || 'control',
            variant_b_name || 'Teste',
            variant_b_param || 'test',
            traffic_split || 50,
            req.user.id,
            test_type || 'vsl',
            JSON.stringify(config_a || {}),
            JSON.stringify(config_b || {})
        ]);
        
        res.json(result.rows[0]);
        
    } catch (error) {
        console.error('Create A/B test error:', error);
        res.status(500).json({ error: 'Failed to create A/B test' });
    }
});

// Admin: Update A/B test
app.put('/api/admin/ab-tests/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { 
            name, 
            description, 
            variant_a_name, 
            variant_a_param,
            variant_b_name, 
            variant_b_param,
            traffic_split,
            test_type,
            config_a,
            config_b
        } = req.body;
        
        const result = await pool.query(`
            UPDATE ab_tests SET
                name = COALESCE($1, name),
                description = COALESCE($2, description),
                variant_a_name = COALESCE($3, variant_a_name),
                variant_a_param = COALESCE($4, variant_a_param),
                variant_b_name = COALESCE($5, variant_b_name),
                variant_b_param = COALESCE($6, variant_b_param),
                traffic_split = COALESCE($7, traffic_split),
                test_type = COALESCE($8, test_type),
                config_a = COALESCE($9, config_a),
                config_b = COALESCE($10, config_b)
            WHERE id = $11
            RETURNING *
        `, [
            name, 
            description, 
            variant_a_name, 
            variant_a_param, 
            variant_b_name, 
            variant_b_param, 
            traffic_split,
            test_type,
            config_a ? JSON.stringify(config_a) : null,
            config_b ? JSON.stringify(config_b) : null,
            id
        ]);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Test not found' });
        }
        
        res.json(result.rows[0]);
        
    } catch (error) {
        console.error('Update A/B test error:', error);
        res.status(500).json({ error: 'Failed to update A/B test' });
    }
});

// Admin: Start/Stop A/B test
app.post('/api/admin/ab-tests/:id/status', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { action } = req.body; // 'start', 'stop', 'reset'
        
        let query, params;
        
        if (action === 'start') {
            // Check if there's already a running test for this funnel
            const existing = await pool.query(`
                SELECT id, name FROM ab_tests 
                WHERE id != $1 AND status = 'running' 
                AND funnel = (SELECT funnel FROM ab_tests WHERE id = $1)
            `, [id]);
            
            if (existing.rows.length > 0) {
                return res.status(400).json({ 
                    error: `Another test "${existing.rows[0].name}" is already running for this funnel` 
                });
            }
            
            query = `UPDATE ab_tests SET status = 'running', started_at = NOW() WHERE id = $1 RETURNING *`;
            params = [id];
        } else if (action === 'stop') {
            query = `UPDATE ab_tests SET status = 'stopped', ended_at = NOW() WHERE id = $1 RETURNING *`;
            params = [id];
        } else if (action === 'reset') {
            // Delete all visitors and conversions for this test
            await pool.query(`DELETE FROM ab_test_conversions WHERE test_id = $1`, [id]);
            await pool.query(`DELETE FROM ab_test_visitors WHERE test_id = $1`, [id]);
            query = `UPDATE ab_tests SET status = 'draft', started_at = NULL, ended_at = NULL, winner = NULL WHERE id = $1 RETURNING *`;
            params = [id];
        } else {
            return res.status(400).json({ error: 'Invalid action' });
        }
        
        const result = await pool.query(query, params);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Test not found' });
        }
        
        res.json(result.rows[0]);
        
    } catch (error) {
        console.error('Update A/B test status error:', error);
        res.status(500).json({ error: 'Failed to update test status' });
    }
});

// Admin: Set winner and end test
app.post('/api/admin/ab-tests/:id/winner', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { winner } = req.body; // 'A' or 'B'
        
        if (!['A', 'B'].includes(winner)) {
            return res.status(400).json({ error: 'Winner must be A or B' });
        }
        
        const result = await pool.query(`
            UPDATE ab_tests SET 
                status = 'completed',
                winner = $1,
                ended_at = NOW()
            WHERE id = $2
            RETURNING *
        `, [winner, id]);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Test not found' });
        }
        
        res.json(result.rows[0]);
        
    } catch (error) {
        console.error('Set A/B winner error:', error);
        res.status(500).json({ error: 'Failed to set winner' });
    }
});

// Admin: Delete A/B test
app.delete('/api/admin/ab-tests/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        await pool.query(`DELETE FROM ab_tests WHERE id = $1`, [id]);
        
        res.json({ success: true });
        
    } catch (error) {
        console.error('Delete A/B test error:', error);
        res.status(500).json({ error: 'Failed to delete test' });
    }
});

// Public endpoint to check recent CAPI activity (limited info for debugging)
app.get('/api/capi/status', async (req, res) => {
    try {
        // Get recent transaction counts
        const last24h = await pool.query(`
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) as refunded
            FROM transactions 
            WHERE created_at > NOW() - INTERVAL '24 hours'
        `);
        
        // Get recent postback logs count
        const postbackLogs = await pool.query(`
            SELECT COUNT(*) as count 
            FROM postback_logs 
            WHERE created_at > NOW() - INTERVAL '24 hours'
        `);
        
        // Get last 5 transactions (limited info)
        const recentTx = await pool.query(`
            SELECT 
                status, 
                funnel_language,
                created_at,
                CASE WHEN CAST(value AS NUMERIC) > 0 THEN 'has_value' ELSE 'no_value' END as value_status
            FROM transactions 
            ORDER BY created_at DESC 
            LIMIT 5
        `);
        
        res.json({
            status: 'ok',
            timestamp: new Date().toISOString(),
            last24h: {
                totalTransactions: parseInt(last24h.rows[0]?.total || 0),
                approved: parseInt(last24h.rows[0]?.approved || 0),
                pending: parseInt(last24h.rows[0]?.pending || 0),
                refunded: parseInt(last24h.rows[0]?.refunded || 0)
            },
            postbacksReceived24h: parseInt(postbackLogs.rows[0]?.count || 0),
            recentTransactions: recentTx.rows,
            capiEndpoint: '/api/capi/event',
            postbackEndpoint: '/api/postback/monetizze',
            perfectpayWebhookEndpoint: '/api/postback/perfectpay'
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Real-time active users count (protected - for admin dashboard)
app.get('/api/admin/active-users', authenticateToken, async (req, res) => {
    try {
        const minutes = parseInt(req.query.minutes) || 5; // Default: last 5 minutes
        const cappedMinutes = Math.min(Math.max(minutes, 1), 60); // Clamp 1-60

        // Count distinct visitor_ids with events in the last N minutes from funnel_events
        const result = await pool.query(`
            SELECT 
                COUNT(DISTINCT visitor_id) AS total_active,
                COUNT(DISTINCT CASE WHEN COALESCE(metadata->>'funnelLanguage', 'en') = 'en' THEN visitor_id END) AS active_en,
                COUNT(DISTINCT CASE WHEN metadata->>'funnelLanguage' = 'es' THEN visitor_id END) AS active_es,
                COUNT(*) AS total_events
            FROM funnel_events
            WHERE created_at >= NOW() - make_interval(mins => $1)
        `, [cappedMinutes]);

        // Also get page breakdown
        const pageBreakdown = await pool.query(`
            SELECT 
                page,
                COUNT(DISTINCT visitor_id) AS visitors
            FROM funnel_events
            WHERE created_at >= NOW() - make_interval(mins => $1)
                AND page IS NOT NULL
            GROUP BY page
            ORDER BY visitors DESC
            LIMIT 10
        `, [cappedMinutes]);

        const row = result.rows[0] || {};
        res.json({
            active_users: parseInt(row.total_active) || 0,
            active_en: parseInt(row.active_en) || 0,
            active_es: parseInt(row.active_es) || 0,
            total_events: parseInt(row.total_events) || 0,
            interval_minutes: cappedMinutes,
            pages: pageBreakdown.rows.map(r => ({ page: r.page, visitors: parseInt(r.visitors) })),
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error fetching active users:', error);
        res.status(500).json({ error: 'Failed to fetch active users' });
    }
});

// Diagnostic endpoint: check funnel_events table health (protected)
app.get('/api/admin/debug/funnel-events', authenticateToken, async (req, res) => {
    try {
        // Check table exists and count recent events
        const tableCheck = await pool.query(`
            SELECT 
                COUNT(*) AS total_rows,
                COUNT(CASE WHEN created_at >= NOW() - INTERVAL '5 minutes' THEN 1 END) AS last_5min,
                COUNT(CASE WHEN created_at >= NOW() - INTERVAL '1 hour' THEN 1 END) AS last_1h,
                COUNT(CASE WHEN created_at >= NOW() - INTERVAL '24 hours' THEN 1 END) AS last_24h,
                MIN(created_at) AS oldest_event,
                MAX(created_at) AS newest_event,
                NOW() AS server_time
            FROM funnel_events
        `);
        
        // Check table columns
        const columns = await pool.query(`
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'funnel_events'
            ORDER BY ordinal_position
        `);
        
        // Recent events sample
        const recentEvents = await pool.query(`
            SELECT visitor_id, event, page, ip_address, metadata->>'funnelLanguage' as lang, created_at
            FROM funnel_events
            ORDER BY created_at DESC
            LIMIT 10
        `);
        
        const row = tableCheck.rows[0] || {};
        res.json({
            table_exists: true,
            columns: columns.rows.map(c => c.column_name),
            total_rows: parseInt(row.total_rows) || 0,
            last_5min: parseInt(row.last_5min) || 0,
            last_1h: parseInt(row.last_1h) || 0,
            last_24h: parseInt(row.last_24h) || 0,
            oldest_event: row.oldest_event,
            newest_event: row.newest_event,
            server_time: row.server_time,
            recent_events: recentEvents.rows
        });
    } catch (error) {
        console.error('Debug funnel-events error:', error);
        res.json({ 
            table_exists: false, 
            error: error.message,
            hint: 'funnel_events table may not exist or has schema issues'
        });
    }
});

// Trends comparison endpoint - current vs previous period (protected)
app.get('/api/admin/stats/trends', authenticateToken, async (req, res) => {
    try {
        const days = parseInt(req.query.days) || 7;
        const cappedDays = Math.min(Math.max(days, 1), 90);

        // Check cache (TTL: 3 min)
        const cacheKey = `trends-${cappedDays}`;
        const cached = getCached(cacheKey, 3 * 60 * 1000);
        if (cached) return res.json(cached);

        // Run all queries in parallel with Promise.allSettled to avoid unhandled rejections
        const [
            leadsCurrent, leadsPrevious,
            salesCurrent, salesPrevious,
            leadsSparkline, salesSparkline,
            checkoutCurrent, checkoutPrevious
        ] = await Promise.allSettled([
            pool.query(
                `SELECT COUNT(*) as count FROM leads WHERE created_at >= NOW() - make_interval(days => $1)`,
                [cappedDays]
            ),
            pool.query(
                `SELECT COUNT(*) as count FROM leads 
                 WHERE created_at >= NOW() - make_interval(days => $1) 
                   AND created_at < NOW() - make_interval(days => $2)`,
                [cappedDays * 2, cappedDays]
            ),
            pool.query(
                `SELECT COUNT(*) as count, COALESCE(SUM(CASE WHEN CAST(value AS numeric) > 0 THEN CAST(value AS numeric) ELSE 0 END), 0) as revenue 
                 FROM transactions WHERE status = 'approved' AND created_at >= NOW() - make_interval(days => $1)`,
                [cappedDays]
            ),
            pool.query(
                `SELECT COUNT(*) as count, COALESCE(SUM(CASE WHEN CAST(value AS numeric) > 0 THEN CAST(value AS numeric) ELSE 0 END), 0) as revenue 
                 FROM transactions WHERE status = 'approved' 
                   AND created_at >= NOW() - make_interval(days => $1) 
                   AND created_at < NOW() - make_interval(days => $2)`,
                [cappedDays * 2, cappedDays]
            ),
            pool.query(
                `SELECT DATE(created_at AT TIME ZONE 'America/Sao_Paulo') as day, COUNT(*) as count 
                 FROM leads WHERE created_at >= NOW() - make_interval(days => $1) 
                 GROUP BY day ORDER BY day ASC`,
                [cappedDays]
            ),
            pool.query(
                `SELECT DATE(created_at AT TIME ZONE 'America/Sao_Paulo') as day, COUNT(*) as count,
                        COALESCE(SUM(CASE WHEN CAST(value AS numeric) > 0 THEN CAST(value AS numeric) ELSE 0 END), 0) as revenue
                 FROM transactions WHERE status = 'approved' AND created_at >= NOW() - make_interval(days => $1) 
                 GROUP BY day ORDER BY day ASC`,
                [cappedDays]
            ),
            pool.query(
                `SELECT COUNT(DISTINCT visitor_id) as count FROM funnel_events 
                 WHERE event LIKE 'page_view_cta%' AND created_at >= NOW() - make_interval(days => $1)`,
                [cappedDays]
            ),
            pool.query(
                `SELECT COUNT(DISTINCT visitor_id) as count FROM funnel_events 
                 WHERE event LIKE 'page_view_cta%' 
                   AND created_at >= NOW() - make_interval(days => $1) 
                   AND created_at < NOW() - make_interval(days => $2)`,
                [cappedDays * 2, cappedDays]
            )
        ]);

        // Helper to safely extract result
        const getRows = (result) => result.status === 'fulfilled' ? result.value.rows : [];
        const getFirst = (result) => (getRows(result)[0]) || {};

        const calcChange = (current, previous) => {
            if (previous === 0 && current === 0) return 0;
            if (previous === 0) return 100;
            return ((current - previous) / previous * 100);
        };

        const lc = parseInt(getFirst(leadsCurrent).count) || 0;
        const lp = parseInt(getFirst(leadsPrevious).count) || 0;
        const sc = parseInt(getFirst(salesCurrent).count) || 0;
        const sp = parseInt(getFirst(salesPrevious).count) || 0;
        const rc = parseFloat(getFirst(salesCurrent).revenue) || 0;
        const rp = parseFloat(getFirst(salesPrevious).revenue) || 0;
        const cc = parseInt(getFirst(checkoutCurrent).count) || 0;
        const cp = parseInt(getFirst(checkoutPrevious).count) || 0;

        const response = {
            period_days: cappedDays,
            leads: { current: lc, previous: lp, change: calcChange(lc, lp).toFixed(1) },
            sales: { current: sc, previous: sp, change: calcChange(sc, sp).toFixed(1) },
            revenue: { current: rc, previous: rp, change: calcChange(rc, rp).toFixed(1) },
            checkout: { current: cc, previous: cp, change: calcChange(cc, cp).toFixed(1) },
            sparklines: {
                leads: getRows(leadsSparkline).map(r => ({ day: r.day, count: parseInt(r.count) })),
                sales: getRows(salesSparkline).map(r => ({ day: r.day, count: parseInt(r.count), revenue: parseFloat(r.revenue) }))
            },
            timestamp: new Date().toISOString()
        };
        setCache(cacheKey, response);
        res.json(response);
    } catch (error) {
        console.error('Error fetching trends:', error);
        res.status(500).json({ error: 'Failed to fetch trends' });
    }
});

// Purchase CAPI attribution logs (protected - for admin dashboard)
app.get('/api/admin/capi-purchase-logs', authenticateToken, async (req, res) => {
    try {
        const { language, limit = 50, period } = req.query;
        
        // Build WHERE conditions for both queries (logs + summary)
        const conditions = [];
        const params = [];
        let paramIdx = 1;
        
        if (language === 'en' || language === 'es') {
            conditions.push(`funnel_language = $${paramIdx}`);
            params.push(language);
            paramIdx++;
        }
        
        // Period filter (matches the pixelDateRange dropdown)
        if (period) {
            const periodMap = {
                '30min': '30 minutes', '1h': '1 hour', '2h': '2 hours', '6h': '6 hours',
                '12h': '12 hours', '24h': '24 hours', '48h': '48 hours', '7d': '7 days',
                '14d': '14 days', '30d': '30 days'
            };
            const interval = periodMap[period];
            if (interval) {
                conditions.push(`created_at >= NOW() - INTERVAL '${interval}'`);
            }
        }
        
        const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
        
        // Logs query
        const logsQuery = `SELECT * FROM capi_purchase_logs ${whereClause} ORDER BY created_at DESC LIMIT $${paramIdx}`;
        params.push(parseInt(limit) || 50);
        const result = await pool.query(logsQuery, params);
        
        // Summary query (same filters, without LIMIT)
        const summaryParams = params.slice(0, -1); // Remove the LIMIT param
        const summary = await pool.query(`
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE capi_success = true) as success,
                COUNT(*) FILTER (WHERE capi_success = false) as failed,
                COUNT(*) FILTER (WHERE has_fbc = true) as with_fbc,
                COUNT(*) FILTER (WHERE has_fbp = true) as with_fbp,
                COUNT(*) FILTER (WHERE has_ip = true) as with_ip,
                COUNT(*) FILTER (WHERE has_user_agent = true) as with_ua,
                COUNT(*) FILTER (WHERE lead_found = true) as with_lead,
                COUNT(*) FILTER (WHERE has_email = true) as with_email,
                COUNT(*) FILTER (WHERE funnel_language = 'en') as en_count,
                COUNT(*) FILTER (WHERE funnel_language = 'es') as es_count,
                COUNT(*) FILTER (WHERE funnel_source = 'affiliate') as affiliate_count,
                COUNT(*) FILTER (WHERE funnel_source = 'main') as main_count,
                COALESCE(SUM(value), 0) as total_value
            FROM capi_purchase_logs ${whereClause}
        `, summaryParams);
        
        res.json({
            logs: result.rows,
            summary: summary.rows[0] || {}
        });
    } catch (error) {
        // Table might not exist yet
        if (error.message.includes('does not exist')) {
            return res.json({ logs: [], summary: {} });
        }
        console.error('Error fetching CAPI purchase logs:', error);
        res.status(500).json({ error: 'Failed to fetch CAPI purchase logs' });
    }
});

// Pixel & CAPI aggregated stats (protected - for admin dashboard)
app.get('/api/admin/pixel-stats', authenticateToken, async (req, res) => {
    try {
        let startDate = req.query.startDate;
        let endDate = req.query.endDate;
        const days = parseInt(req.query.days, 10);
        const hours = parseFloat(req.query.hours, 10);
        const minutes = parseInt(req.query.minutes, 10);
        const useInterval = minutes > 0 || (hours > 0 && hours < 24 * 365);

        if (minutes > 0) {
            startDate = null;
            endDate = null;
        } else if (hours > 0) {
            startDate = null;
            endDate = null;
        } else if (days > 0) {
            endDate = new Date().toISOString().slice(0, 10);
            const d = new Date();
            d.setDate(d.getDate() - days);
            startDate = d.toISOString().slice(0, 10);
        }
        if (!useInterval && !startDate && !endDate) {
            return res.status(400).json({ error: 'Use startDate+endDate, days (1,7,30), hours (1,2,6,12), or minutes (30,60)' });
        }
        const language = req.query.language || ''; // '' = all, 'en', 'es'

        let whereClause;
        let params;
        let txWhereClause;
        let txParams;

        if (minutes > 0) {
            whereClause = `created_at >= NOW() - ($1::int || ' minutes')::interval`;
            params = [minutes];
            txWhereClause = `created_at >= NOW() - ($1::int || ' minutes')::interval AND status = 'approved'`;
            txParams = [minutes];
        } else if (hours > 0) {
            whereClause = `created_at >= NOW() - ($1::numeric || ' hours')::interval`;
            params = [hours];
            txWhereClause = `created_at >= NOW() - ($1::numeric || ' hours')::interval AND status = 'approved'`;
            txParams = [hours];
        } else {
            whereClause = `(created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $1::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $2::date`;
            params = [startDate, endDate];
            txWhereClause = `(created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $1::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $2::date AND status = 'approved'`;
            txParams = [startDate, endDate];
        }
        if (language === 'en') {
            whereClause += ` AND (funnel_language = 'en' OR funnel_language IS NULL)`;
        } else if (language === 'es') {
            whereClause += ` AND funnel_language = 'es'`;
        }

        const baseQuery = `SELECT 
            COUNT(*)::int as total,
            COUNT(*) FILTER (WHERE fbp IS NOT NULL AND fbp != '')::int as with_fbp,
            COUNT(*) FILTER (WHERE fbc IS NOT NULL AND fbc != '')::int as with_fbc,
            COUNT(*) FILTER (WHERE email IS NOT NULL AND email != '')::int as with_email,
            COUNT(*) FILTER (WHERE whatsapp IS NOT NULL AND whatsapp != '')::int as with_phone,
            COUNT(*) FILTER (WHERE ip_address IS NOT NULL AND ip_address != '')::int as with_ip,
            COUNT(*) FILTER (WHERE user_agent IS NOT NULL AND user_agent != '')::int as with_user_agent,
            COUNT(*) FILTER (WHERE visitor_id IS NOT NULL AND visitor_id != '')::int as with_visitor_id,
            COUNT(*) FILTER (WHERE country_code IS NOT NULL AND country_code != '')::int as with_country,
            COUNT(*) FILTER (WHERE (fbc IS NOT NULL AND fbc != '') OR (utm_source IS NOT NULL AND LOWER(TRIM(utm_source)) IN ('facebook','fb','meta')))::int as facebook_ads,
            COUNT(*) FILTER (WHERE ((fbc IS NOT NULL AND fbc != '') OR (utm_source IS NOT NULL AND LOWER(TRIM(utm_source)) IN ('facebook','fb','meta'))) AND fbc IS NOT NULL AND fbc != '')::int as facebook_ads_with_fbc,
            COUNT(*) FILTER (WHERE (fbc IS NULL OR fbc = '') AND (utm_source IS NULL OR utm_source = '') AND (referrer IS NULL OR referrer = ''))::int as direct,
            COUNT(*) FILTER (WHERE utm_source IS NOT NULL AND utm_source != '' AND LOWER(TRIM(utm_source)) NOT IN ('facebook','fb','meta'))::int as other,
            COUNT(*) FILTER (WHERE (fbc IS NULL OR fbc = '') AND (utm_source IS NULL OR utm_source = ''))::int as unidentified
        FROM leads WHERE ${whereClause}`;

        const [aggResult, byLangResult, valueResult] = await Promise.all([
            pool.query(baseQuery, params),
            pool.query(`
                SELECT funnel_language as lang,
                    COUNT(*)::int as total,
                    COUNT(*) FILTER (WHERE fbp IS NOT NULL AND fbp != '')::int as with_fbp,
                    COUNT(*) FILTER (WHERE fbc IS NOT NULL AND fbc != '')::int as with_fbc
                FROM leads WHERE ${whereClause}
                GROUP BY funnel_language
            `, params),
            pool.query(`
                SELECT COALESCE(SUM(CAST(value AS NUMERIC)), 0) as total_value
                FROM transactions
                WHERE ${txWhereClause}
            `, txParams)
        ]);

        const row = aggResult.rows[0];
        const byLang = {};
        (byLangResult.rows || []).forEach(r => {
            const lang = r.lang || 'en';
            byLang[lang] = { total: r.total, with_fbp: r.with_fbp, with_fbc: r.with_fbc };
        });
        const totalValueBRL = parseFloat(valueResult.rows[0]?.total_value || 0);
        const brlToUsd = parseFloat(process.env.CONVERSION_BRL_TO_USD || '0.18');
        if (minutes > 0) {
            const end = new Date();
            const start = new Date(end.getTime() - minutes * 60 * 1000);
            startDate = start.toISOString().slice(0, 19);
            endDate = end.toISOString().slice(0, 19);
        } else if (hours > 0) {
            const end = new Date();
            const start = new Date(end.getTime() - hours * 60 * 60 * 1000);
            startDate = start.toISOString().slice(0, 19);
            endDate = end.toISOString().slice(0, 19);
        }

        res.json({
            startDate: startDate || undefined,
            endDate: endDate || undefined,
            total: row.total,
            with_fbp: row.with_fbp,
            with_fbc: row.with_fbc,
            with_email: row.with_email,
            with_phone: row.with_phone,
            with_ip: row.with_ip,
            with_user_agent: row.with_user_agent,
            with_visitor_id: row.with_visitor_id,
            with_country: row.with_country,
            facebook_ads: row.facebook_ads,
            facebook_ads_with_fbc: row.facebook_ads_with_fbc,
            direct: row.direct,
            other: row.other,
            unidentified: row.unidentified,
            by_language: byLang,
            total_value_brl: totalValueBRL,
            total_value_usd_approx: Math.round(totalValueBRL * brlToUsd * 100) / 100
        });
    } catch (error) {
        console.error('pixel-stats error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Database diagnostic endpoint (admin only)
app.get('/api/health/db', authenticateToken, (req, res, next) => {
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Access denied. Admin only.' });
    next();
}, async (req, res) => {
    try {
        const leadsCount = await pool.query('SELECT COUNT(*) FROM leads');
        const transactionsCount = await pool.query('SELECT COUNT(*) FROM transactions');
        const eventsCount = await pool.query('SELECT COUNT(*) FROM funnel_events');
        
        res.json({
            status: 'ok',
            database: 'connected',
            counts: {
                leads: parseInt(leadsCount.rows[0].count),
                transactions: parseInt(transactionsCount.rows[0].count),
                funnel_events: parseInt(eventsCount.rows[0].count)
            },
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(500).json({
            status: 'error',
            database: 'disconnected',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// Root route - serves HTML with Facebook domain verification meta tag
app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html>
<html><head>
<meta name="facebook-domain-verification" content="88bg7nb3af9s66oo1b7oekmo287t2i" />
<meta name="facebook-domain-verification" content="mmgxqvywkcn38obhqg1g5j1cj3g7d8" />
<title>ZapSpy.ai</title>
</head><body>
<h1>ZapSpy.ai API</h1>
<p>Status: running</p>
<p><a href="/admin.html">Admin Panel</a></p>
</body></html>`);
});

// Capture lead (from frontend form)
app.post('/api/leads', leadLimiter, async (req, res) => {
    try {
        const {
            name,
            email,
            whatsapp,
            targetPhone,
            targetGender,
            city: frontendCity,              // City from frontend geo detection
            country_code: frontendCountryCode, // Country code from frontend
            state: frontendState,            // State/province from frontend geo detection
            pageUrl,                         // Actual page URL for eventSourceUrl
            referrer,
            userAgent,
            fbc,  // Facebook click ID (from URL param or cookie)
            fbp,  // Facebook browser ID (from cookie)
            funnelLanguage,  // 'en' or 'es' - funnel language for pixel selection
            visitorId,  // Funnel visitor ID for journey tracking
            funnelSource,  // 'main' or 'affiliate' - source of the lead
            // UTM parameters for campaign tracking
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            utm_term
        } = req.body;
        
        // Validation
        if (!email || !whatsapp) {
            return res.status(400).json({ error: 'Email and WhatsApp are required' });
        }
        
        // Get IP address
        const ipAddress = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        const ua = userAgent || req.headers['user-agent'];
        
        // Determine language (default to 'en' for backward compatibility)
        const language = funnelLanguage || 'en';
        
        // Determine source (default to 'main' for backward compatibility)
        const source = funnelSource || 'main';
        
        // Get country from IP (non-blocking), but prefer frontend-provided values
        const ipGeoData = await getCountryFromIP(ipAddress);
        const geoData = {
            country: ipGeoData.country,
            country_code: frontendCountryCode || ipGeoData.country_code,
            city: frontendCity || ipGeoData.city,
            state: frontendState || ipGeoData.state
        };
        
        // Check if lead already exists (by email or whatsapp)
        const existingLead = await pool.query(
            `SELECT id, email, whatsapp, visit_count FROM leads WHERE LOWER(email) = LOWER($1) OR whatsapp = $2 LIMIT 1`,
            [email, whatsapp]
        );
        
        let result;
        let isNewLead = false;
        
        if (existingLead.rows.length > 0) {
            // Update existing lead with new visit info
            const currentVisitCount = existingLead.rows[0].visit_count || 1;
            result = await pool.query(
                `UPDATE leads SET 
                    name = COALESCE($1, name),
                    target_phone = COALESCE($2, target_phone),
                    target_gender = COALESCE($3, target_gender),
                    ip_address = $4,
                    referrer = $5,
                    user_agent = $6,
                    visit_count = $7,
                    country = COALESCE($8, country),
                    country_code = COALESCE($9, country_code),
                    city = COALESCE($10, city),
                    visitor_id = COALESCE($11, visitor_id),
                    funnel_source = COALESCE($12, funnel_source),
                    utm_source = COALESCE($14, utm_source),
                    utm_medium = COALESCE($15, utm_medium),
                    utm_campaign = COALESCE($16, utm_campaign),
                    utm_content = COALESCE($17, utm_content),
                    utm_term = COALESCE($18, utm_term),
                    fbc = COALESCE($19, fbc),
                    fbp = COALESCE($20, fbp),
                    state = COALESCE($21, state),
                    last_visit_at = NOW(),
                    updated_at = NOW()
                WHERE id = $13
                RETURNING id, created_at`,
                [name || null, targetPhone || null, targetGender || null, ipAddress, referrer || null, ua || null, currentVisitCount + 1, geoData.country, geoData.country_code, geoData.city, visitorId || null, source, existingLead.rows[0].id, utm_source || null, utm_medium || null, utm_campaign || null, utm_content || null, utm_term || null, fbc || null, fbp || null, geoData.state || null]
            );
            console.log(`Returning lead [${language.toUpperCase()}/${source}]: ${name || 'No name'} - ${email} - ${geoData.country || 'Unknown'} (visit #${currentVisitCount + 1})`);
        } else {
            // Insert new lead
            result = await pool.query(
                `INSERT INTO leads (name, email, whatsapp, target_phone, target_gender, ip_address, referrer, user_agent, funnel_language, funnel_source, visit_count, country, country_code, city, state, visitor_id, utm_source, utm_medium, utm_campaign, utm_content, utm_term, fbc, fbp, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, NOW())
                 RETURNING id, created_at`,
                [name || null, email, whatsapp, targetPhone || null, targetGender || null, ipAddress, referrer || null, ua || null, language, source, geoData.country, geoData.country_code, geoData.city, geoData.state || null, visitorId || null, utm_source || null, utm_medium || null, utm_campaign || null, utm_content || null, utm_term || null, fbc || null, fbp || null]
            );
            isNewLead = true;
            console.log(`New lead captured [${language.toUpperCase()}/${source}]: ${name || 'No name'} - ${email} - ${whatsapp} - ${geoData.country || 'Unknown'}${utm_source ? ` [UTM: ${utm_source}]` : ''}`);
        }
        
        // NOTE: Lead CAPI event is already sent by the frontend (FacebookCAPI.trackEvent('Lead'))
        // with proper eventID for deduplication. Sending another here with null eventID would cause duplicates.
        // The /api/leads endpoint only stores the lead data now.
        console.log(`📊 Lead stored. CAPI Lead event handled by frontend with deduplication.`);
        
        // Invalidate relevant caches
        invalidateCache('trends');
        invalidateCache('traffic-sources');
        
        // Auto-verify WhatsApp for new leads (async - does not block response)
        if (isNewLead && whatsapp && result.rows[0]?.id) {
            const leadId = result.rows[0].id;
            const cleanPhone = whatsapp.replace(/\D/g, '');
            if (cleanPhone.length >= 10) {
                setImmediate(async () => {
                    try {
                        const zapiHeaders = {};
                        if (ZAPI_CLIENT_TOKEN) zapiHeaders['Client-Token'] = ZAPI_CLIENT_TOKEN;
                        
                        const verifyResponse = await fetch(`${ZAPI_BASE_URL}/phone-exists/${cleanPhone}`, {
                            method: 'GET',
                            headers: zapiHeaders
                        });
                        const verifyData = await verifyResponse.json();
                        const isRegistered = verifyData.exists === true;
                        
                        // Try to get profile picture if registered
                        let profilePicture = null;
                        if (isRegistered) {
                            try {
                                const picResponse = await fetch(`${ZAPI_BASE_URL}/profile-picture?phone=${cleanPhone}`, {
                                    headers: zapiHeaders
                                });
                                const picData = await picResponse.json();
                                if (picData.link && picData.link !== 'null' && picData.link.startsWith('http')) {
                                    profilePicture = picData.link;
                                }
                            } catch (e) { /* ignore pic errors */ }
                        }
                        
                        // Update lead with verification result
                        await pool.query(`
                            UPDATE leads SET 
                                whatsapp_verified = $1,
                                whatsapp_verified_at = NOW(),
                                whatsapp_profile_pic = $2,
                                updated_at = NOW()
                            WHERE id = $3
                        `, [isRegistered, profilePicture, leadId]);
                        
                        console.log(`📱 Auto-verified WhatsApp for lead #${leadId}: ${cleanPhone} → ${isRegistered ? '✓ Valid' : '✕ Invalid'}${profilePicture ? ' (with pic)' : ''}`);
                    } catch (verifyError) {
                        console.log(`📱 Auto-verify WhatsApp failed for lead #${leadId}:`, verifyError.message);
                    }
                });
            }
        }
        
        res.status(201).json({
            success: true,
            message: 'Lead captured successfully',
            id: result.rows[0].id,
            language: language
        });
        
    } catch (error) {
        console.error('Error capturing lead:', error);
        res.status(500).json({ error: 'Failed to capture lead' });
    }
});

// ==================== FACEBOOK CAPI ENDPOINT ====================

// Test CAPI events - use this endpoint to verify events in Facebook Events Manager
// Test codes: EN = TEST23104, ES = TEST96875
app.post('/api/capi/test', async (req, res) => {
    try {
        const { language, eventName } = req.body;
        
        // Test event codes from Facebook Events Manager
        const testCodes = {
            'en': 'TEST23104',
            'es': 'TEST96875'
        };
        
        const lang = language || 'en';
        const testCode = testCodes[lang];
        const event = eventName || 'PageView';
        
        if (!testCode) {
            return res.status(400).json({ error: 'Invalid language. Use "en" or "es"' });
        }
        
        // Get IP and User Agent from request
        const ipAddress = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        const userAgent = req.headers['user-agent'];
        
        // Build test user data
        const userData = {
            email: 'test@example.com',
            phone: '+5511999999999',
            firstName: 'Test User',
            ip: ipAddress,
            userAgent,
            externalId: 'test_visitor_' + Date.now()
        };
        
        // Build test custom data
        const customData = {
            value: lang === 'es' ? 27.00 : 37.00,
            currency: 'USD',
            content_name: 'Test Event',
            content_category: 'test'
        };
        
        // Send with test_event_code
        const results = await sendToFacebookCAPI(
            event, 
            userData, 
            customData, 
            `https://${lang === 'es' ? 'espanhol' : 'ingles'}.zappdetect.com/landing.html`,
            `test_${Date.now()}`,
            { 
                language: lang,
                testEventCode: testCode  // This enables test mode
            }
        );
        
        res.json({ 
            success: true, 
            message: `Test event ${event} sent to ${lang.toUpperCase()} pixel`,
            testCode: testCode,
            language: lang,
            results,
            instructions: 'Check Facebook Events Manager > "Eventos de teste" tab to see this event'
        });
        
    } catch (error) {
        console.error('CAPI test error:', error);
        res.status(500).json({ error: 'Failed to send test event', details: error.message });
    }
});

// Send event to Facebook CAPI (from frontend)
app.post('/api/capi/event', async (req, res) => {
    try {
        const {
            eventName,
            eventId,           // For deduplication with browser pixel
            externalId,        // Visitor ID for cross-device tracking
            email,
            phone,
            firstName,
            lastName,
            country,           // Country code (2-letter ISO) for better match quality
            city,              // City name for better match quality
            state,             // State/province for better match quality
            gender,            // Gender (m/f) for better match quality
            value,
            currency,
            contentName,
            contentIds,
            contentType,
            contentCategory,
            numItems,
            fbc,
            fbp,
            eventSourceUrl,
            // Language and custom pixel support for multi-language funnels
            funnelLanguage,    // 'en' or 'es'
            pixelIds,          // Array of custom pixel IDs (from frontend)
            accessToken        // Custom access token (from frontend)
        } = req.body;
        
        if (!eventName) {
            return res.status(400).json({ error: 'eventName is required' });
        }
        
        // Get IP and User Agent from request
        const ipAddress = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        const userAgent = req.headers['user-agent'];
        
        // Build user data (including geo data for better match quality)
        const userData = {
            email,
            phone,
            firstName,
            lastName,
            country,           // Country code for CAPI matching
            city,              // City for CAPI matching
            state,             // State/province for CAPI matching
            gender,            // Gender for CAPI matching
            externalId,        // For cross-device tracking
            ip: ipAddress,
            userAgent,
            fbc,
            fbp
        };
        
        // Build custom data
        const customData = {};
        if (value !== undefined) customData.value = parseFloat(value);
        if (currency) customData.currency = currency;
        if (contentName) customData.content_name = contentName;
        if (contentIds) customData.content_ids = Array.isArray(contentIds) ? contentIds : [contentIds];
        if (contentType) customData.content_type = contentType;
        if (contentCategory) customData.content_category = contentCategory;
        if (numItems) customData.num_items = parseInt(numItems);
        
        // Options for pixel selection
        const options = {
            language: funnelLanguage || 'en',
            pixelIds: pixelIds,
            accessToken: accessToken
        };
        
        // Send to Facebook CAPI with eventId for deduplication
        const results = await sendToFacebookCAPI(eventName, userData, customData, eventSourceUrl, eventId, options);
        
        res.json({ 
            success: true, 
            message: `Event ${eventName} sent to CAPI`,
            eventId: eventId || results[0]?.eventId,
            language: options.language,
            results 
        });
        
    } catch (error) {
        console.error('CAPI endpoint error:', error);
        res.status(500).json({ error: 'Failed to send event' });
    }
});

// ==================== ADMIN API ROUTES ====================

// Admin login
app.post('/api/admin/login', apiLimiter, async (req, res) => {
    try {
        const { email, password, username } = req.body;
        const loginIdentifier = email || username;
        
        if (!loginIdentifier || !password) {
            return res.status(400).json({ error: 'Email/username and password are required' });
        }
        
        let user = null;
        let validPassword = false;
        
        // First, check environment variables for master admin (backward compatibility)
        const envEmail = process.env.ADMIN_EMAIL;
        const envPassword = process.env.ADMIN_PASSWORD;
        
        if (envEmail && envPassword) {
            if (loginIdentifier === envEmail || loginIdentifier.toLowerCase() === envEmail.toLowerCase()) {
                validPassword = password === envPassword;
                if (validPassword) {
                    user = { email: envEmail, role: 'admin', name: 'Administrador Master', id: 0, username: 'admin' };
                    console.log(`✅ Master admin login: ${envEmail}`);
                }
            }
        }
        
        // If not master admin, try database users
        if (!validPassword) {
            const userResult = await pool.query(
                'SELECT * FROM admin_users WHERE (LOWER(email) = LOWER($1) OR LOWER(username) = LOWER($1)) AND is_active = true',
                [loginIdentifier]
            );
            
            if (userResult.rows.length > 0) {
                // User found in database
                const dbUser = userResult.rows[0];
                validPassword = await bcrypt.compare(password, dbUser.password_hash);
                
                if (validPassword) {
                    user = dbUser;
                    // Update last login
                    await pool.query('UPDATE admin_users SET last_login = NOW() WHERE id = $1', [dbUser.id]);
                    console.log(`✅ Database user login: ${dbUser.email} (${dbUser.role})`);
                }
            }
        }
        
        if (!validPassword || !user) {
            console.log(`❌ Failed login attempt for: ${loginIdentifier}`);
            return res.status(401).json({ error: 'Invalid credentials' });
        }
        
        // Generate JWT token with user info
        const token = jwt.sign(
            { 
                userId: user.id,
                email: user.email, 
                username: user.username,
                role: user.role,
                name: user.name
            },
            process.env.JWT_SECRET,
            { expiresIn: '24h' }
        );
        
        res.json({
            success: true,
            token,
            user: {
                id: user.id,
                email: user.email,
                username: user.username,
                name: user.name,
                role: user.role
            },
            expiresIn: '24h'
        });
        
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ error: 'Login failed' });
    }
});

// ==================== USER MANAGEMENT (Admin Only) ====================

// Middleware to check admin role
const requireAdmin = (req, res, next) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({ error: 'Access denied. Admin only.' });
    }
    next();
};

// Get all users
app.get('/api/admin/users', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT id, username, email, COALESCE(name, full_name) as name, role, is_active, last_login, created_at
            FROM admin_users
            ORDER BY created_at DESC
        `);
        
        res.json({ users: result.rows });
    } catch (error) {
        console.error('Error fetching users:', error);
        res.status(500).json({ error: 'Failed to fetch users' });
    }
});

// Create new user
app.post('/api/admin/users', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const { username, email, password, name, role } = req.body;
        
        if (!username || !email || !password) {
            return res.status(400).json({ error: 'Username, email and password are required' });
        }
        
        // Validate role
        const allowedRoles = ['admin', 'support', 'viewer'];
        const userRole = allowedRoles.includes(role) ? role : 'support';
        
        // Check if user already exists - with specific error
        const existingEmail = await pool.query(
            'SELECT id FROM admin_users WHERE email = $1',
            [email]
        );
        
        if (existingEmail.rows.length > 0) {
            return res.status(409).json({ error: 'Este email já está cadastrado no sistema' });
        }
        
        const existingUsername = await pool.query(
            'SELECT id FROM admin_users WHERE username = $1',
            [username]
        );
        
        if (existingUsername.rows.length > 0) {
            return res.status(409).json({ error: 'Este username já está em uso' });
        }
        
        // Hash password
        const hashedPassword = await bcrypt.hash(password, 10);
        
        // Insert new user (use both name and full_name for compatibility)
        const userName = name || username;
        const result = await pool.query(`
            INSERT INTO admin_users (username, email, password_hash, name, full_name, role, is_active, created_by)
            VALUES ($1, $2, $3, $4, $4, $5, true, $6)
            RETURNING id, username, email, COALESCE(name, full_name) as name, role, is_active, created_at
        `, [username, email, hashedPassword, userName, userRole, req.user.userId]);
        
        console.log(`✅ New user created: ${username} (${role}) by admin ${req.user.email}`);
        
        res.json({ success: true, user: result.rows[0] });
    } catch (error) {
        console.error('Error creating user:', error);
        
        // Provide more specific error messages
        if (error.code === '23505') {
            // Unique constraint violation
            if (error.constraint && error.constraint.includes('email')) {
                return res.status(409).json({ error: 'Este email já está em uso' });
            }
            if (error.constraint && error.constraint.includes('username')) {
                return res.status(409).json({ error: 'Este username já está em uso' });
            }
            return res.status(409).json({ error: 'Usuário já existe com este email ou username' });
        }
        
        res.status(500).json({ error: 'Falha ao criar usuário: ' + (error.message || 'Erro desconhecido') });
    }
});

// Update user
app.put('/api/admin/users/:id', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const { id } = req.params;
        const { name, email, role, is_active, password } = req.body;
        
        // Don't allow modifying the main admin (id = 1) by non-main-admins
        if (parseInt(id) === 1 && req.user.userId !== 1) {
            return res.status(403).json({ error: 'Cannot modify main admin user' });
        }
        
        // Build update query dynamically
        let updates = [];
        let values = [];
        let paramCount = 1;
        
        if (name !== undefined) {
            updates.push(`name = $${paramCount++}`);
            values.push(name);
        }
        if (email !== undefined && email.trim()) {
            // Check if email is already in use by another user
            const emailCheck = await pool.query(
                'SELECT id FROM admin_users WHERE email = $1 AND id != $2',
                [email.trim().toLowerCase(), id]
            );
            if (emailCheck.rows.length > 0) {
                return res.status(409).json({ error: 'Este email já está em uso por outro usuário' });
            }
            updates.push(`email = $${paramCount++}`);
            values.push(email.trim().toLowerCase());
        }
        if (role !== undefined) {
            const allowedRoles = ['admin', 'support', 'viewer'];
            if (allowedRoles.includes(role)) {
                updates.push(`role = $${paramCount++}`);
                values.push(role);
            }
        }
        if (is_active !== undefined) {
            updates.push(`is_active = $${paramCount++}`);
            values.push(is_active);
        }
        if (password) {
            if (password.length < 6) {
                return res.status(400).json({ error: 'A senha deve ter pelo menos 6 caracteres' });
            }
            const hashedPassword = await bcrypt.hash(password, 10);
            updates.push(`password_hash = $${paramCount++}`);
            values.push(hashedPassword);
        }
        
        if (updates.length === 0) {
            return res.status(400).json({ error: 'No fields to update' });
        }
        
        values.push(id);
        
        const result = await pool.query(`
            UPDATE admin_users 
            SET ${updates.join(', ')}
            WHERE id = $${paramCount}
            RETURNING id, username, email, name, role, is_active, created_at
        `, values);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }
        
        console.log(`✅ User updated: ${id} by admin ${req.user.email}`);
        
        res.json({ success: true, user: result.rows[0] });
    } catch (error) {
        console.error('Error updating user:', error);
        res.status(500).json({ error: 'Failed to update user' });
    }
});

// Delete user
app.delete('/api/admin/users/:id', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const { id } = req.params;
        
        // Don't allow deleting the main admin
        if (parseInt(id) === 1) {
            return res.status(403).json({ error: 'Cannot delete main admin user' });
        }
        
        // Don't allow self-delete
        if (parseInt(id) === req.user.userId) {
            return res.status(403).json({ error: 'Cannot delete your own account' });
        }
        
        const result = await pool.query(
            'DELETE FROM admin_users WHERE id = $1 RETURNING username, email',
            [id]
        );
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }
        
        console.log(`🗑️ User deleted: ${result.rows[0].username} by admin ${req.user.email}`);
        
        res.json({ success: true, message: 'User deleted' });
    } catch (error) {
        console.error('Error deleting user:', error);
        res.status(500).json({ error: 'Failed to delete user' });
    }
});

// Verify password for security-sensitive actions
app.post('/api/admin/verify-password', authenticateToken, async (req, res) => {
    try {
        const { password } = req.body;
        
        if (!password) {
            return res.json({ valid: false, message: 'Password required' });
        }
        
        // Check if master admin (from env vars)
        if (req.user.userId === 0 || req.user.email === process.env.ADMIN_EMAIL) {
            // Verify against env password
            const envPassword = process.env.ADMIN_PASSWORD;
            if (password === envPassword) {
                console.log(`🔐 Password verified for master admin: ${req.user.email}`);
                return res.json({ valid: true });
            } else {
                console.log(`❌ Invalid password attempt for master admin: ${req.user.email}`);
                return res.json({ valid: false });
            }
        }
        
        // Check database user
        const result = await pool.query(
            'SELECT password_hash FROM admin_users WHERE id = $1',
            [req.user.userId]
        );
        
        if (result.rows.length === 0) {
            return res.json({ valid: false, message: 'User not found' });
        }
        
        const passwordMatch = await bcrypt.compare(password, result.rows[0].password_hash);
        
        if (passwordMatch) {
            console.log(`🔐 Password verified for user ID: ${req.user.userId}`);
            return res.json({ valid: true });
        } else {
            console.log(`❌ Invalid password attempt for user ID: ${req.user.userId}`);
            return res.json({ valid: false });
        }
        
    } catch (error) {
        console.error('Error verifying password:', error);
        res.status(500).json({ valid: false, error: 'Verification failed' });
    }
});

// Get current user profile
app.get('/api/admin/profile', authenticateToken, async (req, res) => {
    try {
        if (req.user.userId === 0) {
            // Fallback admin from env vars
            return res.json({
                user: {
                    id: 0,
                    username: 'admin',
                    email: req.user.email,
                    name: 'Administrador',
                    role: 'admin'
                }
            });
        }
        
        const result = await pool.query(`
            SELECT id, username, email, name, role, last_login, created_at
            FROM admin_users WHERE id = $1
        `, [req.user.userId]);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }
        
        res.json({ user: result.rows[0] });
    } catch (error) {
        console.error('Error fetching profile:', error);
        res.status(500).json({ error: 'Failed to fetch profile' });
    }
});

// Update own password
app.put('/api/admin/profile/password', authenticateToken, async (req, res) => {
    try {
        const { currentPassword, newPassword } = req.body;
        
        if (!currentPassword || !newPassword) {
            return res.status(400).json({ error: 'Current and new passwords are required' });
        }
        
        if (newPassword.length < 6) {
            return res.status(400).json({ error: 'New password must be at least 6 characters' });
        }
        
        // Get current user
        const userResult = await pool.query(
            'SELECT password_hash FROM admin_users WHERE id = $1',
            [req.user.userId]
        );
        
        if (userResult.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }
        
        // Verify current password
        const validPassword = await bcrypt.compare(currentPassword, userResult.rows[0].password_hash);
        if (!validPassword) {
            return res.status(401).json({ error: 'Current password is incorrect' });
        }
        
        // Update password
        const hashedPassword = await bcrypt.hash(newPassword, 10);
        await pool.query(
            'UPDATE admin_users SET password_hash = $1 WHERE id = $2',
            [hashedPassword, req.user.userId]
        );
        
        console.log(`🔐 Password changed for user ${req.user.email}`);
        
        res.json({ success: true, message: 'Password updated successfully' });
    } catch (error) {
        console.error('Error changing password:', error);
        res.status(500).json({ error: 'Failed to change password' });
    }
});

// Get all leads (protected)
app.get('/api/admin/leads', authenticateToken, async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 50;
        const offset = (page - 1) * limit;
        const search = req.query.search || '';
        const status = req.query.status || '';
        const language = req.query.language || '';  // Filter by funnel language (en/es)
        const source = req.query.source || '';  // Filter by funnel source (main/affiliate)
        const { startDate, endDate } = req.query;
        
        let query = `SELECT * FROM leads`;
        let countQuery = `SELECT COUNT(*) FROM leads`;
        let params = [];
        let conditions = [];
        
        if (search) {
            conditions.push(`(email ILIKE $${params.length + 1} OR whatsapp ILIKE $${params.length + 1} OR target_phone ILIKE $${params.length + 1} OR name ILIKE $${params.length + 1})`);
            params.push(`%${search}%`);
        }
        
        if (status) {
            conditions.push(`status = $${params.length + 1}`);
            params.push(status);
        }
        
        if (language) {
            // Treat NULL as 'en' (English is default for legacy leads)
            if (language === 'en') {
                conditions.push(`(funnel_language = $${params.length + 1} OR funnel_language IS NULL)`);
            } else {
                conditions.push(`funnel_language = $${params.length + 1}`);
            }
            params.push(language);
        }
        
        if (source) {
            // Treat NULL as 'main' (main is default for legacy leads)
            if (source === 'main') {
                conditions.push(`(funnel_source = $${params.length + 1} OR funnel_source IS NULL)`);
            } else {
                conditions.push(`funnel_source = $${params.length + 1}`);
            }
            params.push(source);
        }
        
        if (startDate && endDate) {
            conditions.push(`(created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${params.length + 1}::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${params.length + 2}::date`);
            params.push(startDate, endDate);
        }
        
        // WhatsApp verification filter
        const whatsappVerified = req.query.whatsapp_verified || '';
        if (whatsappVerified) {
            if (whatsappVerified === 'verified') {
                conditions.push(`whatsapp_verified = true`);
            } else if (whatsappVerified === 'invalid') {
                conditions.push(`whatsapp_verified = false`);
            } else if (whatsappVerified === 'pending') {
                conditions.push(`whatsapp_verified IS NULL`);
            }
        }
        
        if (conditions.length > 0) {
            query += ` WHERE ${conditions.join(' AND ')}`;
            countQuery += ` WHERE ${conditions.join(' AND ')}`;
        }
        
        query += ` ORDER BY created_at DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
        params.push(limit, offset);
        
        const [leadsResult, countResult] = await Promise.all([
            pool.query(query, params),
            pool.query(countQuery, params.slice(0, -2))
        ]);
        
        res.json({
            leads: leadsResult.rows,
            pagination: {
                page,
                limit,
                total: parseInt(countResult.rows[0].count),
                totalPages: Math.ceil(parseInt(countResult.rows[0].count) / limit)
            }
        });
        
    } catch (error) {
        console.error('Error fetching leads:', error);
        res.status(500).json({ error: 'Failed to fetch leads' });
    }
});

// Get clients (leads who purchased) - protected
app.get('/api/admin/clients', authenticateToken, async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 50;
        const offset = (page - 1) * limit;
        const search = req.query.search || '';
        const language = req.query.language || '';
        const source = req.query.source || '';
        const { startDate, endDate } = req.query;
        
        // Clients = leads with status 'converted' OR leads that have transactions with status 'approved'
        let conditions = [`(l.status = 'converted' OR l.total_spent > 0 OR l.first_purchase_at IS NOT NULL)`];
        let params = [];
        
        if (search) {
            conditions.push(`(l.email ILIKE $${params.length + 1} OR l.whatsapp ILIKE $${params.length + 1} OR l.name ILIKE $${params.length + 1})`);
            params.push(`%${search}%`);
        }
        
        if (language) {
            if (language === 'en') {
                conditions.push(`(l.funnel_language = $${params.length + 1} OR l.funnel_language IS NULL)`);
            } else {
                conditions.push(`l.funnel_language = $${params.length + 1}`);
            }
            params.push(language);
        }
        
        if (source) {
            if (source === 'main') {
                conditions.push(`(l.funnel_source = $${params.length + 1} OR l.funnel_source IS NULL)`);
            } else {
                conditions.push(`l.funnel_source = $${params.length + 1}`);
            }
            params.push(source);
        }
        
        // Date range filter on first_purchase_at
        if (startDate && endDate) {
            conditions.push(`(l.first_purchase_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${params.length + 1}::date`);
            conditions.push(`(l.first_purchase_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${params.length + 2}::date`);
            params.push(startDate, endDate);
        }
        
        const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
        
        // Main query: get clients with their purchase info
        const query = `
            SELECT l.id, l.name, l.email, l.whatsapp, l.country_code, l.country, l.city,
                   l.funnel_language, l.funnel_source, l.status,
                   l.products_purchased, l.total_spent, l.first_purchase_at, l.last_purchase_at,
                   l.whatsapp_verified, l.whatsapp_profile_pic, l.created_at,
                   (SELECT COUNT(*) FROM transactions t WHERE LOWER(t.email) = LOWER(l.email) AND t.status = 'approved') as tx_count,
                   (SELECT SUM(CAST(t.value AS DECIMAL)) FROM transactions t WHERE LOWER(t.email) = LOWER(l.email) AND t.status = 'approved') as tx_total
            FROM leads l
            ${whereClause}
            ORDER BY l.last_purchase_at DESC NULLS LAST, l.first_purchase_at DESC NULLS LAST
            LIMIT $${params.length + 1} OFFSET $${params.length + 2}
        `;
        
        const countQuery = `SELECT COUNT(*) FROM leads l ${whereClause}`;
        
        // Stats query
        const statsQuery = `
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE (l.first_purchase_at AT TIME ZONE 'America/Sao_Paulo')::date = CURRENT_DATE) as today,
                COALESCE(SUM(l.total_spent), 0) as total_revenue,
                CASE WHEN COUNT(*) > 0 THEN COALESCE(SUM(l.total_spent), 0) / COUNT(*) ELSE 0 END as avg_ticket
            FROM leads l
            ${whereClause}
        `;
        
        const queryParams = [...params, limit, offset];
        
        const [clientsResult, countResult, statsResult] = await Promise.all([
            pool.query(query, queryParams),
            pool.query(countQuery, params),
            pool.query(statsQuery, params)
        ]);
        
        const stats = statsResult.rows[0] || {};
        
        res.json({
            clients: clientsResult.rows,
            stats: {
                total: parseInt(stats.total || 0),
                today: parseInt(stats.today || 0),
                totalRevenue: parseFloat(stats.total_revenue || 0),
                avgTicket: parseFloat(stats.avg_ticket || 0)
            },
            pagination: {
                page,
                limit,
                total: parseInt(countResult.rows[0].count),
                totalPages: Math.ceil(parseInt(countResult.rows[0].count) / limit)
            }
        });
        
    } catch (error) {
        console.error('Error fetching clients:', error);
        res.status(500).json({ error: 'Failed to fetch clients' });
    }
});

// ==================== FINANCIAL ENDPOINTS ====================

// Get financial summary (revenue from transactions + costs)
app.get('/api/admin/financial/summary', authenticateToken, async (req, res) => {
    try {
        const days = parseInt(req.query.days) || 30;
        const language = req.query.language || '';
        const source = req.query.source || '';
        
        // Build transaction filters
        let txConditions = [`t.status = 'approved'`];
        let txParams = [];
        
        if (language) {
            if (language === 'en') {
                txConditions.push(`(t.funnel_language = $${txParams.length + 1} OR t.funnel_language IS NULL)`);
            } else {
                txConditions.push(`t.funnel_language = $${txParams.length + 1}`);
            }
            txParams.push(language);
        }
        
        if (source) {
            if (source === 'main') {
                txConditions.push(`(t.funnel_source = $${txParams.length + 1} OR t.funnel_source IS NULL)`);
            } else {
                txConditions.push(`t.funnel_source = $${txParams.length + 1}`);
            }
            txParams.push(source);
        }
        
        const txWhere = txConditions.join(' AND ');
        
        console.log('📊 Financial summary request - days:', days, 'language:', language, 'source:', source);
        
        // Today's revenue and sales (use NULLIF to handle empty strings in value)
        const todayQuery = `
            SELECT 
                COALESCE(SUM(CASE WHEN t.value ~ '^[0-9.]+$' THEN CAST(t.value AS DECIMAL) ELSE 0 END), 0) as revenue,
                COUNT(*) as sales
            FROM transactions t
            WHERE ${txWhere}
            AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date = CURRENT_DATE
        `;
        
        // This month's revenue and sales
        const monthQuery = `
            SELECT 
                COALESCE(SUM(CASE WHEN t.value ~ '^[0-9.]+$' THEN CAST(t.value AS DECIMAL) ELSE 0 END), 0) as revenue,
                COUNT(*) as sales
            FROM transactions t
            WHERE ${txWhere}
            AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= date_trunc('month', CURRENT_DATE)
        `;
        
        // Today's costs
        const todayCostsQuery = `
            SELECT COALESCE(SUM(amount_usd), 0) as total_usd
            FROM financial_costs
            WHERE cost_date = CURRENT_DATE
        `;
        
        // This month's costs
        const monthCostsQuery = `
            SELECT COALESCE(SUM(amount_usd), 0) as total_usd
            FROM financial_costs
            WHERE cost_date >= date_trunc('month', CURRENT_DATE)::date
        `;
        
        // Daily breakdown for the period
        // Use string interpolation for the days interval since pg parameterized queries 
        // don't work well with interval arithmetic on CURRENT_DATE
        const safeDays = Math.min(Math.max(parseInt(days) || 30, 1), 365);
        
        const dailyQuery = `
            WITH daily_revenue AS (
                SELECT 
                    (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date as day,
                    COALESCE(SUM(CASE WHEN t.value ~ '^[0-9.]+$' THEN CAST(t.value AS DECIMAL) ELSE 0 END), 0) as revenue,
                    COUNT(*) as sales
                FROM transactions t
                WHERE ${txWhere}
                AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= CURRENT_DATE - INTERVAL '${safeDays} days'
                GROUP BY (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date
            ),
            daily_costs AS (
                SELECT 
                    cost_date as day,
                    COALESCE(SUM(amount_usd), 0) as costs
                FROM financial_costs
                WHERE cost_date >= CURRENT_DATE - INTERVAL '${safeDays} days'
                GROUP BY cost_date
            ),
            date_series AS (
                SELECT generate_series(
                    (CURRENT_DATE - INTERVAL '${safeDays} days')::date,
                    CURRENT_DATE,
                    '1 day'::interval
                )::date as day
            )
            SELECT 
                ds.day,
                COALESCE(dr.revenue, 0) as revenue,
                COALESCE(dr.sales, 0) as sales,
                COALESCE(dc.costs, 0) as costs,
                COALESCE(dr.revenue, 0) - COALESCE(dc.costs, 0) as profit
            FROM date_series ds
            LEFT JOIN daily_revenue dr ON ds.day = dr.day
            LEFT JOIN daily_costs dc ON ds.day = dc.day
            ORDER BY ds.day DESC
        `;
        
        // Monthly breakdown (last 12 months)
        const monthlyQuery = `
            WITH monthly_revenue AS (
                SELECT 
                    date_trunc('month', (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date)::date as month,
                    COALESCE(SUM(CASE WHEN t.value ~ '^[0-9.]+$' THEN CAST(t.value AS DECIMAL) ELSE 0 END), 0) as revenue,
                    COUNT(*) as sales
                FROM transactions t
                WHERE ${txWhere}
                AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= CURRENT_DATE - INTERVAL '12 months'
                GROUP BY date_trunc('month', (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date)::date
            ),
            monthly_costs AS (
                SELECT 
                    date_trunc('month', cost_date)::date as month,
                    COALESCE(SUM(amount_usd), 0) as costs
                FROM financial_costs
                WHERE cost_date >= CURRENT_DATE - INTERVAL '12 months'
                GROUP BY date_trunc('month', cost_date)::date
            )
            SELECT 
                COALESCE(mr.month, mc.month) as month,
                COALESCE(mr.revenue, 0) as revenue,
                COALESCE(mr.sales, 0) as sales,
                COALESCE(mc.costs, 0) as costs,
                COALESCE(mr.revenue, 0) - COALESCE(mc.costs, 0) as profit
            FROM monthly_revenue mr
            FULL OUTER JOIN monthly_costs mc ON mr.month = mc.month
            ORDER BY month DESC
            LIMIT 12
        `;
        
        // Execute queries one by one for better error identification
        let todayResult, monthResult, todayCosts, monthCosts, dailyResult, monthlyResult;
        
        try {
            console.log('📊 Running todayQuery...');
            todayResult = await pool.query(todayQuery, txParams);
        } catch (e) {
            console.error('❌ todayQuery failed:', e.message);
            throw e;
        }
        
        try {
            console.log('📊 Running monthQuery...');
            monthResult = await pool.query(monthQuery, txParams);
        } catch (e) {
            console.error('❌ monthQuery failed:', e.message);
            throw e;
        }
        
        try {
            console.log('📊 Running todayCostsQuery...');
            todayCosts = await pool.query(todayCostsQuery);
        } catch (e) {
            console.error('❌ todayCostsQuery failed:', e.message);
            throw e;
        }
        
        try {
            console.log('📊 Running monthCostsQuery...');
            monthCosts = await pool.query(monthCostsQuery);
        } catch (e) {
            console.error('❌ monthCostsQuery failed:', e.message);
            throw e;
        }
        
        try {
            console.log('📊 Running dailyQuery...');
            dailyResult = await pool.query(dailyQuery, txParams);
        } catch (e) {
            console.error('❌ dailyQuery failed:', e.message);
            throw e;
        }
        
        try {
            console.log('📊 Running monthlyQuery...');
            monthlyResult = await pool.query(monthlyQuery, txParams);
        } catch (e) {
            console.error('❌ monthlyQuery failed:', e.message);
            throw e;
        }
        
        console.log('✅ All financial queries completed successfully');
        
        const todayData = todayResult.rows[0] || {};
        const monthData = monthResult.rows[0] || {};
        const todayCostData = todayCosts.rows[0] || {};
        const monthCostData = monthCosts.rows[0] || {};
        
        const todayRevenue = parseFloat(todayData.revenue || 0);
        const todaySales = parseInt(todayData.sales || 0);
        const todayCost = parseFloat(todayCostData.total_usd || 0);
        const todayProfit = todayRevenue - todayCost;
        
        const monthRevenue = parseFloat(monthData.revenue || 0);
        const monthSales = parseInt(monthData.sales || 0);
        const monthCost = parseFloat(monthCostData.total_usd || 0);
        const monthProfit = monthRevenue - monthCost;
        
        const monthROI = monthCost > 0 ? ((monthRevenue - monthCost) / monthCost * 100) : 0;
        const monthMargin = monthRevenue > 0 ? ((monthRevenue - monthCost) / monthRevenue * 100) : 0;
        const monthCPA = monthSales > 0 ? monthCost / monthSales : 0;
        
        res.json({
            today: {
                revenue: todayRevenue,
                sales: todaySales,
                costs: todayCost,
                profit: todayProfit
            },
            month: {
                revenue: monthRevenue,
                sales: monthSales,
                costs: monthCost,
                profit: monthProfit,
                roi: Math.round(monthROI * 100) / 100,
                margin: Math.round(monthMargin * 100) / 100,
                cpa: Math.round(monthCPA * 100) / 100
            },
            daily: dailyResult.rows,
            monthly: monthlyResult.rows
        });
        
    } catch (error) {
        console.error('❌ Error fetching financial summary:', error.message, error.stack?.split('\n').slice(0, 3).join('\n'));
        res.status(500).json({ error: 'Failed to fetch financial summary', details: error.message });
    }
});

// Get financial costs list
app.get('/api/admin/financial/costs', authenticateToken, async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 50;
        const offset = (page - 1) * limit;
        const category = req.query.category || '';
        const startDate = req.query.startDate || '';
        const endDate = req.query.endDate || '';
        
        let conditions = [];
        let params = [];
        
        if (category) {
            conditions.push(`category = $${params.length + 1}`);
            params.push(category);
        }
        
        if (startDate) {
            conditions.push(`cost_date >= $${params.length + 1}::date`);
            params.push(startDate);
        }
        
        if (endDate) {
            conditions.push(`cost_date <= $${params.length + 1}::date`);
            params.push(endDate);
        }
        
        const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
        
        const query = `
            SELECT * FROM financial_costs
            ${whereClause}
            ORDER BY cost_date DESC, created_at DESC
            LIMIT $${params.length + 1} OFFSET $${params.length + 2}
        `;
        
        const countQuery = `SELECT COUNT(*) FROM financial_costs ${whereClause}`;
        
        const sumQuery = `
            SELECT 
                COALESCE(SUM(amount_usd), 0) as total_usd,
                COALESCE(SUM(CASE WHEN currency = 'BRL' THEN amount ELSE 0 END), 0) as total_brl,
                COUNT(*) as count
            FROM financial_costs ${whereClause}
        `;
        
        const queryParams = [...params, limit, offset];
        
        const [costsResult, countResult, sumResult] = await Promise.all([
            pool.query(query, queryParams),
            pool.query(countQuery, params),
            pool.query(sumQuery, params)
        ]);
        
        const stats = sumResult.rows[0] || {};
        
        res.json({
            costs: costsResult.rows,
            stats: {
                totalUsd: parseFloat(stats.total_usd || 0),
                totalBrl: parseFloat(stats.total_brl || 0),
                count: parseInt(stats.count || 0)
            },
            pagination: {
                page,
                limit,
                total: parseInt(countResult.rows[0].count),
                totalPages: Math.ceil(parseInt(countResult.rows[0].count) / limit)
            }
        });
        
    } catch (error) {
        console.error('Error fetching costs:', error);
        res.status(500).json({ error: 'Failed to fetch costs' });
    }
});

// Add a new cost
app.post('/api/admin/financial/costs', authenticateToken, async (req, res) => {
    try {
        const { cost_date, category, description, amount, currency, exchange_rate, notes } = req.body;
        
        if (!cost_date || !description || !amount) {
            return res.status(400).json({ error: 'Data, descrição e valor são obrigatórios' });
        }
        
        // Convert to USD if BRL
        let amountUsd = parseFloat(amount);
        let xRate = parseFloat(exchange_rate) || null;
        
        if (currency === 'BRL' && xRate) {
            amountUsd = parseFloat(amount) / xRate;
        } else if (currency === 'BRL') {
            // Use a default rate if none provided
            amountUsd = parseFloat(amount) / 5.80;
            xRate = 5.80;
        }
        
        const result = await pool.query(`
            INSERT INTO financial_costs (cost_date, category, description, amount, currency, amount_usd, exchange_rate, notes, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING *
        `, [cost_date, category || 'other', description, parseFloat(amount), currency || 'BRL', Math.round(amountUsd * 100) / 100, xRate, notes || null, req.user?.id || null]);
        
        console.log(`💰 New cost added: ${description} - ${currency} ${amount} (USD ${amountUsd.toFixed(2)}) on ${cost_date}`);
        
        res.json({ success: true, cost: result.rows[0] });
        
    } catch (error) {
        console.error('Error adding cost:', error);
        res.status(500).json({ error: 'Failed to add cost' });
    }
});

// Update a cost
app.put('/api/admin/financial/costs/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { cost_date, category, description, amount, currency, exchange_rate, notes } = req.body;
        
        let amountUsd = parseFloat(amount);
        let xRate = parseFloat(exchange_rate) || null;
        
        if (currency === 'BRL' && xRate) {
            amountUsd = parseFloat(amount) / xRate;
        } else if (currency === 'BRL') {
            amountUsd = parseFloat(amount) / 5.80;
            xRate = 5.80;
        }
        
        const result = await pool.query(`
            UPDATE financial_costs 
            SET cost_date = $1, category = $2, description = $3, amount = $4, currency = $5, 
                amount_usd = $6, exchange_rate = $7, notes = $8, updated_at = NOW()
            WHERE id = $9
            RETURNING *
        `, [cost_date, category, description, parseFloat(amount), currency, Math.round(amountUsd * 100) / 100, xRate, notes || null, id]);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Cost not found' });
        }
        
        res.json({ success: true, cost: result.rows[0] });
        
    } catch (error) {
        console.error('Error updating cost:', error);
        res.status(500).json({ error: 'Failed to update cost' });
    }
});

// Delete a cost
app.delete('/api/admin/financial/costs/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        const result = await pool.query('DELETE FROM financial_costs WHERE id = $1 RETURNING *', [id]);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Cost not found' });
        }
        
        console.log(`🗑️ Cost deleted: ID ${id}`);
        res.json({ success: true });
        
    } catch (error) {
        console.error('Error deleting cost:', error);
        res.status(500).json({ error: 'Failed to delete cost' });
    }
});

// Get lead statistics (protected)
app.get('/api/admin/stats', authenticateToken, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        
        // Build date filter (using Brazil timezone)
        let dateFilter = '';
        const params = [];
        if (startDate && endDate) {
            dateFilter = ` AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $1::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $2::date`;
            params.push(startDate, endDate);
        }
        
        const [totalResult, todayResult, weekResult, statusResult] = await Promise.all([
            pool.query(`SELECT COUNT(*) FROM leads WHERE 1=1${dateFilter}`, params),
            pool.query(`SELECT COUNT(*) FROM leads WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date${dateFilter}`, params),
            pool.query(`SELECT COUNT(*) FROM leads WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '7 days')::date${dateFilter}`, params),
            pool.query(`SELECT status, COUNT(*) FROM leads WHERE 1=1${dateFilter} GROUP BY status`, params)
        ]);
        
        // Get leads by day for the last 7 days (using Brazil timezone)
        const dailyResult = await pool.query(`
            SELECT (created_at AT TIME ZONE 'America/Sao_Paulo')::date as date, COUNT(*) as count
            FROM leads
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '7 days')::date${dateFilter}
            GROUP BY (created_at AT TIME ZONE 'America/Sao_Paulo')::date
            ORDER BY date DESC
        `, params);
        
        // Get leads by gender
        const genderResult = await pool.query(`
            SELECT target_gender, COUNT(*) FROM leads WHERE 1=1${dateFilter} GROUP BY target_gender
        `, params);
        
        res.json({
            total: parseInt(totalResult.rows[0].count),
            today: parseInt(todayResult.rows[0].count),
            thisWeek: parseInt(weekResult.rows[0].count),
            byStatus: statusResult.rows,
            byDay: dailyResult.rows,
            byGender: genderResult.rows
        });
        
    } catch (error) {
        console.error('Error fetching stats:', error);
        res.status(500).json({ error: 'Failed to fetch statistics' });
    }
});

// Get period comparison stats (current week vs previous week)
app.get('/api/admin/stats/comparison', authenticateToken, async (req, res) => {
    try {
        // Current week stats (using Brazil timezone)
        const currentWeekLeads = await pool.query(`
            SELECT COUNT(*) FROM leads 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '7 days')::date
        `);
        
        const currentWeekSales = await pool.query(`
            SELECT COUNT(*), COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
            FROM transactions 
            WHERE status = 'approved' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '7 days')::date
        `);
        
        // Previous week stats (using Brazil timezone)
        const previousWeekLeads = await pool.query(`
            SELECT COUNT(*) FROM leads 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '14 days')::date 
            AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date < ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '7 days')::date
        `);
        
        const previousWeekSales = await pool.query(`
            SELECT COUNT(*), COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
            FROM transactions 
            WHERE status = 'approved' 
            AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '14 days')::date
            AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date < ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '7 days')::date
        `);
        
        // Hourly heatmap data (for conversion optimization) - using Brazil timezone
        const hourlyData = await pool.query(`
            SELECT 
                EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/Sao_Paulo') as hour,
                EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo') as day_of_week,
                COUNT(*) as count
            FROM leads
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '30 days')::date
            GROUP BY EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/Sao_Paulo'), EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo')
            ORDER BY day_of_week, hour
        `);
        
        res.json({
            currentWeek: {
                leads: parseInt(currentWeekLeads.rows[0].count),
                sales: parseInt(currentWeekSales.rows[0].count),
                revenue: parseFloat(currentWeekSales.rows[0].revenue) || 0
            },
            previousWeek: {
                leads: parseInt(previousWeekLeads.rows[0].count),
                sales: parseInt(previousWeekSales.rows[0].count),
                revenue: parseFloat(previousWeekSales.rows[0].revenue) || 0
            },
            hourlyHeatmap: hourlyData.rows
        });
        
    } catch (error) {
        console.error('Error fetching comparison stats:', error);
        res.status(500).json({ error: 'Failed to fetch comparison statistics' });
    }
});

// Get flexible period comparison (day, week, month, quarter)
app.get('/api/admin/stats/period-comparison', authenticateToken, async (req, res) => {
    try {
        const { type = 'week' } = req.query;
        
        let currentLeads, currentSales, previousLeads, previousSales;
        
        if (type === 'day') {
            // Today vs Yesterday - use specific dates
            // Today: from start of today until now
            currentLeads = await pool.query(`
                SELECT COUNT(*) FROM leads 
                WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
            `);
            
            currentSales = await pool.query(`
                SELECT COUNT(*), COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
                FROM transactions 
                WHERE status = 'approved' 
                AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
            `);
            
            // Yesterday: full day
            previousLeads = await pool.query(`
                SELECT COUNT(*) FROM leads 
                WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '1 day')::date
            `);
            
            previousSales = await pool.query(`
                SELECT COUNT(*), COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
                FROM transactions 
                WHERE status = 'approved' 
                AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '1 day')::date
            `);
        } else {
            // Week, month, quarter - use intervals
            let currentInterval, previousInterval;
            switch (type) {
                case 'month':
                    currentInterval = '30 days';
                    previousInterval = '60 days';
                    break;
                case 'quarter':
                    currentInterval = '90 days';
                    previousInterval = '180 days';
                    break;
                case 'week':
                default:
                    currentInterval = '7 days';
                    previousInterval = '14 days';
                    break;
            }
            
            // Current period stats
            currentLeads = await pool.query(`
                SELECT COUNT(*) FROM leads 
                WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${currentInterval}')::date
            `);
            
            currentSales = await pool.query(`
                SELECT COUNT(*), COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
                FROM transactions 
                WHERE status = 'approved' 
                AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${currentInterval}')::date
            `);
            
            // Previous period stats
            previousLeads = await pool.query(`
                SELECT COUNT(*) FROM leads 
                WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${previousInterval}')::date 
                AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date < ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${currentInterval}')::date
            `);
            
            previousSales = await pool.query(`
                SELECT COUNT(*), COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
                FROM transactions 
                WHERE status = 'approved' 
                AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${previousInterval}')::date
                AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date < ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${currentInterval}')::date
            `);
        }
        
        res.json({
            periodType: type,
            current: {
                leads: parseInt(currentLeads.rows[0].count),
                sales: parseInt(currentSales.rows[0].count),
                revenue: parseFloat(currentSales.rows[0].revenue) || 0
            },
            previous: {
                leads: parseInt(previousLeads.rows[0].count),
                sales: parseInt(previousSales.rows[0].count),
                revenue: parseFloat(previousSales.rows[0].revenue) || 0
            }
        });
        
    } catch (error) {
        console.error('Error fetching period comparison:', error);
        res.status(500).json({ error: 'Failed to fetch period comparison' });
    }
});

// Get heatmap data by type (leads, sales, revenue)
app.get('/api/admin/stats/heatmap', authenticateToken, async (req, res) => {
    try {
        const { type = 'leads' } = req.query;
        
        let query;
        if (type === 'leads') {
            query = `
                SELECT 
                    EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/Sao_Paulo') as hour,
                    EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo') as day_of_week,
                    COUNT(*) as count
                FROM leads
                WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '30 days')::date
                GROUP BY EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/Sao_Paulo'), EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo')
                ORDER BY day_of_week, hour
            `;
        } else if (type === 'sales') {
            query = `
                SELECT 
                    EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/Sao_Paulo') as hour,
                    EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo') as day_of_week,
                    COUNT(*) as count
                FROM transactions
                WHERE status = 'approved' 
                AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '30 days')::date
                GROUP BY EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/Sao_Paulo'), EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo')
                ORDER BY day_of_week, hour
            `;
        } else if (type === 'revenue') {
            query = `
                SELECT 
                    EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/Sao_Paulo') as hour,
                    EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo') as day_of_week,
                    COALESCE(SUM(CAST(value AS DECIMAL)), 0) as value,
                    COUNT(*) as count
                FROM transactions
                WHERE status = 'approved' 
                AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '30 days')::date
                GROUP BY EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/Sao_Paulo'), EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo')
                ORDER BY day_of_week, hour
            `;
        }
        
        const result = await pool.query(query);
        res.json({ type, hourlyHeatmap: result.rows });
        
    } catch (error) {
        console.error('Error fetching heatmap data:', error);
        res.status(500).json({ error: 'Failed to fetch heatmap data' });
    }
});

// Get top countries by sales
app.get('/api/admin/stats/countries-sales', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                COALESCE(l.country, 'Unknown') as country_code,
                COALESCE(l.country, 'Desconhecido') as country_name,
                COUNT(t.id) as sales,
                COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) as revenue
            FROM transactions t
            LEFT JOIN leads l ON t.email = l.email
            WHERE t.status = 'approved'
            AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '30 days')::date
            GROUP BY l.country
            ORDER BY sales DESC, revenue DESC
            LIMIT 5
        `);
        
        res.json({ countries: result.rows });
        
    } catch (error) {
        console.error('Error fetching countries sales:', error);
        res.status(500).json({ error: 'Failed to fetch countries sales' });
    }
});

// Get traffic sources from UTM and referrer data
app.get('/api/admin/stats/traffic-sources', authenticateToken, async (req, res) => {
    try {
        // Check cache first (TTL: 5 min)
        const cacheKey = 'traffic-sources';
        const cached = getCached(cacheKey, 5 * 60 * 1000);
        if (cached) return res.json(cached);

        const result = await pool.query(`
            SELECT 
                CASE 
                    WHEN utm_source IS NOT NULL AND utm_source != '' AND LOWER(utm_source) LIKE '%fb%' THEN 'Facebook Ads'
                    WHEN utm_source IS NOT NULL AND utm_source != '' AND LOWER(utm_source) LIKE '%facebook%' THEN 'Facebook Ads'
                    WHEN utm_source IS NOT NULL AND utm_source != '' AND LOWER(utm_source) LIKE '%ig%' THEN 'Instagram Ads'
                    WHEN utm_source IS NOT NULL AND utm_source != '' AND LOWER(utm_source) LIKE '%google%' THEN 'Google Ads'
                    WHEN utm_source IS NOT NULL AND utm_source != '' AND LOWER(utm_source) LIKE '%tiktok%' THEN 'TikTok Ads'
                    WHEN utm_source IS NOT NULL AND utm_source != '' AND LOWER(utm_source) LIKE '%youtube%' THEN 'YouTube'
                    WHEN utm_source IS NOT NULL AND utm_source != '' THEN INITCAP(utm_source)
                    WHEN referrer IS NOT NULL AND referrer != '' AND referrer LIKE '%facebook%' THEN 'Facebook'
                    WHEN referrer IS NOT NULL AND referrer != '' AND referrer LIKE '%google%' THEN 'Google Organico'
                    WHEN referrer IS NOT NULL AND referrer != '' AND referrer LIKE '%instagram%' THEN 'Instagram'
                    WHEN referrer IS NOT NULL AND referrer != '' AND referrer LIKE '%tiktok%' THEN 'TikTok'
                    WHEN referrer IS NULL OR referrer = '' THEN 'Trafego Direto'
                    ELSE 'Outro'
                END as source,
                COUNT(*) as count
            FROM leads
            WHERE created_at >= NOW() - INTERVAL '30 days'
            GROUP BY source
            ORDER BY count DESC
            LIMIT 8
        `);

        const response = { sources: result.rows.map(r => ({ source: r.source, count: parseInt(r.count) })) };
        setCache(cacheKey, response);
        res.json(response);
    } catch (error) {
        console.error('Error fetching traffic sources:', error);
        res.status(500).json({ error: 'Failed to fetch traffic sources' });
    }
});

// Get weekly performance data
app.get('/api/admin/stats/weekly-performance', authenticateToken, async (req, res) => {
    try {
        const dayNames = ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'];
        
        // Get leads by day of week (last 4 weeks)
        const leadsResult = await pool.query(`
            SELECT 
                EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo') as day_of_week,
                COUNT(*) as count
            FROM leads
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '28 days')::date
            GROUP BY EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo')
            ORDER BY day_of_week
        `);
        
        // Get sales by day of week (last 4 weeks)
        const salesResult = await pool.query(`
            SELECT 
                EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo') as day_of_week,
                COUNT(*) as count
            FROM transactions
            WHERE status = 'approved'
            AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '28 days')::date
            GROUP BY EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo')
            ORDER BY day_of_week
        `);
        
        // Build arrays starting from Monday (1) to Sunday (0)
        const leadsMap = {};
        const salesMap = {};
        
        leadsResult.rows.forEach(r => { leadsMap[r.day_of_week] = parseInt(r.count); });
        salesResult.rows.forEach(r => { salesMap[r.day_of_week] = parseInt(r.count); });
        
        // Reorder: Mon(1), Tue(2), Wed(3), Thu(4), Fri(5), Sat(6), Sun(0)
        const order = [1, 2, 3, 4, 5, 6, 0];
        const labels = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'];
        const leads = order.map(d => leadsMap[d] || 0);
        const sales = order.map(d => salesMap[d] || 0);
        
        res.json({ labels, leads, sales });
        
    } catch (error) {
        console.error('Error fetching weekly performance:', error);
        res.status(500).json({ error: 'Failed to fetch weekly performance' });
    }
});

// Funnel stats for conversion funnel chart
app.get('/api/admin/funnel-stats', authenticateToken, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        let dateFilter = '';
        let dateFilterTx = '';
        const params = [];
        
        if (startDate && endDate) {
            dateFilter = ` AND created_at >= $1::date AND created_at < $2::date + INTERVAL '1 day'`;
            dateFilterTx = ` AND created_at >= $1::date AND created_at < $2::date + INTERVAL '1 day'`;
            params.push(startDate, endDate);
        }
        
        // Total leads (visitors)
        const leadsRes = await pool.query(`SELECT COUNT(*) as count FROM leads WHERE 1=1${dateFilter}`, params);
        const totalLeads = parseInt(leadsRes.rows[0]?.count || 0);
        
        // Leads that reached checkout
        const checkoutRes = await pool.query(`
            SELECT COUNT(DISTINCT visitor_id) as count FROM funnel_events 
            WHERE event = 'checkout_clicked'${dateFilter}
        `, params);
        const checkouts = parseInt(checkoutRes.rows[0]?.count || 0);
        
        // Total approved sales
        const salesRes = await pool.query(`SELECT COUNT(*) as count FROM transactions WHERE status = 'approved'${dateFilterTx}`, params);
        const totalSales = parseInt(salesRes.rows[0]?.count || 0);
        
        // Visitors (page views - people who entered the funnel)
        const visitorsRes = await pool.query(`
            SELECT COUNT(DISTINCT visitor_id) as count FROM funnel_events 
            WHERE event IN ('page_view', 'landing_visit')${dateFilter}
        `, params);
        const visitors = parseInt(visitorsRes.rows[0]?.count || 0);
        
        res.json({
            visitors: Math.max(visitors, totalLeads),
            leads: totalLeads,
            checkouts,
            sales: totalSales,
            totalLeads,
            totalSales
        });
    } catch (error) {
        console.error('Error fetching funnel stats:', error);
        res.status(500).json({ error: 'Failed to fetch funnel stats' });
    }
});

// ==================== WHATSAPP Z-API INTEGRATION ====================
// Z-API credentials - use env vars or fallback to defaults
const ZAPI_INSTANCE = process.env.ZAPI_INSTANCE_ID || '3EEA70039B0B31BFC5924A7638EE86FD';
const ZAPI_TOKEN = process.env.ZAPI_TOKEN || '448359FB9C302BCE9D09F8D0';
const ZAPI_CLIENT_TOKEN = process.env.ZAPI_CLIENT_TOKEN || 'F221fdbff74cc4d998046c12b9e0a65ddS';
const ZAPI_BASE_URL = `https://api.z-api.io/instances/${ZAPI_INSTANCE}/token/${ZAPI_TOKEN}`;

// Log Z-API config on startup (masked for security)
console.log(`📱 Z-API Config: Instance=${ZAPI_INSTANCE.substring(0,8)}..., Token=${ZAPI_TOKEN.substring(0,8)}..., ClientToken=${ZAPI_CLIENT_TOKEN ? ZAPI_CLIENT_TOKEN.substring(0,8) + '...' : 'NOT SET'}, URL=${ZAPI_BASE_URL}`);

// ---- Z-API Diagnostic endpoint ----
app.get('/api/admin/whatsapp/diagnostics', authenticateToken, async (req, res) => {
    const results = {
        config: {
            instanceId: ZAPI_INSTANCE,
            instanceIdLength: ZAPI_INSTANCE.length,
            instanceIdSource: process.env.ZAPI_INSTANCE_ID ? 'ENV_VAR' : 'FALLBACK_DEFAULT',
            token: ZAPI_TOKEN.substring(0, 6) + '***' + ZAPI_TOKEN.slice(-4),
            tokenLength: ZAPI_TOKEN.length,
            tokenSource: process.env.ZAPI_TOKEN ? 'ENV_VAR' : 'FALLBACK_DEFAULT',
            clientToken: ZAPI_CLIENT_TOKEN ? ZAPI_CLIENT_TOKEN.substring(0, 6) + '***' + ZAPI_CLIENT_TOKEN.slice(-4) : 'NOT SET',
            clientTokenLength: ZAPI_CLIENT_TOKEN.length,
            clientTokenSource: process.env.ZAPI_CLIENT_TOKEN ? 'ENV_VAR' : 'FALLBACK_DEFAULT',
            baseUrl: ZAPI_BASE_URL
        },
        tests: {}
    };

    // Test 1: Status without Client-Token
    try {
        const resp1 = await fetch(`${ZAPI_BASE_URL}/status`, { method: 'GET' });
        const text1 = await resp1.text();
        results.tests.statusWithoutClientToken = {
            httpStatus: resp1.status,
            response: text1.substring(0, 500)
        };
    } catch (e) {
        results.tests.statusWithoutClientToken = { error: e.message };
    }

    // Test 2: Status with Client-Token
    try {
        const headers2 = {};
        if (ZAPI_CLIENT_TOKEN) headers2['Client-Token'] = ZAPI_CLIENT_TOKEN;
        const resp2 = await fetch(`${ZAPI_BASE_URL}/status`, { method: 'GET', headers: headers2 });
        const text2 = await resp2.text();
        results.tests.statusWithClientToken = {
            httpStatus: resp2.status,
            headers: Object.fromEntries(Object.entries(headers2).map(([k,v]) => [k, k === 'Client-Token' ? v.substring(0,6) + '***' : v])),
            response: text2.substring(0, 500)
        };
    } catch (e) {
        results.tests.statusWithClientToken = { error: e.message };
    }

    // Test 3: Try phone-exists with a known number
    try {
        const headers3 = {};
        if (ZAPI_CLIENT_TOKEN) headers3['Client-Token'] = ZAPI_CLIENT_TOKEN;
        const resp3 = await fetch(`${ZAPI_BASE_URL}/phone-exists/5511999999999`, { method: 'GET', headers: headers3 });
        const text3 = await resp3.text();
        results.tests.phoneExistsTest = {
            httpStatus: resp3.status,
            response: text3.substring(0, 500)
        };
    } catch (e) {
        results.tests.phoneExistsTest = { error: e.message };
    }

    res.json(results);
});

// ---- Z-API Custom URL Test ----
app.post('/api/admin/whatsapp/test-url', authenticateToken, async (req, res) => {
    try {
        const { url, clientToken } = req.body;
        if (!url) return res.status(400).json({ error: 'URL is required' });
        
        // Extract base URL (remove /send-text or other endpoints)
        let baseUrl = url.replace(/\/(send-text|send-message-text|status|phone-exists\/\d+)\/?$/, '');
        
        const results = {};
        
        // Test status endpoint
        const statusUrl = `${baseUrl}/status`;
        const headers = {};
        if (clientToken) headers['Client-Token'] = clientToken;
        
        console.log(`📱 Custom URL test: ${statusUrl}`);
        console.log(`📱 Client-Token: ${clientToken ? 'SET' : 'NOT SET'}`);
        
        try {
            const resp = await fetch(statusUrl, { method: 'GET', headers });
            const text = await resp.text();
            results.statusTest = { url: statusUrl, httpStatus: resp.status, response: text.substring(0, 500) };
        } catch (e) {
            results.statusTest = { url: statusUrl, error: e.message };
        }
        
        // Test phone-exists
        const phoneUrl = `${baseUrl}/phone-exists/5511999999999`;
        try {
            const resp2 = await fetch(phoneUrl, { method: 'GET', headers });
            const text2 = await resp2.text();
            results.phoneTest = { url: phoneUrl, httpStatus: resp2.status, response: text2.substring(0, 500) };
        } catch (e) {
            results.phoneTest = { url: phoneUrl, error: e.message };
        }
        
        // Compare with our configured URL
        results.comparison = {
            yourUrl: baseUrl,
            ourUrl: ZAPI_BASE_URL,
            match: baseUrl === ZAPI_BASE_URL
        };
        
        res.json(results);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ---- Send WhatsApp message via Z-API ----
app.post('/api/admin/whatsapp/send', authenticateToken, async (req, res) => {
    try {
        const { phone, message } = req.body;
        
        if (!phone || !message) {
            return res.status(400).json({ error: 'Phone and message are required' });
        }
        
        // Clean phone number - remove all non-digits
        const cleanPhone = phone.replace(/\D/g, '');
        
        if (cleanPhone.length < 10) {
            return res.status(400).json({ error: 'Invalid phone number' });
        }
        
        // Send via Z-API send-text
        const headers = { 'Content-Type': 'application/json' };
        if (ZAPI_CLIENT_TOKEN) headers['Client-Token'] = ZAPI_CLIENT_TOKEN;
        
        const response = await fetch(`${ZAPI_BASE_URL}/send-text`, {
            method: 'POST',
            headers,
            body: JSON.stringify({
                phone: cleanPhone,
                message: message,
                delayMessage: 3
            })
        });
        
        const data = await response.json();
        
        if (response.ok && data.messageId) {
            // Log the message in database
            try {
                await pool.query(`
                    INSERT INTO whatsapp_messages (phone, message, message_id, zaap_id, status, sent_by, created_at)
                    VALUES ($1, $2, $3, $4, 'sent', $5, NOW())
                `, [cleanPhone, message, data.messageId, data.zaapId, req.user?.email || 'admin']);
            } catch (dbError) {
                // Table might not exist yet, ignore
                console.log('WhatsApp message log skipped (table may not exist):', dbError.message);
            }
            
            res.json({ 
                success: true, 
                messageId: data.messageId,
                zaapId: data.zaapId
            });
        } else {
            console.error('Z-API send error:', data);
            res.status(response.status || 500).json({ 
                error: 'Failed to send WhatsApp message', 
                details: data.error || data.message || 'Unknown error'
            });
        }
        
    } catch (error) {
        console.error('Error sending WhatsApp:', error);
        res.status(500).json({ error: 'Failed to send WhatsApp message', details: error.message });
    }
});

// ---- Check Z-API instance status ----
app.get('/api/admin/whatsapp/status', authenticateToken, async (req, res) => {
    try {
        const headers = {};
        if (ZAPI_CLIENT_TOKEN) headers['Client-Token'] = ZAPI_CLIENT_TOKEN;
        
        const statusUrl = `${ZAPI_BASE_URL}/status`;
        console.log(`📱 Checking Z-API status: ${statusUrl}`);
        console.log(`📱 Client-Token set: ${ZAPI_CLIENT_TOKEN ? 'YES (***' + ZAPI_CLIENT_TOKEN.slice(-4) + ')' : 'NO'}`);
        
        const response = await fetch(statusUrl, { method: 'GET', headers });
        const responseText = await response.text();
        console.log(`📱 Z-API status response: HTTP ${response.status} → ${responseText}`);
        
        let data;
        try {
            data = JSON.parse(responseText);
        } catch(e) {
            return res.status(500).json({ error: 'Invalid Z-API response', raw: responseText.substring(0, 500) });
        }
        
        if (!response.ok) {
            return res.status(response.status).json({ 
                error: data.message || data.error || `Z-API error ${response.status}`,
                zapiStatus: response.status,
                zapiResponse: data,
                configUsed: {
                    instanceId: ZAPI_INSTANCE.substring(0,8) + '...',
                    token: ZAPI_TOKEN.substring(0,8) + '...',
                    clientToken: ZAPI_CLIENT_TOKEN ? '***' + ZAPI_CLIENT_TOKEN.slice(-4) : 'NOT SET',
                    baseUrl: ZAPI_BASE_URL
                }
            });
        }
        
        res.json({
            connected: data.connected || false,
            smartphoneConnected: data.smartphoneConnected || false,
            session: data.session || false,
            ...data
        });
    } catch (error) {
        console.error('Error checking WhatsApp status:', error);
        res.status(500).json({ error: 'Failed to check WhatsApp status', details: error.message });
    }
});

// Verify a single WhatsApp number
app.post('/api/admin/leads/:id/verify-whatsapp', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        // Get lead's phone number
        const leadResult = await pool.query('SELECT * FROM leads WHERE id = $1', [id]);
        if (leadResult.rows.length === 0) {
            return res.status(404).json({ error: 'Lead not found' });
        }
        
        const lead = leadResult.rows[0];
        let phone = lead.whatsapp || '';
        
        if (!phone) {
            return res.status(400).json({ error: 'No phone number available', verified: false });
        }
        
        // Clean phone number - remove all non-digits
        phone = phone.replace(/\D/g, '');
        
        // Check if number exists on WhatsApp via Z-API
        try {
            const zapiHeaders = {};
            if (ZAPI_CLIENT_TOKEN) zapiHeaders['Client-Token'] = ZAPI_CLIENT_TOKEN;
            
            const fullUrl = `${ZAPI_BASE_URL}/phone-exists/${phone}`;
            console.log(`📱 Verifying WhatsApp: ${phone} → ${fullUrl}`);
            console.log(`📱 Headers:`, JSON.stringify({ ...zapiHeaders, 'Client-Token': zapiHeaders['Client-Token'] ? '***' + zapiHeaders['Client-Token'].slice(-4) : 'NOT SET' }));
            
            const response = await fetch(fullUrl, {
                method: 'GET',
                headers: zapiHeaders
            });
            
            const responseText = await response.text();
            console.log(`📱 Z-API raw response for ${phone}: HTTP ${response.status} → ${responseText}`);
            
            let data;
            try {
                data = JSON.parse(responseText);
            } catch(e) {
                console.error(`📱 Z-API response is not JSON: ${responseText}`);
                return res.status(500).json({ 
                    error: 'Resposta inválida da Z-API', 
                    details: responseText.substring(0, 200),
                    verified: false 
                });
            }
            
            // Check for API errors first
            if (!response.ok) {
                console.error(`📱 Z-API HTTP error ${response.status} for ${phone}:`, data);
                return res.status(500).json({ 
                    error: data.message || data.error || `Z-API retornou erro ${response.status}`, 
                    details: JSON.stringify(data),
                    verified: false 
                });
            }
            
            const isRegistered = data.exists === true;
            
            // Try to get profile picture if registered
            let profilePicture = null;
            if (isRegistered) {
                try {
                    const picResponse = await fetch(`${ZAPI_BASE_URL}/profile-picture?phone=${phone}`, {
                        headers: zapiHeaders
                    });
                    const picData = await picResponse.json();
                    if (picData.link && picData.link !== 'null' && picData.link.startsWith('http')) {
                        profilePicture = picData.link;
                    }
                } catch (e) {
                    console.log('Profile picture fetch error:', e);
                }
            }
            
            // Update lead in database
            await pool.query(`
                UPDATE leads SET 
                    whatsapp_verified = $1,
                    whatsapp_verified_at = NOW(),
                    whatsapp_profile_pic = $2,
                    updated_at = NOW()
                WHERE id = $3
            `, [isRegistered, profilePicture, id]);
            
            res.json({ 
                success: true, 
                verified: isRegistered, 
                profilePicture,
                phone
            });
            
        } catch (apiError) {
            console.error('Z-API error:', apiError);
            res.status(500).json({ error: 'WhatsApp API error', details: apiError.message });
        }
        
    } catch (error) {
        console.error('Error verifying WhatsApp:', error);
        res.status(500).json({ error: 'Failed to verify WhatsApp' });
    }
});

// Bulk verify multiple leads (with rate limiting)
app.post('/api/admin/leads/bulk-verify-whatsapp', authenticateToken, async (req, res) => {
    try {
        const { leadIds } = req.body;
        
        if (!leadIds || !Array.isArray(leadIds) || leadIds.length === 0) {
            return res.status(400).json({ error: 'No lead IDs provided' });
        }
        
        // Limit to 20 at a time to avoid rate limiting
        const limitedIds = leadIds.slice(0, 20);
        
        // Get leads
        const leadsResult = await pool.query(
            'SELECT id, whatsapp FROM leads WHERE id = ANY($1)',
            [limitedIds]
        );
        
        const results = [];
        
        for (const lead of leadsResult.rows) {
            let phone = lead.whatsapp || '';
            if (!phone) {
                results.push({ id: lead.id, verified: false, error: 'No phone' });
                continue;
            }
            
            phone = phone.replace(/\D/g, '');
            
            try {
                const bvHeaders = {};
                if (ZAPI_CLIENT_TOKEN) bvHeaders['Client-Token'] = ZAPI_CLIENT_TOKEN;
                const response = await fetch(`${ZAPI_BASE_URL}/phone-exists/${phone}`, {
                    headers: bvHeaders
                });
                
                const data = await response.json();
                const isRegistered = data.exists === true;
                
                // Update lead
                await pool.query(`
                    UPDATE leads SET 
                        whatsapp_verified = $1,
                        whatsapp_verified_at = NOW(),
                        updated_at = NOW()
                    WHERE id = $2
                `, [isRegistered, lead.id]);
                
                results.push({ id: lead.id, verified: isRegistered });
                
                // Rate limit: wait 500ms between requests
                await new Promise(resolve => setTimeout(resolve, 500));
                
            } catch (e) {
                results.push({ id: lead.id, verified: false, error: e.message });
            }
        }
        
        res.json({ 
            success: true, 
            results,
            verified: results.filter(r => r.verified).length,
            failed: results.filter(r => !r.verified).length
        });
        
    } catch (error) {
        console.error('Error bulk verifying WhatsApp:', error);
        res.status(500).json({ error: 'Failed to bulk verify WhatsApp' });
    }
});

// In-memory job tracker for verify-all
let verifyAllJob = null;

// Start verify ALL leads WhatsApp numbers
app.post('/api/admin/leads/verify-all-whatsapp', authenticateToken, async (req, res) => {
    if (verifyAllJob && verifyAllJob.status === 'running') {
        return res.status(409).json({ error: 'Job already running', job: verifyAllJob });
    }

    try {
        const leadsResult = await pool.query(
            `SELECT id, whatsapp FROM leads 
             WHERE whatsapp IS NOT NULL AND whatsapp != ''
             ORDER BY id ASC`
        );

        const totalLeads = leadsResult.rows.length;

        verifyAllJob = {
            status: 'running',
            total: totalLeads,
            processed: 0,
            verified: 0,
            invalid: 0,
            errors: 0,
            skipped: 0,
            percent: 0,
            startedAt: Date.now(),
            message: `Verificando ${totalLeads} leads...`
        };

        res.json({ success: true, job: verifyAllJob });

        // Run in background
        (async () => {
            const zapiHeaders = {};
            if (ZAPI_CLIENT_TOKEN) zapiHeaders['Client-Token'] = ZAPI_CLIENT_TOKEN;

            for (const lead of leadsResult.rows) {
                let phone = lead.whatsapp || '';
                if (!phone || phone.replace(/\D/g, '').length < 10) {
                    verifyAllJob.skipped++;
                    verifyAllJob.processed++;
                    verifyAllJob.percent = Math.round((verifyAllJob.processed / totalLeads) * 100);
                    continue;
                }

                phone = phone.replace(/\D/g, '');

                try {
                    const response = await fetch(`${ZAPI_BASE_URL}/phone-exists/${phone}`, {
                        method: 'GET',
                        headers: zapiHeaders
                    });

                    const data = await response.json();
                    const isRegistered = data.exists === true;

                    let profilePicture = null;
                    if (isRegistered) {
                        try {
                            const picResponse = await fetch(`${ZAPI_BASE_URL}/profile-picture?phone=${phone}`, {
                                method: 'GET',
                                headers: zapiHeaders
                            });
                            const picData = await picResponse.json();
                            if (picData.link && picData.link !== 'null' && picData.link.startsWith('http')) {
                                profilePicture = picData.link;
                            }
                        } catch (e) { /* ignore */ }
                    }

                    await pool.query(`
                        UPDATE leads SET 
                            whatsapp_verified = $1,
                            whatsapp_verified_at = NOW(),
                            whatsapp_profile_pic = COALESCE($2, whatsapp_profile_pic),
                            updated_at = NOW()
                        WHERE id = $3
                    `, [isRegistered, profilePicture, lead.id]);

                    if (isRegistered) verifyAllJob.verified++;
                    else verifyAllJob.invalid++;

                } catch (e) {
                    verifyAllJob.errors++;
                    console.log(`📱 Verify-all error for lead #${lead.id}: ${e.message}`);
                }

                verifyAllJob.processed++;
                verifyAllJob.percent = Math.round((verifyAllJob.processed / totalLeads) * 100);

                if (verifyAllJob.processed % 10 === 0) {
                    console.log(`📱 Verify-all progress: ${verifyAllJob.processed}/${totalLeads} (${verifyAllJob.percent}%) - ✓${verifyAllJob.verified} ✕${verifyAllJob.invalid} ⚠${verifyAllJob.errors}`);
                }

                await new Promise(resolve => setTimeout(resolve, 600));
            }

            verifyAllJob.status = 'complete';
            verifyAllJob.message = `Concluído! ${verifyAllJob.verified} válidos, ${verifyAllJob.invalid} inválidos, ${verifyAllJob.errors} erros, ${verifyAllJob.skipped} ignorados`;
            verifyAllJob.completedAt = Date.now();
            console.log(`📱 Verify-all COMPLETE: ${verifyAllJob.message}`);
        })().catch(err => {
            verifyAllJob.status = 'error';
            verifyAllJob.message = err.message;
            console.error('📱 Verify-all FAILED:', err);
        });

    } catch (error) {
        console.error('Error starting verify-all:', error);
        res.status(500).json({ error: error.message });
    }
});

// Poll verify-all job status
app.get('/api/admin/leads/verify-all-status', authenticateToken, (req, res) => {
    if (!verifyAllJob) {
        return res.json({ status: 'idle', message: 'Nenhum job em andamento' });
    }
    res.json(verifyAllJob);
});

// Get single lead by ID (protected)
app.get('/api/admin/leads/:id(\\d+)', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM leads WHERE id = $1', [req.params.id]);
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Lead not found' });
        }
        res.json(result.rows[0]);
    } catch (error) {
        console.error('Error fetching lead:', error);
        res.status(500).json({ error: 'Failed to fetch lead' });
    }
});

// Update lead status (protected)
app.put('/api/admin/leads/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { status, notes } = req.body;
        
        const result = await pool.query(
            `UPDATE leads SET status = $1, notes = $2, updated_at = NOW() WHERE id = $3 RETURNING *`,
            [status, notes, id]
        );
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Lead not found' });
        }
        
        res.json({ success: true, lead: result.rows[0] });
        
    } catch (error) {
        console.error('Error updating lead:', error);
        res.status(500).json({ error: 'Failed to update lead' });
    }
});

// Delete lead (protected)
app.delete('/api/admin/leads/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        const result = await pool.query('DELETE FROM leads WHERE id = $1 RETURNING id', [id]);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Lead not found' });
        }
        
        res.json({ success: true, message: 'Lead deleted' });
        
    } catch (error) {
        console.error('Error deleting lead:', error);
        res.status(500).json({ error: 'Failed to delete lead' });
    }
});

// Clear all test data (protected) - USE WITH CAUTION
app.delete('/api/admin/clear-all-data', authenticateToken, async (req, res) => {
    try {
        const { confirm } = req.query;
        
        if (confirm !== 'yes-delete-everything') {
            return res.status(400).json({ 
                error: 'Confirmation required', 
                message: 'Add ?confirm=yes-delete-everything to confirm deletion' 
            });
        }
        
        // Delete all data from tables
        const leadsResult = await pool.query('DELETE FROM leads RETURNING id');
        const eventsResult = await pool.query('DELETE FROM funnel_events RETURNING id');
        const transactionsResult = await pool.query('DELETE FROM transactions RETURNING id');
        const refundsResult = await pool.query('DELETE FROM refund_requests RETURNING id');
        
        console.log('⚠️ ALL DATA CLEARED BY ADMIN');
        
        res.json({ 
            success: true, 
            message: 'All data cleared',
            deleted: {
                leads: leadsResult.rowCount,
                funnel_events: eventsResult.rowCount,
                transactions: transactionsResult.rowCount,
                refund_requests: refundsResult.rowCount
            }
        });
        
    } catch (error) {
        console.error('Error clearing data:', error);
        res.status(500).json({ error: 'Failed to clear data' });
    }
});

// Enrich geolocation data for existing leads (protected)
app.post('/api/admin/enrich-geolocation', authenticateToken, async (req, res) => {
    try {
        // Get all leads with IP but without country data
        const leadsToEnrich = await pool.query(
            `SELECT id, ip_address FROM leads 
             WHERE ip_address IS NOT NULL 
             AND ip_address != '' 
             AND (country IS NULL OR country = '' OR country_code IS NULL OR country_code = 'XX' OR country_code = '')
             LIMIT 50`
        );
        
        if (leadsToEnrich.rows.length === 0) {
            return res.json({ 
                success: true, 
                message: 'No leads need geolocation enrichment',
                enriched: 0,
                remaining: 0
            });
        }
        
        console.log(`Enriching geolocation for ${leadsToEnrich.rows.length} leads...`);
        
        let enrichedCount = 0;
        let errors = 0;
        
        // Process each lead (with delay to avoid rate limiting)
        for (const lead of leadsToEnrich.rows) {
            try {
                const geoData = await getCountryFromIP(lead.ip_address);
                
                if (geoData.country && geoData.country_code) {
                    await pool.query(
                        `UPDATE leads SET 
                            country = $1, 
                            country_code = $2, 
                            city = $3, 
                            updated_at = NOW() 
                         WHERE id = $4`,
                        [geoData.country, geoData.country_code, geoData.city, lead.id]
                    );
                    enrichedCount++;
                    console.log(`Enriched lead ${lead.id}: ${geoData.country} (${geoData.country_code})`);
                } else {
                    console.log(`Could not get geo data for lead ${lead.id} (IP: ${lead.ip_address})`);
                }
                
                // Small delay to avoid rate limiting
                await new Promise(resolve => setTimeout(resolve, 200));
                
            } catch (err) {
                console.error(`Error enriching lead ${lead.id}:`, err.message);
                errors++;
            }
        }
        
        // Count remaining leads needing enrichment
        const remainingResult = await pool.query(
            `SELECT COUNT(*) as count FROM leads 
             WHERE ip_address IS NOT NULL 
             AND ip_address != '' 
             AND (country IS NULL OR country = '' OR country_code IS NULL OR country_code = 'XX' OR country_code = '')`
        );
        
        res.json({ 
            success: true, 
            message: `Enriched ${enrichedCount} leads`,
            enriched: enrichedCount,
            errors: errors,
            remaining: parseInt(remainingResult.rows[0].count)
        });
        
    } catch (error) {
        console.error('Error enriching geolocation:', error);
        res.status(500).json({ error: 'Failed to enrich geolocation data' });
    }
});

// Manually add a transaction (for when postback didn't arrive)
app.post('/api/admin/transactions/manual', authenticateToken, async (req, res) => {
    try {
        const { transaction_id, email, phone, name, product, value, status, funnel_language } = req.body;
        
        if (!transaction_id || !email || !product || !value) {
            return res.status(400).json({ error: 'transaction_id, email, product, and value are required' });
        }
        
        // Insert transaction
        await pool.query(`
            INSERT INTO transactions (
                transaction_id, email, phone, name, product, value, 
                monetizze_status, status, funnel_language, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, '2', $7, $8, NOW())
            ON CONFLICT (transaction_id) 
            DO UPDATE SET 
                status = $7,
                funnel_language = $8,
                updated_at = NOW()
        `, [
            transaction_id,
            email,
            phone || null,
            name || null,
            product,
            value,
            status || 'approved',
            funnel_language || 'en'
        ]);
        
        // Try to update lead with full purchase info
        if (email) {
            const purchaseValue = parseFloat(value) || 0;
            const productIdentifier = product.substring(0, 50);
            
            await pool.query(`
                UPDATE leads SET 
                    status = 'converted',
                    products_purchased = CASE 
                        WHEN products_purchased IS NULL THEN ARRAY[$2]::TEXT[]
                        WHEN NOT ($2 = ANY(products_purchased)) THEN array_append(products_purchased, $2)
                        ELSE products_purchased
                    END,
                    total_spent = COALESCE(total_spent, 0) + $3,
                    first_purchase_at = CASE 
                        WHEN first_purchase_at IS NULL THEN NOW()
                        ELSE first_purchase_at
                    END,
                    last_purchase_at = NOW(),
                    updated_at = NOW()
                WHERE LOWER(email) = LOWER($1)
            `, [email, productIdentifier, purchaseValue]);
        }
        
        console.log(`✅ Manual transaction added: ${transaction_id} - ${product} - R$${value}`);
        
        res.json({ success: true, message: 'Transaction added successfully' });
    } catch (error) {
        console.error('Error adding manual transaction:', error);
        res.status(500).json({ error: error.message });
    }
});

// Clean test transactions (protected - admin only)
app.delete('/api/admin/transactions/test', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const result = await pool.query(`
            DELETE FROM transactions 
            WHERE transaction_id LIKE 'TEST%' 
               OR transaction_id LIKE '%TEST%'
               OR email LIKE '%test%@%' 
               OR email LIKE '%@test.%'
               OR product LIKE '%TEST%'
               OR product = 'DELETE'
            RETURNING transaction_id, email, product
        `);
        
        console.log(`🗑️ Deleted ${result.rowCount} test transactions`);
        
        res.json({
            success: true,
            deleted: result.rowCount,
            transactions: result.rows
        });
        
    } catch (error) {
        console.error('Error deleting test transactions:', error);
        res.status(500).json({ error: 'Failed to delete test transactions' });
    }
});

// Delete ALL transactions (for reset and resync)
app.delete('/api/admin/transactions/all', authenticateToken, requireAdmin, async (req, res) => {
    try {
        // Count before delete
        const countResult = await pool.query('SELECT COUNT(*) FROM transactions');
        const count = parseInt(countResult.rows[0].count);
        
        // Delete all
        await pool.query('DELETE FROM transactions');
        
        console.log(`🗑️ Deleted ALL ${count} transactions for resync`);
        
        res.json({
            success: true,
            deleted: count,
            message: 'All transactions deleted. Ready for resync.'
        });
        
    } catch (error) {
        console.error('Error deleting all transactions:', error);
        res.status(500).json({ error: 'Failed to delete all transactions' });
    }
});

// Migrate existing transactions to set funnel_source based on product codes
app.post('/api/admin/transactions/migrate-source', authenticateToken, requireAdmin, async (req, res) => {
    try {
        // Affiliate product codes
        const affiliateProductCodes = [
            '330254', '341443', '341444', '341448',  // English Affiliates
            '338375', '341452', '341453', '341454'   // Spanish Affiliates
        ];
        
        // Update transactions that match affiliate product codes in raw_data
        let updated = 0;
        
        // Method 1: Check raw_data for produto.codigo
        for (const code of affiliateProductCodes) {
            const result = await pool.query(`
                UPDATE transactions 
                SET funnel_source = 'affiliate'
                WHERE funnel_source IS NULL OR funnel_source = 'main'
                AND raw_data::text LIKE $1
            `, [`%"codigo":"${code}"%`]);
            updated += result.rowCount;
            
            // Also try numeric format
            const result2 = await pool.query(`
                UPDATE transactions 
                SET funnel_source = 'affiliate'
                WHERE (funnel_source IS NULL OR funnel_source = 'main')
                AND raw_data::text LIKE $1
            `, [`%"codigo":${code}%`]);
            updated += result2.rowCount;
        }
        
        // Method 2: Fix funnel_language for transactions that had 'en-aff' or 'es-aff'
        const fixEnAff = await pool.query(`
            UPDATE transactions 
            SET funnel_language = 'en', funnel_source = 'affiliate'
            WHERE funnel_language = 'en-aff'
        `);
        const fixEsAff = await pool.query(`
            UPDATE transactions 
            SET funnel_language = 'es', funnel_source = 'affiliate'
            WHERE funnel_language = 'es-aff'
        `);
        updated += fixEnAff.rowCount + fixEsAff.rowCount;
        
        // Set remaining NULL funnel_source to 'main'
        const fixNull = await pool.query(`
            UPDATE transactions 
            SET funnel_source = 'main'
            WHERE funnel_source IS NULL
        `);
        
        console.log(`🔄 Migration complete: ${updated} transactions marked as affiliate, ${fixNull.rowCount} set to main`);
        
        res.json({
            success: true,
            affiliateUpdated: updated,
            mainUpdated: fixNull.rowCount,
            message: `Migration complete. ${updated} affiliate, ${fixNull.rowCount} main.`
        });
        
    } catch (error) {
        console.error('Error migrating transaction sources:', error);
        res.status(500).json({ error: 'Failed to migrate transaction sources' });
    }
});

// Recalculate lead totals from transactions
app.post('/api/admin/leads/recalculate', authenticateToken, async (req, res) => {
    try {
        // Get all approved transactions grouped by email
        const transactionsResult = await pool.query(`
            SELECT 
                LOWER(email) as email,
                array_agg(DISTINCT product) as products,
                SUM(CAST(value AS DECIMAL)) as total_spent,
                MIN(created_at) as first_purchase,
                MAX(created_at) as last_purchase,
                COUNT(*) as purchase_count
            FROM transactions 
            WHERE status = 'approved' AND email IS NOT NULL
            GROUP BY LOWER(email)
        `);
        
        let updatedCount = 0;
        let createdCount = 0;
        
        for (const trans of transactionsResult.rows) {
            const result = await pool.query(`
                UPDATE leads SET 
                    status = 'converted',
                    products_purchased = $2,
                    total_spent = $3,
                    first_purchase_at = $4,
                    last_purchase_at = $5,
                    updated_at = NOW()
                WHERE LOWER(email) = $1
                RETURNING id
            `, [
                trans.email,
                trans.products,
                trans.total_spent || 0,
                trans.first_purchase,
                trans.last_purchase
            ]);
            
            if (result.rows.length > 0) {
                updatedCount++;
            } else {
                // No matching lead - create one from transaction data
                try {
                    // Get additional info from the transaction (name, phone, language, source)
                    const txInfo = await pool.query(`
                        SELECT name, phone, funnel_language, funnel_source 
                        FROM transactions 
                        WHERE LOWER(email) = $1 AND status = 'approved'
                        ORDER BY created_at DESC LIMIT 1
                    `, [trans.email]);
                    
                    const info = txInfo.rows[0] || {};
                    
                    await pool.query(`
                        INSERT INTO leads (email, name, whatsapp, status, funnel_language, funnel_source,
                            products_purchased, total_spent, first_purchase_at, last_purchase_at,
                            created_at, updated_at)
                        VALUES (LOWER($1), $2, $3, 'converted', $4, $5,
                            $6, $7, $8, $9, $8, NOW())
                    `, [
                        trans.email,
                        info.name || '',
                        info.phone || '',
                        info.funnel_language || 'en',
                        info.funnel_source || 'main',
                        trans.products,
                        trans.total_spent || 0,
                        trans.first_purchase,
                        trans.last_purchase
                    ]);
                    createdCount++;
                } catch (insertErr) {
                    console.error(`⚠️ Error creating lead for ${trans.email}: ${insertErr.message}`);
                }
            }
        }
        
        res.json({ 
            success: true, 
            message: `Recalculated ${updatedCount} leads, created ${createdCount} new leads from ${transactionsResult.rows.length} buyer emails`,
            updated: updatedCount,
            created: createdCount,
            totalBuyers: transactionsResult.rows.length
        });
        
    } catch (error) {
        console.error('Error recalculating leads:', error);
        res.status(500).json({ error: error.message });
    }
});

// Test geolocation API with a known IP
app.get('/api/admin/test-geolocation', authenticateToken, async (req, res) => {
    try {
        const testIP = req.query.ip || '8.8.8.8'; // Google DNS as test
        console.log('Testing geolocation with IP:', testIP);
        
        const geoData = await getCountryFromIP(testIP);
        
        res.json({
            success: true,
            test_ip: testIP,
            result: geoData,
            api_provider: 'ip-api.com (free, no key required)'
        });
    } catch (error) {
        console.error('Test geolocation error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Debug endpoint to check leads geo status
app.get('/api/admin/leads/geo-debug', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT id, email, ip_address, country, country_code, city 
            FROM leads 
            ORDER BY created_at DESC 
            LIMIT 20
        `);
        
        const summary = await pool.query(`
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN ip_address IS NOT NULL AND ip_address != '' THEN 1 END) as with_ip,
                COUNT(CASE WHEN country IS NOT NULL AND country != '' THEN 1 END) as with_country,
                COUNT(CASE WHEN country_code IS NOT NULL AND country_code != '' AND country_code != 'XX' THEN 1 END) as with_valid_country_code
            FROM leads
        `);
        
        res.json({
            summary: summary.rows[0],
            sample_leads: result.rows.map(l => ({
                id: l.id,
                email: l.email ? l.email.substring(0, 10) + '...' : null,
                ip: l.ip_address,
                country: l.country,
                country_code: l.country_code,
                city: l.city
            }))
        });
    } catch (error) {
        console.error('Geo debug error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Export leads as CSV (protected)
app.get('/api/admin/leads/export', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM leads ORDER BY created_at DESC');
        
        // Create CSV
        const headers = ['ID', 'Name', 'Email', 'WhatsApp', 'Target Phone', 'Gender', 'Status', 'IP', 'Created At'];
        const rows = result.rows.map(lead => [
            lead.id,
            (lead.name || '').replace(/,/g, ' '),
            lead.email,
            lead.whatsapp,
            lead.target_phone || '',
            lead.target_gender || '',
            lead.status || 'new',
            lead.ip_address || '',
            lead.created_at
        ]);
        
        const csv = [headers.join(','), ...rows.map(row => row.join(','))].join('\n');
        
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', `attachment; filename=leads-${new Date().toISOString().split('T')[0]}.csv`);
        res.send(csv);
        
    } catch (error) {
        console.error('Error exporting leads:', error);
        res.status(500).json({ error: 'Failed to export leads' });
    }
});

// Serve admin panel (no-cache to always get latest version)
app.get('/admin', (req, res) => {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.set('Pragma', 'no-cache');
    res.set('Expires', '0');
    res.sendFile(__dirname + '/public/admin.html');
});
app.get('/admin.html', (req, res) => {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.set('Pragma', 'no-cache');
    res.set('Expires', '0');
    res.sendFile(__dirname + '/public/admin.html');
});

// ==================== FUNNEL TRACKING API ====================

// Track funnel event (public - no auth required)
app.post('/api/track', async (req, res) => {
    try {
        const {
            visitorId,
            event,
            page,
            targetPhone,
            targetGender,
            funnelLanguage,  // 'en' or 'es'
            funnelSource,    // 'main' or 'affiliate'
            fbc,             // Facebook Click ID (for CAPI attribution)
            fbp,             // Facebook Browser ID (for CAPI attribution)
            metadata
        } = req.body;
        
        if (!visitorId || !event) {
            return res.status(400).json({ error: 'visitorId and event are required' });
        }
        
        const ipAddress = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        const userAgent = req.headers['user-agent'] || null;
        const language = funnelLanguage || 'en';
        const source = funnelSource || 'main';
        
        // Add language and source to metadata
        const enrichedMetadata = {
            ...(metadata || {}),
            funnelLanguage: language,
            funnelSource: source
        };
        
        // Try INSERT with fbc/fbp columns first; fallback without them if columns don't exist
        try {
            await pool.query(
                `INSERT INTO funnel_events (visitor_id, event, page, target_phone, target_gender, ip_address, user_agent, fbc, fbp, metadata, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())`,
                [visitorId, event, page || null, targetPhone || null, targetGender || null, ipAddress, userAgent, fbc || null, fbp || null, JSON.stringify(enrichedMetadata)]
            );
        } catch (insertError) {
            // If fbc/fbp columns don't exist yet, retry without them
            if (insertError.message && insertError.message.includes('column')) {
                console.warn('⚠️ funnel_events INSERT failed (column issue), retrying without fbc/fbp:', insertError.message);
                await pool.query(
                    `INSERT INTO funnel_events (visitor_id, event, page, target_phone, target_gender, ip_address, user_agent, metadata, created_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())`,
                    [visitorId, event, page || null, targetPhone || null, targetGender || null, ipAddress, userAgent, JSON.stringify(enrichedMetadata)]
                );
            } else {
                throw insertError;
            }
        }
        
        res.json({ success: true, language });
        
    } catch (error) {
        console.error('Error tracking event:', error);
        res.status(500).json({ error: 'Failed to track event' });
    }
});

// Get funnel analytics (protected)
app.get('/api/admin/funnel', authenticateToken, async (req, res) => {
    try {
        const { language, source, startDate, endDate } = req.query;
        
        // Build language filter condition
        // The metadata column stores JSON with funnelLanguage field
        // For 'en': include events where funnelLanguage='en' OR funnelLanguage is NULL/missing (legacy data, assumed English)
        // For 'es': include only events where funnelLanguage='es'
        // For 'all' or undefined: no filter, return all events
        let langCondition = '';
        if (language === 'en') {
            // English includes legacy data (where funnelLanguage is not set)
            // Use COALESCE to handle NULL metadata and missing keys
            langCondition = `AND COALESCE(metadata->>'funnelLanguage', 'en') = 'en'`;
        } else if (language === 'es') {
            langCondition = `AND metadata->>'funnelLanguage' = 'es'`;
        }
        // else: no filter = all languages
        
        // Build source filter condition (main or affiliate)
        let sourceCondition = '';
        if (source === 'main') {
            sourceCondition = `AND COALESCE(metadata->>'funnelSource', 'main') = 'main'`;
        } else if (source === 'affiliate') {
            sourceCondition = `AND metadata->>'funnelSource' = 'affiliate'`;
        }
        // else: no filter = all sources
        
        // Build date filter condition (using Brazil timezone for correct filtering)
        let dateCondition = '';
        if (startDate && endDate) {
            // Use provided date range with Brazil timezone
            dateCondition = `AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= '${startDate}'::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= '${endDate}'::date`;
        } else if (startDate) {
            dateCondition = `AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= '${startDate}'::date`;
        } else {
            // Default: last 30 days
            dateCondition = `AND created_at >= CURRENT_DATE - INTERVAL '30 days'`;
        }
        
        // Get funnel stats by step
        const funnelStats = await pool.query(`
            SELECT 
                event,
                COUNT(DISTINCT visitor_id) as unique_visitors,
                COUNT(*) as total_events
            FROM funnel_events
            WHERE 1=1
            ${dateCondition}
            ${langCondition}
            ${sourceCondition}
            GROUP BY event
            ORDER BY 
                CASE event
                    WHEN 'page_view_landing' THEN 1
                    WHEN 'page_view_phone' THEN 2
                    WHEN 'phone_submitted' THEN 3
                    WHEN 'page_view_conversas' THEN 4
                    WHEN 'page_view_cta' THEN 5
                    WHEN 'email_captured' THEN 6
                    WHEN 'checkout_clicked' THEN 7
                    WHEN 'upsell_1_view' THEN 8
                    WHEN 'upsell_1_accepted' THEN 9
                    WHEN 'upsell_1_declined' THEN 10
                    WHEN 'upsell_2_view' THEN 11
                    WHEN 'upsell_2_accepted' THEN 12
                    WHEN 'upsell_2_declined' THEN 13
                    WHEN 'upsell_3_view' THEN 14
                    WHEN 'upsell_3_accepted' THEN 15
                    WHEN 'upsell_3_declined' THEN 16
                    WHEN 'thankyou_view' THEN 17
                    ELSE 99
                END
        `);
        
        // Get daily funnel data (use same date range, but limit to 7 days for daily view)
        let dailyDateCondition = '';
        if (startDate && endDate) {
            dailyDateCondition = `AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= '${startDate}'::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= '${endDate}'::date`;
        } else {
            dailyDateCondition = `AND created_at >= CURRENT_DATE - INTERVAL '7 days'`;
        }
        
        const dailyStats = await pool.query(`
            SELECT 
                DATE(created_at) as date,
                event,
                COUNT(DISTINCT visitor_id) as unique_visitors
            FROM funnel_events
            WHERE 1=1
            ${dailyDateCondition}
            ${langCondition}
            ${sourceCondition}
            GROUP BY DATE(created_at), event
            ORDER BY date DESC, event
        `);
        
        // Get visitor journeys (filtered by date range) with lead info
        const journeys = await pool.query(`
            SELECT 
                fe.visitor_id,
                fe.target_phone,
                fe.target_gender,
                array_agg(fe.event ORDER BY fe.created_at) as events,
                MIN(fe.created_at) as first_seen,
                MAX(fe.created_at) as last_seen,
                COUNT(*) as total_events,
                l.email,
                l.name,
                l.whatsapp,
                l.country,
                l.country_code
            FROM funnel_events fe
            LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
            WHERE 1=1 ${dateCondition.replace(/created_at/g, 'fe.created_at')} ${langCondition} ${sourceCondition}
            GROUP BY fe.visitor_id, fe.target_phone, fe.target_gender, l.email, l.name, l.whatsapp, l.country, l.country_code
            ORDER BY MAX(fe.created_at) DESC
            LIMIT 100
        `);
        
        // Get transaction stats (approved/rejected) for the funnel visualization
        let transactionStats = { approved: 0, rejected: 0, pending: 0 };
        try {
            // Build date filter for transactions (using Brazil timezone)
            let txDateCondition = '';
            if (startDate && endDate) {
                txDateCondition = `AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= '${startDate}'::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= '${endDate}'::date`;
            } else {
                txDateCondition = `AND created_at >= CURRENT_DATE - INTERVAL '30 days'`;
            }
            
            // Build language filter for transactions (based on funnel_language column)
            let txLangCondition = '';
            if (language === 'en') {
                txLangCondition = `AND (funnel_language = 'en' OR funnel_language IS NULL)`;
            } else if (language === 'es') {
                txLangCondition = `AND funnel_language = 'es'`;
            }
            
            // Build source filter for transactions
            let txSourceCondition = '';
            if (source === 'main') {
                txSourceCondition = `AND (funnel_source = 'main' OR funnel_source IS NULL)`;
            } else if (source === 'affiliate') {
                txSourceCondition = `AND funnel_source = 'affiliate'`;
            } else if (source === 'perfectpay') {
                txSourceCondition = `AND funnel_source = 'perfectpay'`;
            }
            
            // Count unique emails per status (not total transactions)
            // This makes it consistent with funnel which counts unique visitors
            const txResult = await pool.query(`
                SELECT 
                    status,
                    COUNT(DISTINCT email) as count
                FROM transactions
                WHERE 1=1
                ${txDateCondition}
                ${txLangCondition}
                ${txSourceCondition}
                GROUP BY status
            `);
            
            // Parse transaction stats
            const txStats = {};
            txResult.rows.forEach(row => {
                txStats[row.status] = parseInt(row.count);
            });
            
            transactionStats = {
                approved: txStats['approved'] || 0,
                rejected: (txStats['rejected'] || 0) + (txStats['cancelled'] || 0) + (txStats['pending_payment'] || 0),
                pending: txStats['pending_payment'] || 0
            };
            
            // Get upsell stats for funnel visualization
            // Use same product keywords as /api/admin/sales for consistency
            // English products (Main: 341972, 349241-349243 + Affiliates: 330254, 341443-341448)
            const enFrontKeywords = "product ILIKE '%Monitor%' OR product ILIKE '%ZappDetect%' OR product ILIKE '%341972%' OR product ILIKE '%330254%'";
            const enUp1Keywords = "product ILIKE '%Message Vault%' OR product ILIKE '%349241%' OR product ILIKE '%341443%'";
            const enUp2Keywords = "product ILIKE '%360%' OR product ILIKE '%Tracker%' OR product ILIKE '%349242%' OR product ILIKE '%341444%'";
            const enUp3Keywords = "product ILIKE '%Instant Access%' OR product ILIKE '%349243%' OR product ILIKE '%341448%'";
            
            // Spanish products (Main: 349260-349267 + Affiliates: 338375, 341452-341454)
            const esFrontKeywords = "product ILIKE '%Infidelidad%' OR product ILIKE '%349260%' OR product ILIKE '%338375%'";
            const esUp1Keywords = "product ILIKE '%Recuperación%' OR product ILIKE '%349261%' OR product ILIKE '%341452%'";
            const esUp2Keywords = "product ILIKE '%Visión Total%' OR product ILIKE '%349266%' OR product ILIKE '%341453%'";
            const esUp3Keywords = "product ILIKE '%VIP Sin Esperas%' OR product ILIKE '%349267%' OR product ILIKE '%341454%'";
            
            let frontKeywords, up1Keywords, up2Keywords, up3Keywords;
            if (language === 'es') {
                frontKeywords = esFrontKeywords;
                up1Keywords = esUp1Keywords;
                up2Keywords = esUp2Keywords;
                up3Keywords = esUp3Keywords;
            } else if (language === 'en') {
                frontKeywords = enFrontKeywords;
                up1Keywords = enUp1Keywords;
                up2Keywords = enUp2Keywords;
                up3Keywords = enUp3Keywords;
            } else {
                // All languages - combine both
                frontKeywords = `(${enFrontKeywords}) OR (${esFrontKeywords})`;
                up1Keywords = `(${enUp1Keywords}) OR (${esUp1Keywords})`;
                up2Keywords = `(${enUp2Keywords}) OR (${esUp2Keywords})`;
                up3Keywords = `(${enUp3Keywords}) OR (${esUp3Keywords})`;
            }
            
            // Count front sales (approved)
            const frontResult = await pool.query(`
                SELECT COUNT(DISTINCT email) as count 
                FROM transactions 
                WHERE status = 'approved' AND (${frontKeywords}) ${txDateCondition} ${txLangCondition} ${txSourceCondition}
            `);
            
            // Count front rejected/cancelled (for funnel visualization)
            const frontRejectedResult = await pool.query(`
                SELECT COUNT(DISTINCT email) as count 
                FROM transactions 
                WHERE status IN ('rejected', 'cancelled', 'pending_payment') AND (${frontKeywords}) ${txDateCondition} ${txLangCondition} ${txSourceCondition}
            `);
            
            // Count upsell sales
            const up1Result = await pool.query(`
                SELECT COUNT(DISTINCT email) as count 
                FROM transactions 
                WHERE status = 'approved' AND (${up1Keywords}) ${txDateCondition} ${txLangCondition} ${txSourceCondition}
            `);
            
            const up2Result = await pool.query(`
                SELECT COUNT(DISTINCT email) as count 
                FROM transactions 
                WHERE status = 'approved' AND (${up2Keywords}) ${txDateCondition} ${txLangCondition} ${txSourceCondition}
            `);
            
            const up3Result = await pool.query(`
                SELECT COUNT(DISTINCT email) as count 
                FROM transactions 
                WHERE status = 'approved' AND (${up3Keywords}) ${txDateCondition} ${txLangCondition} ${txSourceCondition}
            `);
            
            transactionStats.front = parseInt(frontResult.rows[0].count) || 0;
            transactionStats.frontRejected = parseInt(frontRejectedResult.rows[0].count) || 0;
            transactionStats.upsell1 = parseInt(up1Result.rows[0].count) || 0;
            transactionStats.upsell2 = parseInt(up2Result.rows[0].count) || 0;
            transactionStats.upsell3 = parseInt(up3Result.rows[0].count) || 0;
            
        } catch (txError) {
            console.error('Error fetching transaction stats for funnel:', txError);
            // Continue without transaction stats
        }
        
        res.json({
            funnelStats: funnelStats.rows,
            dailyStats: dailyStats.rows,
            journeys: journeys.rows,
            transactionStats,
            language: language || 'all',
            source: source || 'all',
            dateRange: { startDate, endDate }
        });
        
    } catch (error) {
        console.error('Error fetching funnel data:', error);
        res.status(500).json({ error: 'Failed to fetch funnel data' });
    }
});

// Debug: Search events by email, phone, or visitor_id (protected)
app.get('/api/debug/funnel/search', authenticateToken, async (req, res) => {
    try {
        const { email, phone, visitor_id } = req.query;
        
        if (!email && !phone && !visitor_id) {
            return res.status(400).json({ error: 'Provide email, phone, or visitor_id parameter' });
        }
        
        let events = [];
        let transactions = [];
        let lead = null;
        
        // Search directly by visitor_id
        if (visitor_id) {
            const eventsResult = await pool.query(`
                SELECT * FROM funnel_events 
                WHERE visitor_id = $1 
                ORDER BY created_at ASC
            `, [visitor_id]);
            events = eventsResult.rows;
            
            // Try to find lead with this visitor_id
            const leadResult = await pool.query(`
                SELECT * FROM leads 
                WHERE visitor_id = $1 
                ORDER BY created_at DESC LIMIT 1
            `, [visitor_id]);
            lead = leadResult.rows[0] || null;
            
            res.json({
                visitor_id,
                lead,
                transactions: [],
                events,
                summary: {
                    total_events: events.length,
                    event_types: [...new Set(events.map(e => e.event))]
                }
            });
            return;
        }
        
        // Search by email in transactions
        if (email) {
            const txResult = await pool.query(`
                SELECT * FROM transactions 
                WHERE email ILIKE $1 
                ORDER BY created_at DESC
            `, [`%${email}%`]);
            transactions = txResult.rows;
            
            // Get lead
            const leadResult = await pool.query(`
                SELECT * FROM leads 
                WHERE email ILIKE $1 
                ORDER BY created_at DESC LIMIT 1
            `, [`%${email}%`]);
            lead = leadResult.rows[0] || null;
            
            // Get visitor_id from lead or events
            if (lead?.visitor_id) {
                const eventsResult = await pool.query(`
                    SELECT * FROM funnel_events 
                    WHERE visitor_id = $1 
                    ORDER BY created_at ASC
                `, [lead.visitor_id]);
                events = eventsResult.rows;
            }
        }
        
        // Search by phone in funnel_events target_phone
        if (phone) {
            const phoneClean = phone.replace(/\D/g, '');
            const eventsResult = await pool.query(`
                SELECT * FROM funnel_events 
                WHERE target_phone LIKE $1 
                ORDER BY created_at DESC LIMIT 100
            `, [`%${phoneClean}%`]);
            events = eventsResult.rows;
            
            // Get unique visitor_ids
            const visitorIds = [...new Set(events.map(e => e.visitor_id))];
            
            // Get lead by target_phone
            const leadResult = await pool.query(`
                SELECT * FROM leads 
                WHERE target_phone LIKE $1 
                ORDER BY created_at DESC LIMIT 1
            `, [`%${phoneClean}%`]);
            lead = leadResult.rows[0] || null;
        }
        
        res.json({
            lead,
            transactions,
            events,
            summary: {
                totalEvents: events.length,
                totalTransactions: transactions.length,
                eventTypes: [...new Set(events.map(e => e.event))],
                hasUpsell1View: events.some(e => e.event === 'upsell_1_view'),
                hasUpsell1Accept: events.some(e => e.event === 'upsell_1_accepted'),
                hasUpsell2View: events.some(e => e.event === 'upsell_2_view'),
                hasUpsell2Accept: events.some(e => e.event === 'upsell_2_accepted'),
                hasUpsell3View: events.some(e => e.event === 'upsell_3_view'),
                hasUpsell3Accept: events.some(e => e.event === 'upsell_3_accepted')
            }
        });
        
    } catch (error) {
        console.error('Error searching funnel events:', error);
        res.status(500).json({ error: 'Search failed' });
    }
});

// Get specific visitor journey (protected)
app.get('/api/admin/funnel/visitor/:visitorId', authenticateToken, async (req, res) => {
    try {
        const { visitorId } = req.params;
        
        // Get all events for this visitor
        const events = await pool.query(`
            SELECT *
            FROM funnel_events
            WHERE visitor_id = $1
            ORDER BY created_at ASC
        `, [visitorId]);
        
        // Get target phone from events to find associated lead
        const targetPhone = events.rows.length > 0 ? events.rows[0].target_phone : null;
        
        // Try to find associated lead by matching target_phone or by email from metadata
        let lead = null;
        if (targetPhone) {
            const leadResult = await pool.query(`
                SELECT name, email, whatsapp, target_phone, target_gender, status, created_at
                FROM leads
                WHERE target_phone = $1
                ORDER BY created_at DESC
                LIMIT 1
            `, [targetPhone]);
            lead = leadResult.rows[0] || null;
        }
        
        // Get transaction info if exists (by lead email)
        let transaction = null;
        if (lead && lead.email) {
            const transResult = await pool.query(`
                SELECT product, value, status, created_at
                FROM transactions
                WHERE LOWER(email) = LOWER($1)
                ORDER BY created_at DESC
                LIMIT 1
            `, [lead.email]);
            transaction = transResult.rows[0] || null;
        }
        
        res.json({ 
            events: events.rows,
            lead: lead,
            transaction: transaction
        });
        
    } catch (error) {
        console.error('Error fetching visitor journey:', error);
        res.status(500).json({ error: 'Failed to fetch visitor journey' });
    }
});

// Get complete customer journey by lead ID (protected)
app.get('/api/admin/customer/:leadId/journey', authenticateToken, async (req, res) => {
    try {
        const { leadId } = req.params;
        
        // Get lead details
        const leadResult = await pool.query(`
            SELECT * FROM leads WHERE id = $1
        `, [leadId]);
        
        if (leadResult.rows.length === 0) {
            return res.status(404).json({ error: 'Lead not found' });
        }
        
        const lead = leadResult.rows[0];
        
        // Get funnel events by visitor_id if available
        let funnelEvents = [];
        if (lead.visitor_id) {
            const eventsResult = await pool.query(`
                SELECT event, page, target_phone, target_gender, created_at, metadata
                FROM funnel_events
                WHERE visitor_id = $1
                ORDER BY created_at ASC
            `, [lead.visitor_id]);
            funnelEvents = eventsResult.rows;
        }
        
        // Get all transactions for this email
        const transactionsResult = await pool.query(`
            SELECT id, product, value, status, monetizze_status, funnel_language, created_at
            FROM transactions
            WHERE LOWER(email) = LOWER($1)
            ORDER BY created_at ASC
        `, [lead.email]);
        
        // Build timeline
        const timeline = [];
        
        // Add funnel events to timeline
        funnelEvents.forEach(event => {
            const eventLabels = {
                // Main funnel events
                'page_view_landing': '👀 Visitou página inicial',
                'page_view_phone': '📱 Página do telefone',
                'gender_selected': '👤 Selecionou gênero',
                'phone_submitted': '✅ Submeteu telefone alvo',
                'page_view_conversas': '💬 Visualizou conversas',
                'page_view_chat': '💬 Visualizou chat',
                'page_view_cta': '🎯 Página de oferta',
                'email_captured': '📧 Email capturado',
                'checkout_clicked': '🛒 Clicou no checkout',
                'checkout_50off_clicked': '🛒 Clicou checkout 50% OFF',
                'scroll_50_percent': '📜 Scroll 50%',
                'scroll_100_percent': '📜 Scroll 100%',
                'scroll_90_percent': '📜 Scroll 90%',
                'time_on_page_30s': '⏱️ 30s na página',
                'time_on_page_60s': '⏱️ 60s na página',
                'exit_intent_shown': '🚪 Exit intent',
                
                // Upsell page views
                'upsell_1_view': '👀 Visualizou Upsell 1',
                'upsell_2_view': '👀 Visualizou Upsell 2',
                'upsell_3_view': '👀 Visualizou Upsell 3',
                'thankyou_view': '🎉 Página de obrigado',
                
                // Upsell page ready
                'upsell_1_ready': '✅ Upsell 1 carregado',
                'upsell_2_ready': '✅ Upsell 2 carregado',
                'upsell_3_ready': '✅ Upsell 3 carregado',
                
                // Upsell accepts
                'upsell_1_accepted': '💰 ACEITOU Upsell 1',
                'upsell_2_accepted': '💰 ACEITOU Upsell 2',
                'upsell_3_accepted': '💰 ACEITOU Upsell 3',
                
                // Upsell declines
                'upsell_1_declined': '❌ Recusou Upsell 1',
                'upsell_2_declined': '❌ Recusou Upsell 2',
                'upsell_3_declined': '❌ Recusou Upsell 3',
                
                // Upsell CTA visibility
                'upsell_1_cta_visible': '👁️ CTA visível Up1',
                'upsell_2_cta_visible': '👁️ CTA visível Up2',
                'upsell_3_cta_visible': '👁️ CTA visível Up3',
                
                // Upsell exits
                'upsell_1_exit': '🚪 Saiu do Upsell 1',
                'upsell_2_exit': '🚪 Saiu do Upsell 2',
                'upsell_3_exit': '🚪 Saiu do Upsell 3',
                
                // Engagement milestones
                'engaged_10s': '⏱️ Engajado 10s',
                'engaged_30s': '⏱️ Engajado 30s',
                'engaged_60s': '⏱️ Engajado 60s'
            };
            
            timeline.push({
                type: 'funnel_event',
                event: event.event,
                label: eventLabels[event.event] || event.event,
                page: event.page,
                timestamp: event.created_at,
                details: event.metadata
            });
        });
        
        // Add lead creation
        timeline.push({
            type: 'lead_captured',
            event: 'lead_captured',
            label: '🎉 Lead capturado',
            timestamp: lead.created_at,
            details: {
                email: lead.email,
                whatsapp: lead.whatsapp,
                country: lead.country
            }
        });
        
        // Add transactions to timeline
        transactionsResult.rows.forEach(tx => {
            const statusLabels = {
                'approved': '✅ Compra aprovada',
                'pending': '⏳ Pagamento pendente',
                'refunded': '💸 Reembolsado',
                'chargeback': '⚠️ Chargeback',
                'cancelled': '❌ Cancelado'
            };
            
            // Identify product type
            const productLower = (tx.product || '').toLowerCase();
            let productType = 'Front';
            if (productLower.includes('recovery') || productLower.includes('recuperação') || productLower.includes('recuperación')) {
                productType = 'Upsell 1';
            } else if (productLower.includes('vision') || productLower.includes('visão') || productLower.includes('visión')) {
                productType = 'Upsell 2';
            } else if (productLower.includes('vip') || productLower.includes('priority') || productLower.includes('prioridade') || productLower.includes('esperas')) {
                productType = 'Upsell 3';
            }
            
            timeline.push({
                type: 'transaction',
                event: tx.status,
                label: statusLabels[tx.status] || tx.status,
                timestamp: tx.created_at,
                details: {
                    product: tx.product,
                    productType: productType,
                    value: tx.value,
                    status: tx.status
                }
            });
        });
        
        // Sort timeline by timestamp
        timeline.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
        
        // Calculate totalSpent from actual approved transactions (not the cached field)
        const approvedTransactions = transactionsResult.rows.filter(t => t.status === 'approved');
        const calculatedTotalSpent = approvedTransactions.reduce((sum, t) => sum + parseFloat(t.value || 0), 0);
        
        // Calculate summary
        const summary = {
            totalEvents: funnelEvents.length,
            totalTransactions: transactionsResult.rows.length,
            totalSpent: calculatedTotalSpent, // Use calculated value from actual transactions
            productsPurchased: [...new Set(approvedTransactions.map(t => t.product).filter(Boolean))],
            firstSeen: funnelEvents.length > 0 ? funnelEvents[0].created_at : lead.created_at,
            lastActivity: timeline.length > 0 ? timeline[timeline.length - 1].timestamp : lead.created_at,
            status: lead.status,
            visitCount: lead.visit_count || 1
        };
        
        res.json({
            lead: lead,
            timeline: timeline,
            transactions: transactionsResult.rows,
            summary: summary
        });
        
    } catch (error) {
        console.error('Error fetching customer journey:', error);
        res.status(500).json({ error: 'Failed to fetch customer journey' });
    }
});

// ==================== MONETIZZE POSTBACK API ====================

// Store last 20 postbacks for debugging
const recentPostbacks = [];

// Quick sales count check (protected)
app.get('/api/admin/debug/sales-count', authenticateToken, async (req, res) => {
    try {
        const total = await pool.query(`SELECT COUNT(*) as count FROM transactions`);
        const approved = await pool.query(`SELECT COUNT(*) as count FROM transactions WHERE status = 'approved'`);
        const byStatus = await pool.query(`SELECT status, COUNT(*) as count FROM transactions GROUP BY status ORDER BY count DESC`);
        const byDate = await pool.query(`
            SELECT (created_at AT TIME ZONE 'America/Sao_Paulo')::date as date, COUNT(*) as total, 
                   SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved
            FROM transactions 
            GROUP BY (created_at AT TIME ZONE 'America/Sao_Paulo')::date 
            ORDER BY date DESC 
            LIMIT 10
        `);
        
        // Debug: count unique transaction_ids vs total rows for cancelled
        const cancelledTotal = await pool.query(`
            SELECT COUNT(*) as total_rows, COUNT(DISTINCT transaction_id) as unique_tx
            FROM transactions 
            WHERE status IN ('cancelled', 'pending_payment', 'blocked', 'refused', 'rejected', 'waiting_payment')
        `);
        
        // Debug: show all cancelled transactions for today
        const cancelledToday = await pool.query(`
            SELECT transaction_id, email, product, value, status, created_at
            FROM transactions 
            WHERE status IN ('cancelled', 'pending_payment', 'blocked', 'refused', 'rejected', 'waiting_payment')
            AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
            ORDER BY created_at DESC
        `);
        
        res.json({
            totalTransactions: parseInt(total.rows[0].count),
            approvedTransactions: parseInt(approved.rows[0].count),
            byStatus: byStatus.rows,
            byDate: byDate.rows,
            cancelledDebug: {
                totalRows: parseInt(cancelledTotal.rows[0].total_rows),
                uniqueTransactions: parseInt(cancelledTotal.rows[0].unique_tx),
                todayTransactions: cancelledToday.rows
            }
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Debug endpoint to see recent postbacks (memory + DB) - admin only
app.get('/api/admin/debug/postbacks', authenticateToken, requireAdmin, async (req, res) => {
    // Extract value fields from each postback for easy viewing
    const postbacksWithValueInfo = recentPostbacks.map(p => {
        const venda = p.body?.venda || {};
        return {
            timestamp: p.timestamp,
            chave_unica: p.body?.chave_unica || venda.codigo,
            status: p.body?.tipoEvento?.codigo,
            produto: p.body?.produto?.nome,
            valores: {
                'body.comissao': p.body?.comissao || 'N/A',
                'venda.comissao': venda.comissao || 'N/A',
                'venda.valorLiquido': venda.valorLiquido || 'N/A',
                'venda.valorRecebido': venda.valorRecebido || 'N/A',
                'venda.valor': venda.valor || 'N/A',
                'body.valor': p.body?.valor || 'N/A'
            },
            comprador: p.body?.comprador?.email,
            hasDotKeys: p.hasDotKeys
        };
    });
    
    // Also get DB logs
    let dbLogs = [];
    try {
        const dbResult = await pool.query(`
            SELECT id, content_type, body, created_at 
            FROM postback_logs 
            ORDER BY created_at DESC 
            LIMIT 30
        `);
        dbLogs = dbResult.rows;
    } catch (err) {
        dbLogs = [{ error: err.message }];
    }
    
    // Also check recent transactions
    let recentTx = [];
    try {
        const txResult = await pool.query(`
            SELECT transaction_id, email, product, value, status, monetizze_status, created_at 
            FROM transactions 
            ORDER BY created_at DESC 
            LIMIT 10
        `);
        recentTx = txResult.rows;
    } catch (err) {
        recentTx = [{ error: err.message }];
    }
    
    res.json({
        memoryCount: recentPostbacks.length,
        dbLogCount: dbLogs.length,
        info: 'Memory postbacks are lost on server restart. DB logs persist. Check if Monetizze is sending postbacks to the correct URL.',
        expectedUrl: 'https://zapspy-funnel-production.up.railway.app/api/postback/monetizze',
        alternateUrl: 'https://painel.xaimonitor.com/api/postback/monetizze',
        postbacks: postbacksWithValueInfo,
        dbLogs,
        recentTransactions: recentTx,
        fullPostbacks: recentPostbacks
    });
});

// Simple test endpoint for postback (no DB)
app.post('/api/postback/test', (req, res) => {
    console.log('🧪 Test postback received:', req.body);
    res.json({ 
        status: 'ok', 
        received: req.body,
        keys: Object.keys(req.body || {})
    });
});

// Debug endpoint to search for a specific transaction
app.get('/api/admin/debug/transaction/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        // Search by transaction_id (exact or partial match)
        const result = await pool.query(`
            SELECT * FROM transactions 
            WHERE transaction_id ILIKE $1 
               OR transaction_id ILIKE $2
            ORDER BY created_at DESC
            LIMIT 10
        `, [`%${id}%`, id]);
        
        // Also check postback_logs for this ID
        const logsResult = await pool.query(`
            SELECT id, content_type, body, created_at 
            FROM postback_logs 
            WHERE body::text ILIKE $1
            ORDER BY created_at DESC
            LIMIT 10
        `, [`%${id}%`]);
        
        res.json({
            searchId: id,
            foundTransactions: result.rows.length,
            transactions: result.rows,
            foundInPostbackLogs: logsResult.rows.length,
            postbackLogs: logsResult.rows
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Debug endpoint to search transactions by email
app.get('/api/admin/debug/transactions-by-email/:email', authenticateToken, async (req, res) => {
    try {
        const { email } = req.params;
        
        // Search transactions by email
        const transactions = await pool.query(`
            SELECT id, transaction_id, product, value, status, monetizze_status, 
                   funnel_language, funnel_source, email, phone, created_at, updated_at,
                   raw_data
            FROM transactions 
            WHERE LOWER(email) = LOWER($1)
            ORDER BY created_at DESC
        `, [email]);
        
        // Get the lead for this email
        const lead = await pool.query(`
            SELECT id, email, name, status, total_spent, products_purchased, 
                   first_purchase_at, last_purchase_at, visitor_id
            FROM leads 
            WHERE LOWER(email) = LOWER($1)
            LIMIT 1
        `, [email]);
        
        // Calculate what the total_spent SHOULD be
        const approvedTransactions = transactions.rows.filter(t => t.status === 'approved');
        const calculatedTotal = approvedTransactions.reduce((sum, t) => sum + parseFloat(t.value || 0), 0);
        
        res.json({
            email: email,
            foundTransactions: transactions.rows.length,
            approvedCount: approvedTransactions.length,
            transactions: transactions.rows,
            lead: lead.rows[0] || null,
            analysis: {
                leadTotalSpent: lead.rows[0]?.total_spent || 0,
                calculatedTotalFromTransactions: calculatedTotal,
                mismatch: lead.rows[0] ? (parseFloat(lead.rows[0].total_spent || 0) !== calculatedTotal) : false,
                suggestedFix: lead.rows[0] && (parseFloat(lead.rows[0].total_spent || 0) !== calculatedTotal) 
                    ? `UPDATE leads SET total_spent = ${calculatedTotal} WHERE LOWER(email) = LOWER('${email}')`
                    : null
            }
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Fix transaction status endpoint
app.post('/api/admin/fix-transaction/:transactionId', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const { transactionId } = req.params;
        const { newStatus, syncFromMonetizze } = req.body;
        
        if (!newStatus && !syncFromMonetizze) {
            return res.status(400).json({ error: 'Provide newStatus or set syncFromMonetizze: true' });
        }
        
        // Find the transaction
        const txResult = await pool.query(`
            SELECT * FROM transactions WHERE transaction_id = $1
        `, [transactionId]);
        
        if (txResult.rows.length === 0) {
            return res.status(404).json({ error: 'Transaction not found' });
        }
        
        const tx = txResult.rows[0];
        const oldStatus = tx.status;
        
        // Update transaction status
        const validStatuses = ['approved', 'pending_payment', 'cancelled', 'refunded', 'chargeback', 'abandoned_checkout'];
        if (!validStatuses.includes(newStatus)) {
            return res.status(400).json({ error: `Invalid status. Use: ${validStatuses.join(', ')}` });
        }
        
        await pool.query(`
            UPDATE transactions 
            SET status = $1, updated_at = NOW()
            WHERE transaction_id = $2
        `, [newStatus, transactionId]);
        
        // Recalculate lead total_spent
        if (tx.email) {
            const approvedSum = await pool.query(`
                SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total
                FROM transactions 
                WHERE LOWER(email) = LOWER($1) AND status = 'approved'
            `, [tx.email]);
            
            await pool.query(`
                UPDATE leads 
                SET total_spent = $1, updated_at = NOW()
                WHERE LOWER(email) = LOWER($2)
            `, [approvedSum.rows[0].total, tx.email]);
        }
        
        res.json({
            success: true,
            transactionId,
            oldStatus,
            newStatus,
            message: `Transaction ${transactionId} updated from ${oldStatus} to ${newStatus}`
        });
        
    } catch (error) {
        console.error('Error fixing transaction:', error);
        res.status(500).json({ error: error.message });
    }
});

// Helper: Authenticate with Monetizze API 2.1 (2-step auth)
// Step 1: GET /token with X_CONSUMER_KEY header → returns temporary token
// Step 2: Use TOKEN header for all subsequent requests
async function getMonetizzeToken() {
    const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
    if (!consumerKey) throw new Error('MONETIZZE_CONSUMER_KEY not configured');
    
    console.log('🔑 Authenticating with Monetizze API 2.1...');
    const response = await fetch('https://api.monetizze.com.br/2.1/token', {
        method: 'GET',
        headers: {
            'X_CONSUMER_KEY': consumerKey,
            'Accept': 'application/json'
        }
    });
    
    if (!response.ok) {
        const errorText = await response.text();
        console.error('❌ Monetizze auth failed:', response.status, errorText);
        throw new Error(`Auth failed (${response.status}): ${errorText}`);
    }
    
    const data = await response.json();
    console.log('✅ Monetizze token obtained successfully');
    return data.token;
}

// Test Monetizze API connectivity (debug endpoint)
app.get('/api/admin/test-monetizze-api', authenticateToken, requireAdmin, async (req, res) => {
    const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
    const results = { tests: [], consumerKeyPresent: !!consumerKey, keyLength: consumerKey?.length };
    
    if (!consumerKey) {
        return res.json({ error: 'MONETIZZE_CONSUMER_KEY not configured', results });
    }
    
    // Step 1: Try to get auth token
    let monetizzeToken = null;
    try {
        console.log('Step 1: Getting auth token...');
        const authResponse = await fetch('https://api.monetizze.com.br/2.1/token', {
            method: 'GET',
            headers: {
                'X_CONSUMER_KEY': consumerKey,
                'Accept': 'application/json'
            }
        });
        const authText = await authResponse.text();
        results.tests.push({
            name: 'Step 1: GET /token (X_CONSUMER_KEY header)',
            status: authResponse.status,
            ok: authResponse.ok,
            body: authText.substring(0, 500)
        });
        
        if (authResponse.ok) {
            try {
                const authData = JSON.parse(authText);
                monetizzeToken = authData.token;
                results.tokenObtained = true;
                results.tokenPreview = monetizzeToken ? monetizzeToken.substring(0, 20) + '...' : 'null';
            } catch (e) {
                results.tokenObtained = false;
            }
        }
    } catch (err) {
        results.tests.push({ name: 'Step 1: GET /token', error: err.message });
    }
    
    // Step 2: If we got a token, try to query transactions
    if (monetizzeToken) {
        try {
            console.log('Step 2: Querying transactions with token...');
            const txResponse = await fetch('https://api.monetizze.com.br/2.1/transactions', {
                method: 'GET',
                headers: {
                    'TOKEN': monetizzeToken,
                    'Accept': 'application/json'
                }
            });
            const txText = await txResponse.text();
            results.tests.push({
                name: 'Step 2: GET /transactions (TOKEN header)',
                status: txResponse.status,
                ok: txResponse.ok,
                body: txText.substring(0, 1000)
            });
        } catch (err) {
            results.tests.push({ name: 'Step 2: GET /transactions', error: err.message });
        }
    }
    
    res.json(results);
});

// ==================== MONETIZZE SYNC CORE FUNCTION ====================
// Reusable sync logic - called by manual endpoint AND auto-sync
async function syncMonetizzeSalesCore(startDate, endDate) {
    const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
    if (!consumerKey) {
        throw new Error('MONETIZZE_CONSUMER_KEY not configured');
    }
    
    console.log('🔄 Starting Monetizze sync...');
    console.log('📅 Date range:', startDate || 'today', 'to', endDate || 'today');
    
    // Step 1: Authenticate
    let monetizzeToken;
    try {
        monetizzeToken = await getMonetizzeToken();
    } catch (authError) {
        throw new Error(`Monetizze authentication failed: ${authError.message}`);
    }
    
    // Step 2: Query transactions
    const params = new URLSearchParams();
    if (startDate) params.append('date_min', `${startDate} 00:00:00`);
    if (endDate) params.append('date_max', `${endDate} 23:59:59`);
    // Monetizze API only supports status 1-6. Chargebacks (8/9) come via postback only.
    // 1=Pending, 2=Approved, 3=Cancelled, 4=Refunded/Devolvida, 5=Blocked, 6=Complete
    ['1','2','3','4','5','6'].forEach(s => params.append('status[]', s));
    
    const validProductCodes = [
        '341972', '349241', '349242', '349243',
        '330254', '341443', '341444', '341448',
        '349260', '349261', '349266', '349267',
        '338375', '341452', '341453', '341454'
    ];
    validProductCodes.forEach(code => params.append('product[]', code));
    
    const txUrl = `https://api.monetizze.com.br/2.1/transactions?${params.toString()}`;
    console.log('🌐 Fetching transactions from Monetizze API 2.1');
    
    const response = await fetch(txUrl, {
        method: 'GET',
        headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
    });
    
    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Monetizze API error ${response.status}: ${errorText.substring(0, 200)}`);
    }
    
    const data = await response.json();
    
    let salesArray = [];
    if (Array.isArray(data)) salesArray = data;
    else if (Array.isArray(data.dados)) salesArray = data.dados;
    else if (Array.isArray(data.vendas)) salesArray = data.vendas;
    else if (Array.isArray(data.recordset)) salesArray = data.recordset;
    
    // Log pagination info for debugging
    console.log(`📄 API response - pagina: ${data.pagina}, paginas: ${data.paginas}, registros: ${data.registros}, first page items: ${salesArray.length}`);
    
    // Handle pagination - keep fetching until no more results
    const totalPages = data.paginas || 0;
    const totalRecords = data.registros || 0;
    
    if (totalPages > 1) {
        console.log(`📄 Pagination: ${totalPages} pages, ${totalRecords} total records`);
        for (let page = 2; page <= Math.min(totalPages, 15); page++) { // Max 15 pages (1500 items) safety limit
            try {
                const pageParams = new URLSearchParams(params.toString());
                pageParams.set('pagina', String(page));
                const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                    method: 'GET',
                    headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                });
                if (pageResponse.ok) {
                    const pageData = await pageResponse.json();
                    const pageItems = pageData.dados || pageData.vendas || [];
                    salesArray = salesArray.concat(pageItems);
                    console.log(`📄 Page ${page}/${totalPages}: ${pageItems.length} items (total: ${salesArray.length})`);
                    if (pageItems.length === 0) break; // No more data
                }
            } catch (pageError) {
                console.error(`❌ Error fetching page ${page}:`, pageError.message);
            }
        }
    } else if (salesArray.length >= 100 && totalPages === 0) {
        // API might not return paginas field - try fetching more pages manually
        console.log(`⚠️ Got ${salesArray.length} items but no pagination info. Trying to fetch more pages...`);
        for (let page = 2; page <= 15; page++) { // Max 15 pages (1500 items)
            try {
                const pageParams = new URLSearchParams(params.toString());
                pageParams.set('pagina', String(page));
                const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                    method: 'GET',
                    headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                });
                if (pageResponse.ok) {
                    const pageData = await pageResponse.json();
                    const pageItems = pageData.dados || pageData.vendas || [];
                    if (pageItems.length === 0) {
                        console.log(`📄 Page ${page}: empty - pagination complete`);
                        break;
                    }
                    salesArray = salesArray.concat(pageItems);
                    console.log(`📄 Page ${page}: ${pageItems.length} items (total: ${salesArray.length})`);
                } else {
                    console.log(`📄 Page ${page}: HTTP ${pageResponse.status} - stopping pagination`);
                    break;
                }
            } catch (pageError) {
                console.error(`❌ Page ${page} error:`, pageError.message);
                break;
            }
        }
    }
    
    console.log(`📦 Total transactions fetched: ${salesArray.length}`);
    
    if (salesArray.length === 0) {
        return { synced: 0, skipped: 0, total: 0 };
    }
    
    let synced = 0;
    let skipped = 0;
    let errors = [];
    
    const spanishCodes = ['349260', '349261', '349266', '349267', '338375', '341452', '341453', '341454'];
    const affiliateCodes = ['330254', '341443', '341444', '341448', '338375', '341452', '341453', '341454'];
    const statusMap = {
        '1': 'pending_payment', '2': 'approved', '3': 'cancelled',
        '4': 'refunded', '5': 'blocked', '6': 'approved',
        '7': 'abandoned_checkout', '8': 'chargeback', '9': 'chargeback'
    };
    
    for (const item of salesArray) {
        try {
            const vendaData = item.venda || item;
            const produtoData = item.produto || {};
            const compradorData = item.comprador || {};
            const tipoEvento = item.tipoEvento || {};
            
            const transactionId = vendaData.codigo || item.codigo_venda || item.chave_unica;
            const email = compradorData.email || vendaData.email;
            const phone = compradorData.telefone || vendaData.telefone;
            const name = compradorData.nome || vendaData.nome;
            const productName = produtoData.nome || vendaData.produto_nome;
            const productCode = produtoData.codigo || vendaData.produto_codigo;
            const value = vendaData.valorRecebido || vendaData.comissao || vendaData.valor;
            const statusCode = String(tipoEvento.codigo || item.codigo_status || '2');
            
            if (productCode && !validProductCodes.includes(String(productCode))) {
                skipped++;
                continue;
            }
            
            // Priority: dataInicio (Data Pedido in Monetizze UI) for consistent ordering
            // dataFinalizada only exists for completed sales, so use dataInicio for all
            const saleDateStr = vendaData.dataInicio || vendaData.dataFinalizada || vendaData.dataVenda || vendaData.data || null;
            const saleDate = parseMonetizzeDate(saleDateStr);
            
            // Debug log for date parsing (remove after testing)
            if (synced < 5) {
                console.log(`📅 DEBUG DATE - Using: "${saleDateStr}" | Parsed: ${saleDate ? saleDate.toISOString() : 'null'} | dataFinalizada: "${vendaData.dataFinalizada}" | dataInicio: "${vendaData.dataInicio}"`);
            }
            
            const funnelLanguage = spanishCodes.includes(String(productCode)) ? 'es' : 'en';
            const funnelSource = affiliateCodes.includes(String(productCode)) ? 'affiliate' : 'main';
            
            // Map status - IMPORTANT: check if sale is actually finalized
            // Monetizze can send status='2' (Finalizada) but without valid dataFinalizada
            let mappedStatus = statusMap[String(statusCode)] || 'approved';
            
            // Check dataFinalizada - if it's "0000-00-00" or empty, it's not finalized
            const dataFinalizada = vendaData.dataFinalizada || '';
            const isFinalized = dataFinalizada && 
                               dataFinalizada !== '0000-00-00 00:00:00' && 
                               dataFinalizada !== '0000-00-00' &&
                               !dataFinalizada.startsWith('0000-00-00');
            
            // Check venda.status text for the REAL status
            const vendaStatus = (vendaData.status || '').toLowerCase();
            // Also check tipoEvento.descricao for additional status info
            const eventoDesc = (tipoEvento.descricao || '').toLowerCase();
            const combinedStatus = `${vendaStatus} ${eventoDesc}`;
            
            // Debug: log refund/chargeback detection
            if (statusCode === '4' || statusCode === '8' || statusCode === '9' || 
                combinedStatus.includes('chargeback') || combinedStatus.includes('devolvida') || 
                combinedStatus.includes('reembolso') || combinedStatus.includes('disputa')) {
                console.log(`🔴 REFUND/CHARGEBACK DETECTED - ID: ${transactionId}, statusCode: ${statusCode}, vendaStatus: "${vendaData.status}", eventoDesc: "${tipoEvento.descricao}", mappedStatus (before text check): ${mappedStatus}`);
            }
            
            // Text-based status detection can UPGRADE to refund/chargeback but should NEVER
            // downgrade from refunded/chargeback back to approved/cancelled/etc.
            // The statusCode from tipoEvento.codigo is the most reliable source for refund/chargeback detection.
            const statusCodeIsRefund = (statusCode === '4' || statusCode === '8' || statusCode === '9');
            
            // Check text for refund/chargeback indicators (can upgrade status)
            if (combinedStatus.includes('chargeback') || combinedStatus.includes('disputa') || combinedStatus.includes('contestação') || combinedStatus.includes('contestacao')) {
                mappedStatus = 'chargeback';
            } else if (combinedStatus.includes('devolvida') || combinedStatus.includes('reembolso') || combinedStatus.includes('reembolsada') || combinedStatus.includes('refund')) {
                mappedStatus = 'refunded';
            } else if (statusCodeIsRefund) {
                // StatusCode says refund/chargeback - KEEP IT, don't let text override change it
                // This handles cases where vendaStatus says "Finalizada" but statusCode is 4/8/9
                console.log(`🔒 Preserving ${mappedStatus} from statusCode=${statusCode} (vendaStatus text was: "${vendaData.status}")`);
            } else if (vendaStatus.includes('cancelada') || vendaStatus.includes('cancel')) {
                mappedStatus = 'cancelled';
            } else if (vendaStatus.includes('aguardando') || vendaStatus.includes('pending')) {
                mappedStatus = 'pending_payment';
            } else if (vendaStatus.includes('finalizada') || vendaStatus.includes('aprovada')) {
                // Monetizze says "Finalizada"/"Aprovada" - trust it as approved
                mappedStatus = 'approved';
                if (!isFinalized) {
                    console.log(`⚠️ SYNC: vendaStatus says "${vendaData.status}" but dataFinalizada invalid - trusting as approved`);
                }
            } else if (statusCode === '2' || statusCode === '6') {
                // Status code 2/6 = Monetizze confirmed payment - always mark as approved
                mappedStatus = 'approved';
                if (!isFinalized) {
                    console.log(`⚠️ SYNC: statusCode=${statusCode} (approved) but dataFinalizada invalid - trusting statusCode`);
                }
            }
            
            // Final debug log for refund/chargeback after all status resolution
            if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
                console.log(`✅ FINAL STATUS: ${transactionId} → ${mappedStatus} (email: ${email}, product: ${productName}, value: ${value})`);
            }
            
            if (!email || !transactionId) {
                skipped++;
                continue;
            }
            
            // Try to get correct WhatsApp from leads table (more reliable than Monetizze phone)
            let finalPhone = phone;
            try {
                const leadResult = await pool.query(
                    `SELECT whatsapp FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                    [email]
                );
                if (leadResult.rows.length > 0 && leadResult.rows[0].whatsapp) {
                    finalPhone = leadResult.rows[0].whatsapp;
                }
            } catch (leadErr) {
                // Silently ignore - use Monetizze phone as fallback
            }
            
            // CHECK: Is this transaction becoming "approved" for the first time?
            // If so, we need to send a CAPI Purchase event in real-time
            let wasNewlyApproved = false;
            if (mappedStatus === 'approved') {
                try {
                    const existingTx = await pool.query(
                        `SELECT status FROM transactions WHERE transaction_id = $1`,
                        [transactionId]
                    );
                    // Newly approved if: didn't exist before, OR existed with different status
                    if (existingTx.rows.length === 0 || existingTx.rows[0].status !== 'approved') {
                        // Also check if CAPI was already sent (by postback handler)
                        const capiExists = await pool.query(
                            `SELECT id FROM capi_purchase_logs WHERE transaction_id = $1`,
                            [transactionId]
                        );
                        if (capiExists.rows.length === 0) {
                            wasNewlyApproved = true;
                        }
                    }
                } catch (checkErr) {
                    // Non-blocking
                }
            }
            
            await pool.query(`
                INSERT INTO transactions (
                    transaction_id, email, phone, name, product, value, 
                    monetizze_status, status, raw_data, funnel_language, funnel_source, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, COALESCE($12, NOW()))
                ON CONFLICT (transaction_id) 
                DO UPDATE SET 
                    monetizze_status = $7,
                    status = $8,
                    raw_data = $9,
                    funnel_language = $10,
                    funnel_source = $11,
                    phone = COALESCE($3, transactions.phone),
                    updated_at = NOW()
            `, [transactionId, email, finalPhone, name, productName, value, String(statusCode), mappedStatus, JSON.stringify(item), funnelLanguage, funnelSource, saleDate]);
            
            // SYNC CAPI: Don't send Purchase immediately - let the catch-up handle it
            // The catch-up (sendMissingCAPIPurchases) runs right after sync finishes
            // and has the LATEST fbc/fbp data (from enrichPurchase on upsell pages)
            if (wasNewlyApproved && email) {
                console.log(`🔥 SYNC: Transaction ${transactionId} (${email}) just became approved! CAPI Purchase will be sent via catch-up after sync completes.`);
            }
            
            // Also create refund_requests entry for refunds/chargebacks so they appear in admin panel
            if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
                try {
                    const refundProtocol = `MON-${String(transactionId).substring(0, 12).toUpperCase()}`;
                    const refundType = mappedStatus === 'chargeback' ? 'chargeback' : 'refund';
                    
                    // Check if already exists to avoid duplicates
                    const existing = await pool.query(
                        'SELECT id FROM refund_requests WHERE transaction_id = $1',
                        [String(transactionId)]
                    );
                    
                    if (existing.rows.length === 0) {
                        await pool.query(`
                            INSERT INTO refund_requests (
                                protocol, full_name, email, phone, product, reason, 
                                status, source, refund_type, transaction_id, value, funnel_language, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, COALESCE($13, NOW()))
                        `, [
                            refundProtocol,
                            name || 'N/A',
                            email,
                            finalPhone || null,
                            productName,
                            refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                            'approved', // Monetizze already processed it
                            'monetizze',
                            refundType,
                            String(transactionId),
                            parseFloat(value) || 0,
                            funnelLanguage,
                            saleDate
                        ]);
                        
                        console.log(`📥 SYNC: ${refundType.toUpperCase()} registered: ${refundProtocol} - ${email} - ${productName}`);
                    } else {
                        // Update existing entry if status changed (e.g., refund -> chargeback)
                        await pool.query(`
                            UPDATE refund_requests SET 
                                refund_type = $1, 
                                status = 'approved',
                                updated_at = NOW()
                            WHERE transaction_id = $2 AND refund_type != $1
                        `, [refundType, String(transactionId)]);
                    }
                } catch (refundError) {
                    console.error(`⚠️ Error registering refund_request during sync: ${refundError.message}`);
                }
            }
            
            synced++;
        } catch (saleError) {
            errors.push({ sale: item?.venda?.codigo || 'unknown', error: saleError.message });
            skipped++;
        }
    }
    
    console.log(`🎉 Sync complete: ${synced} synced, ${skipped} skipped`);
    return { synced, skipped, total: salesArray.length, errors: errors.length > 0 ? errors : undefined };
}

// ==================== AUTO-SYNC MONETIZZE (every 30 minutes) ====================
let autoSyncInterval = null;

async function runAutoSync() {
    try {
        const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
        if (!consumerKey) {
            console.log('⏭️ Auto-sync skipped: MONETIZZE_CONSUMER_KEY not configured');
            return;
        }
        
        // Sync today and yesterday (to catch late-arriving transactions)
        const today = new Date();
        const yesterday = new Date(today);
        yesterday.setDate(yesterday.getDate() - 1);
        
        const startDate = yesterday.toISOString().split('T')[0];
        const endDate = today.toISOString().split('T')[0];
        
        console.log(`🔄 Auto-sync starting (${startDate} to ${endDate})...`);
        const result = await syncMonetizzeSalesCore(startDate, endDate);
        console.log(`✅ Auto-sync complete: ${result.synced} new/updated, ${result.skipped} skipped, ${result.total} total from Monetizze`);
        
        // After sync, check for newly approved transactions that need CAPI Purchase events
        if (result.synced > 0) {
            console.log('🔄 Auto-sync found new/updated transactions - running CAPI catch-up...');
            await sendMissingCAPIPurchases();
        }
    } catch (error) {
        console.error('❌ Auto-sync error:', error.message);
    }
}

async function runRefundBackfill() {
    try {
        console.log('🔄 Running automatic refund backfill...');
        const result = await pool.query(`
            SELECT t.transaction_id, t.email, t.phone, t.name, t.product, t.value, 
                   t.status, t.funnel_language, t.created_at
            FROM transactions t
            LEFT JOIN refund_requests rr ON rr.transaction_id = t.transaction_id
            WHERE t.status IN ('refunded', 'chargeback')
              AND rr.id IS NULL
            ORDER BY t.created_at DESC
        `);
        
        let created = 0;
        for (const tx of result.rows) {
            try {
                const refundProtocol = `MON-${String(tx.transaction_id).substring(0, 12).toUpperCase()}`;
                const refundType = tx.status === 'chargeback' ? 'chargeback' : 'refund';
                
                await pool.query(`
                    INSERT INTO refund_requests (
                        protocol, full_name, email, phone, product, reason, 
                        status, source, refund_type, transaction_id, value, funnel_language, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (protocol) DO NOTHING
                `, [
                    refundProtocol, tx.name || 'N/A', tx.email, tx.phone || null,
                    tx.product,
                    refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                    'approved', 'monetizze', refundType, String(tx.transaction_id),
                    parseFloat(tx.value) || 0, tx.funnel_language || 'en', tx.created_at || new Date()
                ]);
                created++;
            } catch (err) { /* skip duplicates */ }
        }
        
        if (created > 0) {
            console.log(`✅ Backfill: ${created} refund_requests created from ${result.rows.length} orphaned transactions`);
        }
    } catch (error) {
        console.error('⚠️ Refund backfill error:', error.message);
    }
}

async function runDeepSync() {
    // Deep sync strategy:
    // 1. Sync last 7 days day-by-day (full sync - catches all new transactions + status changes)
    // 2. Then do a targeted refund-only sync for last 60 days (only status 3,4 = cancelled/refunded)
    try {
        const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
        if (!consumerKey) return;
        
        const today = new Date();
        let totalSynced = 0;
        let totalSkipped = 0;
        let totalFetched = 0;
        
        // PHASE 1: Full sync of last 7 days (day-by-day to get ALL transactions including status changes)
        console.log(`🔍 Deep sync PHASE 1: Full sync of last 7 days (day-by-day)...`);
        for (let daysBack = 0; daysBack <= 7; daysBack++) {
            const targetDate = new Date(today);
            targetDate.setDate(targetDate.getDate() - daysBack);
            const dateStr = targetDate.toISOString().split('T')[0];
            
            try {
                const result = await syncMonetizzeSalesCore(dateStr, dateStr);
                totalSynced += result.synced || 0;
                totalSkipped += result.skipped || 0;
                totalFetched += result.total || 0;
                
                if (result.synced > 0 || result.total > 0) {
                    console.log(`📅 Day ${dateStr}: ${result.total} fetched, ${result.synced} synced`);
                }
            } catch (dayError) {
                console.error(`❌ Error syncing ${dateStr}:`, dayError.message);
            }
            
            await new Promise(resolve => setTimeout(resolve, 300));
        }
        
        console.log(`✅ Phase 1 complete: ${totalSynced} synced, ${totalFetched} fetched across 7 days`);
        
        // PHASE 2: Targeted refund sync for last 60 days
        // Only fetch status 3 (cancelled) and 4 (refunded/devolvida) - much fewer results
        console.log(`🔍 Deep sync PHASE 2: Targeted refund sync (last 60 days, only refunded/cancelled)...`);
        let refundSynced = 0;
        let refundFetched = 0;
        
        try {
            let monetizzeToken;
            try {
                monetizzeToken = await getMonetizzeToken();
            } catch (e) {
                console.error('❌ Phase 2: Monetizze auth failed');
                return;
            }
            
            const sixtyDaysAgo = new Date(today);
            sixtyDaysAgo.setDate(sixtyDaysAgo.getDate() - 60);
            const startDate = sixtyDaysAgo.toISOString().split('T')[0];
            const endDate = today.toISOString().split('T')[0];
            
            // Fetch ONLY refunded/cancelled transactions (status 3=cancelled, 4=refunded)
            const params = new URLSearchParams();
            params.append('date_min', `${startDate} 00:00:00`);
            params.append('date_max', `${endDate} 23:59:59`);
            // Only refund-related statuses
            ['3', '4'].forEach(s => params.append('status[]', s));
            
            const validProductCodes = [
                '341972', '349241', '349242', '349243',
                '330254', '341443', '341444', '341448',
                '349260', '349261', '349266', '349267',
                '338375', '341452', '341453', '341454'
            ];
            validProductCodes.forEach(code => params.append('product[]', code));
            
            const txUrl = `https://api.monetizze.com.br/2.1/transactions?${params.toString()}`;
            console.log(`🌐 Fetching refunded/cancelled transactions (${startDate} to ${endDate})...`);
            
            const response = await fetch(txUrl, {
                method: 'GET',
                headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
            });
            
            if (response.ok) {
                const data = await response.json();
                let refundArray = [];
                if (Array.isArray(data)) refundArray = data;
                else if (Array.isArray(data.dados)) refundArray = data.dados;
                else if (Array.isArray(data.vendas)) refundArray = data.vendas;
                
                console.log(`📄 Refund query: first page ${refundArray.length} items (paginas: ${data.paginas || 'N/A'})`);
                
                // Handle pagination for refunds
                const totalPages = data.paginas || 0;
                if (totalPages > 1) {
                    for (let page = 2; page <= Math.min(totalPages, 20); page++) {
                        try {
                            const pageParams = new URLSearchParams(params.toString());
                            pageParams.set('pagina', String(page));
                            const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                                method: 'GET',
                                headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                            });
                            if (pageResponse.ok) {
                                const pageData = await pageResponse.json();
                                const pageItems = pageData.dados || pageData.vendas || [];
                                refundArray = refundArray.concat(pageItems);
                                if (pageItems.length === 0) break;
                            }
                        } catch (e) { break; }
                    }
                } else if (refundArray.length >= 100 && totalPages === 0) {
                    // Probe for more pages
                    for (let page = 2; page <= 20; page++) {
                        try {
                            const pageParams = new URLSearchParams(params.toString());
                            pageParams.set('pagina', String(page));
                            const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                                method: 'GET',
                                headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                            });
                            if (pageResponse.ok) {
                                const pageData = await pageResponse.json();
                                const pageItems = Array.isArray(pageData) ? pageData : (pageData.dados || pageData.vendas || []);
                                if (pageItems.length === 0) break;
                                refundArray = refundArray.concat(pageItems);
                            } else break;
                        } catch (e) { break; }
                    }
                }
                
                console.log(`📦 Total refund/cancelled transactions fetched: ${refundArray.length}`);
                refundFetched = refundArray.length;
                
                // Process each refund transaction
                for (const item of refundArray) {
                    try {
                        const vendaData = item.venda || item;
                        const tipoEvento = item.tipoEvento || {};
                        const compradorData = item.comprador || {};
                        
                        const transactionId = String(vendaData.codigo || item.codigo || '');
                        const email = compradorData.email || '';
                        if (!transactionId || !email) continue;
                        
                        const statusCode = String(tipoEvento.codigo || vendaData.statusCodigo || '');
                        const vendaStatus = (vendaData.status || '').toLowerCase();
                        const eventoDesc = (tipoEvento.descricao || '').toLowerCase();
                        
                        // Determine status
                        let mappedStatus = 'cancelled';
                        if (statusCode === '4' || vendaStatus.includes('devolvida') || vendaStatus.includes('reembolso') || eventoDesc.includes('reembolso')) {
                            mappedStatus = 'refunded';
                        }
                        if (vendaStatus.includes('chargeback') || eventoDesc.includes('chargeback') || eventoDesc.includes('disputa') || statusCode === '8' || statusCode === '9') {
                            mappedStatus = 'chargeback';
                        }
                        
                        const productName = (item.produto || {}).nome || 'Unknown';
                        const value = vendaData.valorLiquido || vendaData.comissao || vendaData.valor || '0';
                        const buyerName = compradorData.nome || '';
                        const buyerPhone = compradorData.telefone || '';
                        
                        // Update or insert transaction (PRESERVE fbc/fbp/visitor_id from postback - don't overwrite)
                        const upsert = await pool.query(`
                            INSERT INTO transactions (transaction_id, email, phone, name, product, value, monetizze_status, status, raw_data, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                            ON CONFLICT (transaction_id) DO UPDATE SET
                                status = CASE 
                                    WHEN $8 IN ('refunded', 'chargeback') THEN $8
                                    WHEN transactions.status IN ('refunded', 'chargeback') THEN transactions.status
                                    ELSE $8
                                END,
                                monetizze_status = $7,
                                raw_data = $9,
                                updated_at = NOW()
                            RETURNING (xmax = 0) as is_insert, transactions.fbc, transactions.fbp, transactions.visitor_id
                        `, [
                            transactionId, email, buyerPhone, buyerName, productName,
                            value, statusCode, mappedStatus, JSON.stringify(item),
                            vendaData.dataInicio ? new Date(vendaData.dataInicio.replace(' ', 'T') + '-03:00') : new Date()
                        ]);
                        
                        // Create refund_requests for refunded/chargebacked
                        if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
                            const refundProtocol = `MON-${String(transactionId).substring(0, 12).toUpperCase()}`;
                            const existingRefund = await pool.query('SELECT id FROM refund_requests WHERE transaction_id = $1', [transactionId]);
                            
                            if (existingRefund.rows.length === 0) {
                                await pool.query(`
                                    INSERT INTO refund_requests (
                                        protocol, full_name, email, phone, product, reason,
                                        status, source, refund_type, transaction_id, value, created_at
                                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
                                    ON CONFLICT (protocol) DO NOTHING
                                `, [
                                    refundProtocol, buyerName || 'N/A', email, buyerPhone || null,
                                    productName,
                                    mappedStatus === 'chargeback' ? 'Chargeback - Disputa' : 'Reembolso via Monetizze',
                                    'approved', 'monetizze',
                                    mappedStatus === 'chargeback' ? 'chargeback' : 'refund',
                                    transactionId, parseFloat(value) || 0
                                ]);
                                refundSynced++;
                                console.log(`🔴 REFUND FOUND: ${transactionId} (${email}) → ${mappedStatus} - ${productName} - R$${value}`);
                            }
                        }
                    } catch (itemError) {
                        continue;
                    }
                }
            }
        } catch (phase2Error) {
            console.error('❌ Phase 2 error:', phase2Error.message);
        }
        
        console.log(`✅ Deep sync complete! Phase 1: ${totalSynced} synced/${totalFetched} fetched (7 days) | Phase 2: ${refundSynced} refunds found/${refundFetched} fetched (60 days)`);
    } catch (error) {
        console.error('❌ Deep sync error:', error.message);
    }
}

async function runDiagnosticLog() {
    try {
        console.log('\n========== 🔍 REFUND DIAGNOSTIC LOG ==========');
        
        const txStatus = await pool.query('SELECT status, COUNT(*) as count FROM transactions GROUP BY status ORDER BY count DESC');
        console.log('📊 Transactions by status:', JSON.stringify(txStatus.rows));
        
        const refReq = await pool.query('SELECT refund_type, source, status, COUNT(*) as count FROM refund_requests GROUP BY refund_type, source, status ORDER BY count DESC');
        console.log('📊 Refund requests:', JSON.stringify(refReq.rows));
        
        // Check all unique status combos from Monetizze raw data
        const distinctStatuses = await pool.query(`
            SELECT raw_data->'venda'->>'status' as venda_status, 
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao,
                   COUNT(*) as count
            FROM transactions WHERE raw_data IS NOT NULL
            GROUP BY raw_data->'venda'->>'status', raw_data->'tipoEvento'->>'codigo', raw_data->'tipoEvento'->>'descricao'
            ORDER BY count DESC
        `);
        console.log('📊 Distinct API statuses:', JSON.stringify(distinctStatuses.rows));
        
        // Broad search for refund keywords in ALL raw_data (cast to text)
        const broadSearch = await pool.query(`
            SELECT transaction_id, email, status, monetizze_status,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao
            FROM transactions 
            WHERE LOWER(CAST(raw_data AS text)) LIKE '%devolv%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%chargeback%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%reembolso%'
               OR monetizze_status IN ('4', '8', '9')
            LIMIT 20
        `);
        console.log('📊 Broad refund search results:', broadSearch.rows.length, 'found');
        if (broadSearch.rows.length > 0) {
            broadSearch.rows.forEach(r => console.log(`  🔴 ${r.transaction_id} | status: ${r.status} | monetizze: ${r.monetizze_status} | venda: ${r.venda_status_text} | evento: ${r.evento_codigo}/${r.evento_descricao}`));
        }
        
        // Check specific Monetizze IDs from the screenshot (5580, 5579, 5578, 5577, 5576)
        const specific = await pool.query(`
            SELECT transaction_id, email, status, monetizze_status
            FROM transactions
            WHERE transaction_id IN ('55803165', '55799093', '55785981', '55781116', '55781107', '55780560', '55779651', '55778218', '55777545', '55777542', '55777539', '55777537', '55764031')
        `);
        console.log('📊 Specific Monetizze IDs check:', specific.rows.length > 0 ? JSON.stringify(specific.rows) : 'NONE FOUND - these transactions are NOT in our DB');
        
        // Sample of cancelled transactions
        const cancelled = await pool.query(`
            SELECT transaction_id, email, monetizze_status,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao
            FROM transactions WHERE status = 'cancelled' ORDER BY created_at DESC LIMIT 5
        `);
        console.log('📊 Recent cancelled sample:', JSON.stringify(cancelled.rows));
        
        console.log('========== END DIAGNOSTIC ==========\n');
    } catch (error) {
        console.error('Diagnostic error:', error.message);
    }
}

// Re-check approved transactions by querying Monetizze API individually
// The Monetizze /transactions list API doesn't reflect chargeback status changes,
// but individual transaction queries or re-fetching by date MIGHT show updated status.
// As a workaround, we re-fetch transactions by venda.codigo to check for status changes.
async function recheckApprovedTransactions() {
    try {
        const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
        if (!consumerKey) return;
        
        console.log('🔍 Re-checking approved transactions for status changes...');
        
        // Get all approved transactions from the last 60 days
        const approved = await pool.query(`
            SELECT transaction_id, email, name, phone, product, value, funnel_language, created_at
            FROM transactions 
            WHERE status = 'approved' 
              AND created_at >= NOW() - INTERVAL '60 days'
            ORDER BY created_at DESC
        `);
        
        if (approved.rows.length === 0) {
            console.log('No approved transactions to re-check');
            return;
        }
        
        console.log(`📦 Re-checking ${approved.rows.length} approved transactions...`);
        
        // Get a fresh Monetizze token
        let monetizzeToken;
        try {
            monetizzeToken = await getMonetizzeToken();
        } catch (e) {
            console.error('❌ Cannot re-check: Monetizze auth failed');
            return;
        }
        
        let updated = 0;
        let refundsFound = 0;
        
        // Query each transaction individually using Monetizze API
        // We'll batch by querying each transaction_id (venda.codigo)
        for (const tx of approved.rows) {
            try {
                // Query Monetizze for this specific transaction
                const txUrl = `https://api.monetizze.com.br/2.1/transactions?codigo=${tx.transaction_id}`;
                const response = await fetch(txUrl, {
                    method: 'GET',
                    headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                });
                
                if (!response.ok) continue;
                
                const data = await response.json();
                const items = Array.isArray(data) ? data : (data.dados || data.vendas || []);
                
                if (items.length === 0) continue;
                
                const item = items[0];
                const vendaData = item.venda || item;
                const tipoEvento = item.tipoEvento || {};
                const vendaStatus = (vendaData.status || '').toLowerCase();
                const eventoDesc = (tipoEvento.descricao || '').toLowerCase();
                const eventoCodigo = String(tipoEvento.codigo || '');
                
                // Check if this transaction is now refunded or chargebacked
                const isRefund = eventoCodigo === '4' || vendaStatus.includes('devolvida') || vendaStatus.includes('reembolso');
                const isChargeback = eventoCodigo === '8' || eventoCodigo === '9' || 
                    vendaStatus.includes('chargeback') || eventoDesc.includes('chargeback') || 
                    eventoDesc.includes('disputa');
                
                if (isRefund || isChargeback) {
                    const newStatus = isChargeback ? 'chargeback' : 'refunded';
                    const refundType = isChargeback ? 'chargeback' : 'refund';
                    
                    console.log(`🔴 STATUS CHANGE DETECTED: ${tx.transaction_id} (${tx.email}) was approved → now ${newStatus} (vendaStatus: "${vendaData.status}", eventoDesc: "${tipoEvento.descricao}")`);
                    
                    // Update transaction status
                    await pool.query(
                        `UPDATE transactions SET status = $1, monetizze_status = $2, updated_at = NOW() WHERE transaction_id = $3`,
                        [newStatus, eventoCodigo || '4', tx.transaction_id]
                    );
                    
                    // Create refund_requests entry
                    const refundProtocol = `MON-${String(tx.transaction_id).substring(0, 12).toUpperCase()}`;
                    const existing = await pool.query('SELECT id FROM refund_requests WHERE transaction_id = $1', [tx.transaction_id]);
                    
                    if (existing.rows.length === 0) {
                        await pool.query(`
                            INSERT INTO refund_requests (
                                protocol, full_name, email, phone, product, reason, 
                                status, source, refund_type, transaction_id, value, funnel_language, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                            ON CONFLICT (protocol) DO NOTHING
                        `, [
                            refundProtocol, tx.name || 'N/A', tx.email, tx.phone || null,
                            tx.product,
                            isChargeback ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                            'approved', 'monetizze', refundType, tx.transaction_id,
                            parseFloat(tx.value) || 0, tx.funnel_language || 'en', tx.created_at || new Date()
                        ]);
                        refundsFound++;
                    }
                    
                    updated++;
                }
                
                // Small delay to avoid rate limiting Monetizze API
                await new Promise(resolve => setTimeout(resolve, 200));
                
            } catch (txError) {
                // Skip errors for individual transactions
                continue;
            }
        }
        
        console.log(`✅ Re-check complete: ${updated} status changes found, ${refundsFound} refund_requests created (out of ${approved.rows.length} checked)`);
        
    } catch (error) {
        console.error('❌ Re-check error:', error.message);
    }
}

// Re-process old postback logs to fix entries that were incorrectly mapped due to the priority bug
// (refund/chargeback status was being overridden to 'approved' by venda.status text check)
async function reprocessPostbackLogs() {
    try {
        console.log('🔄 Reprocessing postback logs for missed refunds/chargebacks...');
        
        // Fetch all postback logs
        const logs = await pool.query(`
            SELECT id, body, created_at FROM postback_logs 
            WHERE content_type != 'ERROR_NO_EMAIL' 
              AND content_type != 'CRITICAL_ERROR'
              AND content_type != 'DB_ERROR'
            ORDER BY created_at DESC
        `);
        
        if (logs.rows.length === 0) {
            console.log('No postback logs to reprocess');
            return;
        }
        
        console.log(`📦 Found ${logs.rows.length} postback logs to scan`);
        let fixed = 0;
        
        for (const log of logs.rows) {
            try {
                let body;
                if (typeof log.body === 'string') {
                    body = JSON.parse(log.body);
                } else {
                    body = log.body;
                }
                
                if (!body) continue;
                
                // Check for dot-notation keys
                const hasDotKeys = Object.keys(body).some(k => k.includes('.'));
                function unflattenObj(obj) {
                    const result = {};
                    for (const key of Object.keys(obj)) {
                        if (key.includes('.')) {
                            const parts = key.split('.');
                            let current = result;
                            for (let i = 0; i < parts.length - 1; i++) {
                                if (!current[parts[i]] || typeof current[parts[i]] !== 'object') current[parts[i]] = {};
                                current = current[parts[i]];
                            }
                            current[parts[parts.length - 1]] = obj[key];
                        } else if (result[key] === undefined) {
                            result[key] = obj[key];
                        }
                    }
                    return result;
                }
                const parsed = hasDotKeys ? { ...unflattenObj(body), ...body } : body;
                
                const tipoEvento = (parsed.tipoEvento && typeof parsed.tipoEvento === 'object') ? parsed.tipoEvento : {};
                const venda = (parsed.venda && typeof parsed.venda === 'object') ? parsed.venda : {};
                const comprador = (parsed.comprador && typeof parsed.comprador === 'object') ? parsed.comprador : {};
                
                const statusCode = String(tipoEvento.codigo || parsed['tipoEvento.codigo'] || parsed['tipoEvento[codigo]'] || '');
                const eventoDesc = (tipoEvento.descricao || '').toLowerCase();
                const vendaStatus = (venda.status || '').toLowerCase();
                const transactionId = parsed.chave_unica || venda.codigo || parsed['venda.codigo'] || '';
                const email = comprador.email || parsed.email || parsed['comprador.email'] || '';
                
                if (!transactionId || !email) continue;
                
                // Detect refund/chargeback
                let newStatus = null;
                
                if (statusCode === '4' || vendaStatus.includes('devolvida') || vendaStatus.includes('reembolso') || eventoDesc.includes('reembolso')) {
                    newStatus = 'refunded';
                }
                if (statusCode === '8' || statusCode === '9' || vendaStatus.includes('chargeback') || eventoDesc.includes('chargeback') || eventoDesc.includes('disputa')) {
                    newStatus = 'chargeback';
                }
                
                if (!newStatus) continue;
                
                // Check if this transaction exists in our DB with wrong status
                const existing = await pool.query(
                    `SELECT transaction_id, status FROM transactions WHERE transaction_id = $1`,
                    [transactionId]
                );
                
                if (existing.rows.length > 0 && existing.rows[0].status !== newStatus) {
                    console.log(`🔧 FIXING postback log ${log.id}: tx ${transactionId} (${email}) - was "${existing.rows[0].status}" → should be "${newStatus}" (code: ${statusCode})`);
                    
                    // Update transaction status
                    await pool.query(
                        `UPDATE transactions SET status = $1, monetizze_status = $2, updated_at = NOW() WHERE transaction_id = $3`,
                        [newStatus, statusCode, transactionId]
                    );
                    
                    // Create refund_requests entry
                    const refundProtocol = `MON-${String(transactionId).substring(0, 12).toUpperCase()}`;
                    const existingRefund = await pool.query('SELECT id FROM refund_requests WHERE transaction_id = $1', [transactionId]);
                    
                    if (existingRefund.rows.length === 0) {
                        const produto = (parsed.produto && typeof parsed.produto === 'object') ? parsed.produto : {};
                        const productName = produto.nome || parsed['produto.nome'] || 'Unknown Product';
                        const valor = venda.valor || parsed.valor || parsed['venda.valor'] || '0';
                        const buyerName = comprador.nome || parsed.nome || parsed['comprador.nome'] || 'N/A';
                        const buyerPhone = comprador.telefone || parsed.telefone || parsed['comprador.telefone'] || null;
                        
                        await pool.query(`
                            INSERT INTO refund_requests (
                                protocol, full_name, email, phone, product, reason, 
                                status, source, refund_type, transaction_id, value, funnel_language, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                            ON CONFLICT (protocol) DO NOTHING
                        `, [
                            refundProtocol, buyerName, email, buyerPhone,
                            productName,
                            newStatus === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                            'approved', 'monetizze', newStatus === 'chargeback' ? 'chargeback' : 'refund',
                            transactionId, parseFloat(valor) || 0, 'en', log.created_at || new Date()
                        ]);
                    }
                    
                    fixed++;
                } else if (existing.rows.length === 0) {
                    // Transaction doesn't exist - it may have been skipped
                    console.log(`⚠️ Postback log ${log.id}: tx ${transactionId} (${email}) is ${newStatus} but NOT in transactions table`);
                }
            } catch (logError) {
                // Skip individual log errors
                continue;
            }
        }
        
        console.log(`✅ Postback log reprocessing complete: ${fixed} transactions fixed`);
    } catch (error) {
        console.error('❌ Postback reprocessing error:', error.message);
    }
}

// ==================== CAPI CATCH-UP: Send Purchase events for approved sales missing from capi_purchase_logs ====================
// This handles the case where background sync updates a transaction to 'approved' but doesn't trigger CAPI
// Lock to prevent concurrent execution of CAPI catch-up
let capiCatchupRunning = false;
let capiCatchupLastRun = 0;
const CAPI_CATCHUP_MIN_INTERVAL = 30000; // minimum 30 seconds between runs

async function sendMissingCAPIPurchases() {
    // Prevent concurrent execution (race condition protection)
    if (capiCatchupRunning) {
        console.log('⏳ CAPI CATCH-UP: Already running, skipping this call');
        return;
    }
    
    // Prevent too-frequent runs
    const now = Date.now();
    if (now - capiCatchupLastRun < CAPI_CATCHUP_MIN_INTERVAL) {
        console.log('⏳ CAPI CATCH-UP: Last run was less than 30s ago, skipping');
        return;
    }
    
    capiCatchupRunning = true;
    capiCatchupLastRun = now;
    
    try {
        console.log('🔍 CAPI CATCH-UP: Checking for approved transactions missing Purchase CAPI events...');
        
        // Find approved transactions from the last 7 days that DON'T have a capi_purchase_logs entry
        const missingResult = await pool.query(`
            SELECT t.transaction_id, t.email, t.phone, t.name, t.product, t.value, 
                   t.funnel_language, t.funnel_source, t.raw_data, t.created_at,
                   t.fbc AS tx_fbc, t.fbp AS tx_fbp, t.visitor_id AS tx_visitor_id
            FROM transactions t
            LEFT JOIN capi_purchase_logs c ON t.transaction_id = c.transaction_id
            WHERE t.status = 'approved' 
              AND c.transaction_id IS NULL
              AND t.created_at >= NOW() - INTERVAL '7 days'
              AND t.email IS NOT NULL
            ORDER BY t.created_at DESC
        `);
        
        if (missingResult.rows.length === 0) {
            console.log('✅ CAPI CATCH-UP: No missing Purchase events found. All approved sales have CAPI logs.');
            return;
        }
        
        console.log(`📤 CAPI CATCH-UP: Found ${missingResult.rows.length} approved transactions without CAPI Purchase events. Sending now...`);
        
        const brlToUsdRate = parseFloat(process.env.CONVERSION_BRL_TO_USD || '0.18');
        let sent = 0;
        let failed = 0;
        
        for (const tx of missingResult.rows) {
            try {
                const email = tx.email;
                const transactionId = tx.transaction_id;
                const funnelLanguage = tx.funnel_language || 'en';
                const funnelSource = tx.funnel_source || 'main';
                const productName = tx.product || 'Unknown Product';
                
                // Try to extract product code from raw_data
                let productCode = null;
                try {
                    const rawData = typeof tx.raw_data === 'string' ? JSON.parse(tx.raw_data) : tx.raw_data;
                    if (rawData) {
                        productCode = rawData.produto?.codigo || rawData['produto.codigo'] || null;
                    }
                } catch (e) { /* ignore parse errors */ }
                
                // ===== LEAD MATCHING (simplified version of postback handler) =====
                let leadData = null;
                let matchMethod = 'none';
                
                // Level 1: Match by email in leads table
                if (email) {
                    const leadResult = await pool.query(
                        `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id, referrer
                         FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                        [email]
                    );
                    if (leadResult.rows.length > 0) {
                        leadData = leadResult.rows[0];
                        matchMethod = 'email';
                    }
                }
                
                // Level 2: Match by phone in leads table
                if (!leadData && tx.phone) {
                    const cleanPhone = tx.phone.replace(/\D/g, '');
                    if (cleanPhone.length >= 7) {
                        const phoneResult = await pool.query(
                            `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id, referrer
                             FROM leads WHERE REPLACE(REPLACE(REPLACE(whatsapp, '+', ''), '-', ''), ' ', '') LIKE $1 
                             ORDER BY created_at DESC LIMIT 1`,
                            [`%${cleanPhone.slice(-7)}%`]
                        );
                        if (phoneResult.rows.length > 0) {
                            leadData = phoneResult.rows[0];
                            matchMethod = 'phone';
                        }
                    }
                }
                
                // Level 3: Try funnel_events for fbc/fbp by IP match
                if (!leadData) {
                    // Try to extract buyer IP from raw_data
                    let buyerIp = null;
                    try {
                        const rawData = typeof tx.raw_data === 'string' ? JSON.parse(tx.raw_data) : tx.raw_data;
                        buyerIp = rawData?.comprador?.ip || rawData?.['comprador.ip'] || null;
                    } catch (e) { /* ignore */ }
                    
                    if (buyerIp) {
                        const eventResult = await pool.query(
                            `SELECT visitor_id, ip_address, user_agent, fbc, fbp 
                             FROM funnel_events WHERE ip_address = $1 
                             ORDER BY created_at DESC LIMIT 1`,
                            [buyerIp]
                        );
                        if (eventResult.rows.length > 0) {
                            const eventRow = eventResult.rows[0];
                            leadData = {
                                ip_address: eventRow.ip_address, user_agent: eventRow.user_agent,
                                fbc: eventRow.fbc, fbp: eventRow.fbp,
                                country_code: null, city: null, state: null, target_gender: null,
                                name: tx.name, whatsapp: tx.phone, visitor_id: eventRow.visitor_id,
                                funnel_language: funnelLanguage, referrer: null
                            };
                            matchMethod = 'ip_events';
                        }
                    }
                }
                
                // Level 4: Get fbc/fbp/vid - FIRST from transactions table columns (saved by postback), then fallback to raw_data parsing
                let rawFbc = tx.tx_fbc || null;
                let rawFbp = tx.tx_fbp || null;
                let rawVid = tx.tx_visitor_id || null;
                
                // If not in transactions columns, try raw_data JSON (postback body)
                if (!rawFbc || !rawFbp) {
                    try {
                        const rawData = typeof tx.raw_data === 'string' ? JSON.parse(tx.raw_data) : tx.raw_data;
                        if (rawData) {
                            const venda = (rawData.venda && typeof rawData.venda === 'object') ? rawData.venda : {};
                            if (!rawFbc) rawFbc = rawData.zs_fbc || venda.zs_fbc || rawData['zs_fbc'] || null;
                            if (!rawFbp) rawFbp = rawData.zs_fbp || venda.zs_fbp || rawData['zs_fbp'] || null;
                            if (!rawVid) rawVid = rawData.vid || venda.vid || rawData['vid'] || null;
                            
                            // Build fbc from fbclid if zs_fbc not available
                            if (!rawFbc) {
                                const fbclid = rawData.fbclid || venda.fbclid || null;
                                if (fbclid) {
                                    rawFbc = `fb.1.${Date.now()}.${fbclid}`;
                                }
                            }
                        }
                    } catch (e) { /* ignore parse errors */ }
                }
                
                // If no lead found but we have raw_data params, create minimal lead
                if (!leadData && (rawFbc || rawFbp)) {
                    leadData = {
                        ip_address: null, user_agent: null,
                        fbc: rawFbc, fbp: rawFbp,
                        country_code: null, city: null, state: null, target_gender: null,
                        name: tx.name, whatsapp: tx.phone, visitor_id: rawVid,
                        funnel_language: funnelLanguage, referrer: null
                    };
                    matchMethod = 'raw_data_params';
                }
                
                // ENRICHMENT: If lead found but missing fbc/fbp, try multiple sources
                if (leadData && (!leadData.fbc || !leadData.fbp)) {
                    // Try raw_data params first (most reliable for this transaction)
                    if (!leadData.fbc && rawFbc) leadData.fbc = rawFbc;
                    if (!leadData.fbp && rawFbp) leadData.fbp = rawFbp;
                    
                    // Try funnel_events by visitor_id
                    const visitorId = leadData.visitor_id || rawVid;
                    if ((!leadData.fbc || !leadData.fbp) && visitorId) {
                        try {
                            const enrichResult = await pool.query(
                                `SELECT fbc, fbp, ip_address, user_agent 
                                 FROM funnel_events WHERE visitor_id = $1 AND (fbc IS NOT NULL OR fbp IS NOT NULL)
                                 ORDER BY created_at DESC LIMIT 1`,
                                [visitorId]
                            );
                            if (enrichResult.rows.length > 0) {
                                const enrichRow = enrichResult.rows[0];
                                if (!leadData.fbc && enrichRow.fbc) leadData.fbc = enrichRow.fbc;
                                if (!leadData.fbp && enrichRow.fbp) leadData.fbp = enrichRow.fbp;
                                if (!leadData.ip_address && enrichRow.ip_address) leadData.ip_address = enrichRow.ip_address;
                                if (!leadData.user_agent && enrichRow.user_agent) leadData.user_agent = enrichRow.user_agent;
                            }
                        } catch (enrichErr) { /* non-blocking */ }
                    }
                }
                
                if (leadData) {
                    console.log(`📊 CAPI CATCH-UP: Lead matched [${matchMethod}] for ${email} (tx: ${transactionId}) - fbc=${leadData.fbc ? 'Yes' : 'No'}, fbp=${leadData.fbp ? 'Yes' : 'No'}, IP=${leadData.ip_address ? 'Yes' : 'No'}, txFbc=${tx.tx_fbc ? 'Yes' : 'No'}, txFbp=${tx.tx_fbp ? 'Yes' : 'No'}`);
                } else {
                    console.log(`📊 CAPI CATCH-UP: No lead found for ${email} (tx: ${transactionId}) - txFbc=${tx.tx_fbc ? 'Yes' : 'No'}, txFbp=${tx.tx_fbp ? 'Yes' : 'No'}, rawFbc=${rawFbc ? 'Yes' : 'No'}, rawFbp=${rawFbp ? 'Yes' : 'No'}`);
                }
                
                // Build Facebook user data
                const fbUserData = {
                    email: email,
                    phone: leadData?.whatsapp || tx.phone,
                    firstName: leadData?.name || tx.name,
                    ip: leadData?.ip_address || null,
                    userAgent: leadData?.user_agent || null,
                    fbc: leadData?.fbc || null,
                    fbp: leadData?.fbp || null,
                    country: leadData?.country_code || null,
                    city: leadData?.city || null,
                    state: leadData?.state || null,
                    gender: leadData?.target_gender || null,
                    externalId: leadData?.visitor_id || null,
                    referrer: leadData?.referrer || null
                };
                
                // Convert value to USD
                const rawValue = parseFloat(tx.value) || 0;
                let valueUSD;
                
                if (funnelSource === 'perfectpay') {
                    // PerfectPay may store values in USD (international) or BRL
                    // Check raw_data for currency info
                    let isPerfectPayBRL = true; // default to BRL
                    try {
                        const rawData = typeof tx.raw_data === 'string' ? JSON.parse(tx.raw_data) : tx.raw_data;
                        if (rawData) {
                            const currencyEnum = rawData.currency_enum || rawData.sale_currency_enum;
                            // currency_enum 1 = BRL, others may be USD
                            if (currencyEnum && currencyEnum !== 1 && currencyEnum !== '1') {
                                isPerfectPayBRL = false;
                            }
                        }
                    } catch (e) {}
                    
                    valueUSD = isPerfectPayBRL 
                        ? Math.round((rawValue * brlToUsdRate) * 100) / 100 
                        : rawValue;
                    console.log(`💱 CAPI CATCH-UP: PerfectPay value: ${rawValue} -> USD: ${valueUSD} (isBRL: ${isPerfectPayBRL})`);
                } else {
                    // Monetizze values are always in BRL
                    valueUSD = Math.round((rawValue * brlToUsdRate) * 100) / 100;
                }
                
                // Skip $0 purchases (invalid/test transactions)
                if (valueUSD <= 0) {
                    console.log(`⏭️ CAPI CATCH-UP: Skipping ${transactionId} - value is $0 or negative`);
                    continue;
                }
                
                // Build Facebook custom data
                const fbCustomData = {
                    content_name: productName,
                    content_ids: [productCode || transactionId],
                    content_type: 'product',
                    value: valueUSD,
                    currency: 'USD',
                    order_id: transactionId,
                    num_items: 1,
                    customer_segmentation: 'new_customer_to_business'
                };
                
                // Build event source URL (MUST match the domain where the pixel fires)
                let eventSourceUrl;
                if (funnelSource === 'perfectpay') {
                    eventSourceUrl = funnelLanguage === 'es' 
                        ? 'https://perfect.zappdetect.com/espanhol/' 
                        : 'https://perfect.zappdetect.com/ingles/';
                } else if (funnelSource === 'affiliate') {
                    eventSourceUrl = funnelLanguage === 'es' 
                        ? 'https://afiliado.whatstalker.com/espanhol/' 
                        : 'https://afiliado.whatstalker.com/ingles/';
                } else {
                    eventSourceUrl = funnelLanguage === 'es' 
                        ? 'https://espanhol.zappdetect.com/' 
                        : 'https://ingles.zappdetect.com/';
                }
                
                // Event ID (status-agnostic for dedup) - use correct prefix per source
                const eventPrefix = funnelSource === 'perfectpay' ? 'perfectpay' : 'monetizze';
                const purchaseEventId = `${eventPrefix}_${transactionId}_purchase`;
                
                // CRITICAL: Double-check that this transaction hasn't been logged while we were processing
                // This prevents race conditions when multiple catch-ups overlap
                const doubleCheck = await pool.query(
                    'SELECT 1 FROM capi_purchase_logs WHERE transaction_id = $1 LIMIT 1',
                    [transactionId]
                );
                if (doubleCheck.rows.length > 0) {
                    console.log(`⏭️ CAPI CATCH-UP: Skipping ${transactionId} - already logged (race condition prevented)`);
                    continue;
                }
                
                // Use the actual sale date from the transaction
                const capiOptions = { language: funnelLanguage, eventTime: tx.created_at || null };
                
                // SEND Purchase event
                console.log(`📤 CAPI CATCH-UP: Sending Purchase for ${email} (tx: ${transactionId}, value: $${valueUSD}, lang: ${funnelLanguage})...`);
                const purchaseResults = await sendToFacebookCAPI('Purchase', fbUserData, fbCustomData, eventSourceUrl, purchaseEventId, capiOptions);
                
                // Determine success
                const firstResult = purchaseResults[0] || {};
                const capiSuccess = firstResult.success === true;
                const fbEventsReceived = firstResult.result?.events_received || 0;
                const pixelId = firstResult.pixel || '';
                const pixelName = funnelLanguage === 'es' ? 'PIXEL SPY ESPANHOL' : '[PABLO NOVO] - [SPY INGLES]';
                
                // Attribution data
                const purchaseAttrData = {
                    hasEmail: !!email,
                    hasFbc: !!(leadData?.fbc),
                    hasFbp: !!(leadData?.fbp),
                    hasIp: !!(leadData?.ip_address),
                    hasUa: !!(leadData?.user_agent),
                    hasExternalId: !!(leadData?.visitor_id),
                    hasCountry: !!(leadData?.country_code),
                    hasPhone: !!(leadData?.whatsapp || tx.phone),
                    leadFound: !!leadData
                };
                
                console.log(`📤 CAPI CATCH-UP: Result for ${email}: ${capiSuccess ? '✅ SUCCESS' : '❌ FAILED'} (events_received: ${fbEventsReceived})`);
                
                // Save to capi_purchase_logs (ON CONFLICT prevents duplicate entries)
                try {
                    await pool.query(`
                        INSERT INTO capi_purchase_logs (
                            transaction_id, email, product, value, currency,
                            funnel_language, funnel_source, event_source_url, event_id,
                            pixel_id, pixel_name,
                            has_email, has_fbc, has_fbp, has_ip, has_user_agent,
                            has_external_id, has_country, has_phone, lead_found,
                            capi_success, capi_response, fb_events_received, match_method
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
                        ON CONFLICT (transaction_id) DO NOTHING
                    `, [
                        transactionId, email, productName,
                        fbCustomData.value, fbCustomData.currency,
                        funnelLanguage, funnelSource, eventSourceUrl, purchaseEventId,
                        pixelId, pixelName,
                        purchaseAttrData.hasEmail, purchaseAttrData.hasFbc, purchaseAttrData.hasFbp,
                        purchaseAttrData.hasIp, purchaseAttrData.hasUa,
                        purchaseAttrData.hasExternalId, purchaseAttrData.hasCountry,
                        purchaseAttrData.hasPhone, purchaseAttrData.leadFound,
                        capiSuccess, JSON.stringify(purchaseResults), fbEventsReceived, matchMethod
                    ]);
                } catch (logErr) {
                    console.error(`CAPI CATCH-UP: Error saving log for ${transactionId}:`, logErr.message);
                }
                
                if (capiSuccess) sent++;
                else failed++;
                
                // Small delay between CAPI calls to avoid rate limits
                await new Promise(resolve => setTimeout(resolve, 500));
                
            } catch (txErr) {
                console.error(`CAPI CATCH-UP: Error processing tx ${tx.transaction_id}:`, txErr.message);
                failed++;
            }
        }
        
        console.log(`✅ CAPI CATCH-UP complete: ${sent} sent, ${failed} failed, ${missingResult.rows.length} total checked`);
        
    } catch (error) {
        console.error('❌ CAPI CATCH-UP error:', error.message);
    } finally {
        capiCatchupRunning = false;
    }
}

// Backfill fbc/fbp/visitor_id from raw_data for transactions that have it in their postback body but not in the columns
async function backfillTransactionFbcFbp() {
    try {
        console.log('🔄 Backfilling fbc/fbp/visitor_id from raw_data to transactions table...');
        const result = await pool.query(`
            SELECT transaction_id, raw_data FROM transactions
            WHERE (fbc IS NULL OR fbp IS NULL OR visitor_id IS NULL)
              AND raw_data IS NOT NULL
              AND created_at >= NOW() - INTERVAL '30 days'
        `);
        
        if (result.rows.length === 0) {
            console.log('✅ Backfill: No transactions need fbc/fbp/vid update.');
            return;
        }
        
        let updated = 0;
        for (const tx of result.rows) {
            try {
                const rawData = typeof tx.raw_data === 'string' ? JSON.parse(tx.raw_data) : tx.raw_data;
                if (!rawData) continue;
                
                const venda = (rawData.venda && typeof rawData.venda === 'object') ? rawData.venda : {};
                let fbc = rawData.zs_fbc || venda.zs_fbc || rawData['zs_fbc'] || null;
                const fbp = rawData.zs_fbp || venda.zs_fbp || rawData['zs_fbp'] || null;
                const vid = rawData.vid || venda.vid || rawData['vid'] || null;
                
                // Build fbc from fbclid if not available
                if (!fbc) {
                    const fbclid = rawData.fbclid || venda.fbclid || null;
                    if (fbclid) fbc = `fb.1.${Date.now()}.${fbclid}`;
                }
                
                if (fbc || fbp || vid) {
                    await pool.query(`
                        UPDATE transactions SET 
                            fbc = COALESCE(transactions.fbc, $2),
                            fbp = COALESCE(transactions.fbp, $3),
                            visitor_id = COALESCE(transactions.visitor_id, $4),
                            updated_at = NOW()
                        WHERE transaction_id = $1 AND (fbc IS NULL OR fbp IS NULL OR visitor_id IS NULL)
                    `, [tx.transaction_id, fbc, fbp, vid]);
                    updated++;
                }
            } catch (e) { /* skip individual errors */ }
        }
        
        console.log(`✅ Backfill complete: ${updated}/${result.rows.length} transactions updated with fbc/fbp/vid from raw_data.`);
    } catch (error) {
        console.error('❌ Backfill error:', error.message);
    }
}

function startAutoSync() {
    // Run backfill + CAPI catch-up after startup (10 seconds after start)
    setTimeout(async () => {
        // First, backfill fbc/fbp from raw_data into transactions columns
        await backfillTransactionFbcFbp();
        
        console.log('🚀 Running CAPI catch-up on startup...');
        await sendMissingCAPIPurchases();
        // Schedule recurring CAPI catch-up every 30 minutes (reduced from 10min to prevent excessive sends)
        setInterval(sendMissingCAPIPurchases, 30 * 60 * 1000);
    }, 10000);
    
    // Run heavy startup tasks 30 seconds after server start
    setTimeout(async () => {
        // Step 1: Deep sync last 7 days (most important - gets sales data into DB)
        await runDeepSync();
        
        // CRITICAL: Set up auto-sync IMMEDIATELY after deep sync, before heavy CAPI tasks
        // This ensures sales data stays fresh every 5 minutes even while CAPI catch-up runs
        autoSyncInterval = setInterval(runAutoSync, 5 * 60 * 1000);
        setInterval(recheckApprovedTransactions, 6 * 60 * 60 * 1000);
        console.log('🔄 Auto-sync scheduled: every 5 minutes | Re-check: every 6 hours | CAPI catch-up: every 30 min');
        
        // Step 2: Run one immediate auto-sync to catch any transactions that arrived during restart
        try { await runAutoSync(); } catch(e) { console.error('Initial auto-sync error:', e.message); }
        
        // Step 3+: Background tasks (non-blocking for data freshness)
        try { await reprocessPostbackLogs(); } catch(e) { console.error('Reprocess error:', e.message); }
        try { await recheckApprovedTransactions(); } catch(e) { console.error('Recheck error:', e.message); }
        try { await runRefundBackfill(); } catch(e) { console.error('Refund backfill error:', e.message); }
        try { await runDiagnosticLog(); } catch(e) { console.error('Diagnostic error:', e.message); }
        // CAPI catch-up runs last (heavy task, not blocking data display)
        try { await sendMissingCAPIPurchases(); } catch(e) { console.error('CAPI catch-up error:', e.message); }
    }, 30000);
}

// Manual CAPI catch-up trigger (admin only) - sends Purchase events for approved sales missing from capi_purchase_logs
app.post('/api/admin/capi-catchup', authenticateToken, requireAdmin, async (req, res) => {
    try {
        console.log('🔄 Manual CAPI catch-up triggered by admin...');
        await sendMissingCAPIPurchases();
        res.json({ success: true, message: 'CAPI catch-up executado. Verifique os logs de Purchase.' });
    } catch (error) {
        console.error('CAPI catch-up error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ==================== ENRICH PURCHASE: Capture fbc/fbp from thank-you/upsell page ====================
// Called from upsell pages where the buyer's browser still has _fbc/_fbp cookies
// This associates the Facebook click/browser IDs with the transaction for CAPI attribution
app.post('/api/enrich-purchase', async (req, res) => {
    try {
        const { email, fbc, fbp, visitorId, ip, userAgent } = req.body;
        
        if (!email) {
            return res.status(400).json({ success: false, error: 'email required' });
        }
        
        if (!fbc && !fbp && !visitorId) {
            return res.status(200).json({ success: true, message: 'no enrichment data' });
        }
        
        console.log(`📊 ENRICH-PURCHASE: ${email} - fbc=${fbc ? 'Yes' : 'No'}, fbp=${fbp ? 'Yes' : 'No'}, vid=${visitorId || 'none'}`);
        
        // Update ALL recent transactions for this email that are missing fbc/fbp
        const result = await pool.query(`
            UPDATE transactions SET
                fbc = COALESCE(transactions.fbc, $2),
                fbp = COALESCE(transactions.fbp, $3),
                visitor_id = COALESCE(transactions.visitor_id, $4),
                updated_at = NOW()
            WHERE LOWER(email) = LOWER($1)
              AND created_at >= NOW() - INTERVAL '24 hours'
              AND (fbc IS NULL OR fbp IS NULL OR visitor_id IS NULL)
        `, [email, fbc || null, fbp || null, visitorId || null]);
        
        const updated = result.rowCount || 0;
        console.log(`📊 ENRICH-PURCHASE: Updated ${updated} transactions for ${email}`);
        
        // Also update the lead record with fbc/fbp if missing
        if (fbc || fbp) {
            try {
                await pool.query(`
                    UPDATE leads SET
                        fbc = COALESCE(leads.fbc, $2),
                        fbp = COALESCE(leads.fbp, $3),
                        visitor_id = COALESCE(leads.visitor_id, $4),
                        ip_address = COALESCE(leads.ip_address, $5),
                        user_agent = COALESCE(leads.user_agent, $6),
                        updated_at = NOW()
                    WHERE LOWER(email) = LOWER($1)
                      AND (fbc IS NULL OR fbp IS NULL)
                `, [email, fbc || null, fbp || null, visitorId || null, ip || null, userAgent || null]);
            } catch (leadErr) { /* non-blocking */ }
        }
        
        // TRIGGER CAPI: Check for transactions that need CAPI Purchase events
        if (fbc || fbp) {
            let triggerCatchup = false;
            
            // Case 1: Approved transactions without ANY capi_purchase_logs
            // (Postback used delayed send, and we arrived before it fired - this is the common/happy path)
            const missingCapi = await pool.query(`
                SELECT t.transaction_id FROM transactions t
                LEFT JOIN capi_purchase_logs c ON t.transaction_id = c.transaction_id
                WHERE LOWER(t.email) = LOWER($1) AND t.status = 'approved' AND c.transaction_id IS NULL
                  AND t.created_at >= NOW() - INTERVAL '7 days'
            `, [email]);
            
            if (missingCapi.rows.length > 0) {
                console.log(`🔥 ENRICH-PURCHASE: Found ${missingCapi.rows.length} approved transactions for ${email} missing CAPI - triggering immediate catch-up...`);
                triggerCatchup = true;
            }
            
            // Case 2: CAPI was already sent but WITHOUT fbc
            // Previously we deleted logs and re-sent, but this causes DUPLICATE events in Facebook Ads Manager
            // Now we just log it - the event was already sent and Facebook has it
            if (fbc) {
                try {
                    const staleCapiLogs = await pool.query(`
                        SELECT c.id, c.transaction_id FROM capi_purchase_logs c
                        JOIN transactions t ON c.transaction_id = t.transaction_id
                        WHERE LOWER(t.email) = LOWER($1) AND c.has_fbc = false
                          AND t.created_at >= NOW() - INTERVAL '7 days'
                    `, [email]);
                    
                    if (staleCapiLogs.rows.length > 0) {
                        // Just update the logs with the new fbc/fbp data, DO NOT delete and re-send
                        for (const staleLog of staleCapiLogs.rows) {
                            await pool.query(`UPDATE capi_purchase_logs SET has_fbc = true, has_fbp = true WHERE id = $1`, [staleLog.id]);
                        }
                        console.log(`📝 ENRICH-PURCHASE: Updated ${staleCapiLogs.rows.length} CAPI logs with fbc/fbp for ${email} (no re-send to avoid duplication)`);
                        // DO NOT trigger catch-up - event already sent
                    }
                } catch (staleErr) {
                    console.error('ENRICH-PURCHASE stale logs error:', staleErr.message);
                }
            }
            
            if (triggerCatchup) {
                // Trigger catch-up asynchronously (don't block the response)
                sendMissingCAPIPurchases().catch(err => console.error('CAPI catch-up error:', err.message));
            }
        }
        
        res.json({ success: true, updated, message: `${updated} transactions enriched` });
    } catch (error) {
        console.error('ENRICH-PURCHASE error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Clear CAPI logs missing FBC so they can be resent with correct data
app.post('/api/admin/capi-clear-resend', authenticateToken, requireAdmin, async (req, res) => {
    try {
        console.log('🗑️ Admin requested: clear CAPI logs without FBC for resend...');
        
        // Delete logs that were sent without fbc (these will be resent by catch-up with raw_data extraction)
        const result = await pool.query(
            `DELETE FROM capi_purchase_logs WHERE has_fbc = false`
        );
        const deleted = result.rowCount || 0;
        
        console.log(`🗑️ Deleted ${deleted} CAPI logs without FBC. Running backfill + catch-up to resend...`);
        
        // First backfill fbc/fbp from raw_data into transactions columns
        await backfillTransactionFbcFbp();
        
        // Then run catch-up to resend them with correct fbc/fbp
        await sendMissingCAPIPurchases();
        
        res.json({ success: true, message: `${deleted} eventos limpos e reenviados com FBC/FBP.`, deleted });
    } catch (error) {
        console.error('CAPI clear-resend error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Sync sales from Monetizze API (protected - admin only)
// Uses 2-step auth: GET /token with X_CONSUMER_KEY → then GET /transactions with TOKEN
app.post('/api/admin/sync-monetizze', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const { startDate, endDate, productCodes } = req.body;
        
        const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
        
        if (!consumerKey) {
            return res.status(500).json({ 
                error: 'Monetizze API credentials not configured',
                message: 'Configure MONETIZZE_CONSUMER_KEY in environment variables'
            });
        }
        
        console.log('🔄 Starting manual Monetizze sync...');
        console.log('📅 Date range:', startDate || 'today', 'to', endDate || 'today');
        
        // Step 1: Authenticate - get temporary token
        let monetizzeToken;
        try {
            monetizzeToken = await getMonetizzeToken();
        } catch (authError) {
            console.error('❌ Monetizze authentication failed:', authError.message);
            return res.status(200).json({ 
                success: false,
                error: 'Monetizze authentication failed',
                details: authError.message,
                hint: 'Verifique se a MONETIZZE_CONSUMER_KEY está correta no Railway.'
            });
        }
        
        // Step 2: Query transactions with the token
        const params = new URLSearchParams();
        if (startDate) params.append('date_min', `${startDate} 00:00:00`);
        if (endDate) params.append('date_max', `${endDate} 23:59:59`);
        // Get all statuses so we have complete data
        // Monetizze API supports status 1-6. Chargebacks (8/9) only come via postback.
        ['1','2','3','4','5','6'].forEach(s => params.append('status[]', s));
        
        // Only fetch our 16 specific products (8 main + 8 affiliate)
        // English Main: 341972 (Front), 349241 (UP1), 349242 (UP2), 349243 (UP3)
        // English Affiliates: 330254 (Front), 341443 (UP1), 341444 (UP2), 341448 (UP3)
        // Spanish Main: 349260 (Front), 349261 (UP1), 349266 (UP2), 349267 (UP3)
        // Spanish Affiliates: 338375 (Front), 341452 (UP1), 341453 (UP2), 341454 (UP3)
        const validProductCodes = [
            // English Main
            '341972', '349241', '349242', '349243',
            // English Affiliates
            '330254', '341443', '341444', '341448',
            // Spanish Main
            '349260', '349261', '349266', '349267',
            // Spanish Affiliates
            '338375', '341452', '341453', '341454'
        ];
        validProductCodes.forEach(code => params.append('product[]', code));
        
        const txUrl = `https://api.monetizze.com.br/2.1/transactions?${params.toString()}`;
        console.log('🌐 Fetching transactions from Monetizze API 2.1:', txUrl);
        
        const response = await fetch(txUrl, {
            method: 'GET',
            headers: {
                'TOKEN': monetizzeToken,
                'Accept': 'application/json'
            }
        });
        
        if (!response.ok) {
            const errorText = await response.text();
            console.error('❌ Monetizze API error:', response.status, errorText);
            
            let errorDetails = errorText;
            try {
                const errorJson = JSON.parse(errorText);
                errorDetails = errorJson.message || errorJson.error || errorText;
            } catch (e) {}
            
            return res.status(200).json({ 
                success: false,
                error: 'Monetizze API request failed',
                status: response.status,
                details: errorDetails,
                hint: response.status === 403 ? 'Token expirado. Tente novamente.' : 'Erro na API da Monetizze.'
            });
        }
        
        const data = await response.json();
        
        // Log response structure for debugging
        console.log('📦 Response type:', typeof data);
        console.log('📦 Is array:', Array.isArray(data));
        if (!Array.isArray(data)) {
            console.log('📦 Response keys:', Object.keys(data));
            console.log('📦 Status field:', data.status);
            console.log('📦 Pagination:', data.pagina, '/', data.paginas, 'registros:', data.registros);
        }
        
        // API 2.1 /transactions returns: { status: 200, dados: [...], pagina, paginas, registros }
        let salesArray = [];
        if (Array.isArray(data)) {
            salesArray = data;
        } else if (Array.isArray(data.dados)) {
            salesArray = data.dados;
        } else if (Array.isArray(data.vendas)) {
            salesArray = data.vendas;
        } else if (Array.isArray(data.recordset)) {
            salesArray = data.recordset;
        }
        
        console.log(`📦 First page: ${salesArray.length} transactions`);
        console.log(`📄 API pagination info - pagina: ${data.pagina}, paginas: ${data.paginas}, registros: ${data.registros}`);
        
        // Handle pagination - fetch all pages
        const totalPages = data.paginas || 0;
        const totalRecords = data.registros || salesArray.length;
        
        if (totalPages > 1) {
            console.log(`📄 Pagination detected: ${totalPages} pages, ${totalRecords} total records`);
            for (let page = 2; page <= Math.min(totalPages, 50); page++) {
                try {
                    console.log(`📄 Fetching page ${page}/${totalPages}...`);
                    const pageParams = new URLSearchParams(params.toString());
                    pageParams.set('pagina', String(page));
                    
                    const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                        method: 'GET',
                        headers: {
                            'TOKEN': monetizzeToken,
                            'Accept': 'application/json'
                        }
                    });
                    
                    if (pageResponse.ok) {
                        const pageData = await pageResponse.json();
                        const pageItems = pageData.dados || pageData.vendas || [];
                        salesArray = salesArray.concat(pageItems);
                        console.log(`📄 Page ${page}: ${pageItems.length} transactions (total: ${salesArray.length})`);
                        if (pageItems.length === 0) break;
                    }
                } catch (pageError) {
                    console.error(`❌ Error fetching page ${page}:`, pageError.message);
                }
            }
        } else if (salesArray.length >= 100 && totalPages === 0) {
            // API might not return paginas field - try fetching more pages manually
            console.log(`⚠️ Got ${salesArray.length} items but no pagination info. Probing for more pages...`);
            for (let page = 2; page <= 15; page++) { // Max 15 pages (1500 items)
                try {
                    const pageParams = new URLSearchParams(params.toString());
                    pageParams.set('pagina', String(page));
                    const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                        method: 'GET',
                        headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                    });
                    if (pageResponse.ok) {
                        const pageData = await pageResponse.json();
                        const pageItems = pageData.dados || pageData.vendas || [];
                        if (pageItems.length === 0) {
                            console.log(`📄 Page ${page}: empty - pagination complete`);
                            break;
                        }
                        salesArray = salesArray.concat(pageItems);
                        console.log(`📄 Page ${page}: ${pageItems.length} items (total: ${salesArray.length})`);
                    } else {
                        console.log(`📄 Page ${page}: HTTP ${pageResponse.status} - stopping pagination`);
                        break;
                    }
                } catch (pageError) {
                    console.error(`❌ Page ${page} error:`, pageError.message);
                    break;
                }
            }
        }
        
        console.log(`📦 Total transactions fetched: ${salesArray.length}`);
        
        if (salesArray.length === 0) {
            return res.json({
                success: true,
                message: 'No sales found for the specified period',
                synced: 0,
                skipped: 0,
                total: 0,
                responseKeys: Object.keys(data),
                rawPreview: JSON.stringify(data).substring(0, 500)
            });
        }
        
        let synced = 0;
        let skipped = 0;
        let errors = [];
        
        // Process each sale/transaction
        // API 2.1 format: each item has { chave_unica, venda: {...}, produto: {...}, comprador: {...}, tipoEvento: {...} }
        for (const item of salesArray) {
            try {
                // API 2.1 has nested structure - venda, produto, comprador are sub-objects
                const vendaData = item.venda || item;
                const produtoData = item.produto || {};
                const compradorData = item.comprador || {};
                const tipoEvento = item.tipoEvento || {};
                
                // Extract data from Monetizze API 2.1 format
                const transactionId = vendaData.codigo || item.codigo_venda || item.chave_unica;
                const email = compradorData.email || vendaData.email;
                const phone = compradorData.telefone || vendaData.telefone;
                const name = compradorData.nome || vendaData.nome;
                const productName = produtoData.nome || vendaData.produto_nome;
                const productCode = produtoData.codigo || vendaData.produto_codigo;
                
                // Priority: comissao (commission) > valorRecebido > valor
                const value = vendaData.valorRecebido || vendaData.comissao || vendaData.valor;
                const status = vendaData.status || tipoEvento.descricao;
                const statusCode = String(tipoEvento.codigo || item.codigo_status || '2');
                
                console.log(`📋 Processing sale: ID=${transactionId}, Product=${productName} (${productCode}), Value=${value}, Status=${status} (${statusCode})`);
                
                // Double-check: only sync our 16 products (8 main + 8 affiliate)
                const validProductCodes = [
                    // English Main
                    '341972', '349241', '349242', '349243',
                    // English Affiliates
                    '330254', '341443', '341444', '341448',
                    // Spanish Main
                    '349260', '349261', '349266', '349267',
                    // Spanish Affiliates
                    '338375', '341452', '341453', '341454'
                ];
                if (productCode && !validProductCodes.includes(String(productCode))) {
                    console.log(`⏭️ Skipping product not in our funnel: ${productCode} - ${productName}`);
                    skipped++;
                    continue;
                }
                
                // Extract real sale date from Monetizze (uses helper to handle BR/ISO formats)
                // Priority: dataInicio (Data Pedido in Monetizze UI) for consistent ordering
                const saleDateStr = vendaData.dataInicio || vendaData.dataFinalizada || vendaData.dataVenda || vendaData.data || null;
                const saleDate = parseMonetizzeDate(saleDateStr);
                
                // Detect funnel language and source (main vs affiliate)
                const spanishCodes = ['349260', '349261', '349266', '349267', '338375', '341452', '341453', '341454'];
                const affiliateCodes = ['330254', '341443', '341444', '341448', '338375', '341452', '341453', '341454'];
                
                let funnelLanguage = spanishCodes.includes(String(productCode)) ? 'es' : 'en';
                const funnelSource = affiliateCodes.includes(String(productCode)) ? 'affiliate' : 'main';
                
                // Map status - IMPORTANT: check if sale is actually finalized
                // Monetizze can send status='2' (Finalizada) but without valid dataFinalizada
                const statusMap = {
                    '1': 'pending_payment',
                    '2': 'approved',
                    '3': 'cancelled',
                    '4': 'refunded',
                    '5': 'blocked',
                    '6': 'approved',
                    '7': 'abandoned_checkout',
                    '8': 'chargeback',
                    '9': 'chargeback'
                };
                let mappedStatus = statusMap[String(statusCode)] || 'approved';
                
                // Check dataFinalizada - if it's "0000-00-00" or empty, it's not finalized
                const dataFinalizada = vendaData.dataFinalizada || '';
                const isFinalized = dataFinalizada && 
                                   dataFinalizada !== '0000-00-00 00:00:00' && 
                                   dataFinalizada !== '0000-00-00' &&
                                   !dataFinalizada.startsWith('0000-00-00');
                
                // Check venda.status text for the REAL status
                const vendaStatus = (vendaData.status || '').toLowerCase();
                // Also check tipoEvento.descricao for additional status info
                const eventoDesc = (tipoEvento.descricao || '').toLowerCase();
                const combinedStatus = `${vendaStatus} ${eventoDesc}`;
                
                // Text-based status detection: NEVER downgrade from refunded/chargeback
                const statusCodeIsRefund = (statusCode === '4' || statusCode === '8' || statusCode === '9');
                
                if (combinedStatus.includes('chargeback') || combinedStatus.includes('disputa') || combinedStatus.includes('contestação') || combinedStatus.includes('contestacao')) {
                    mappedStatus = 'chargeback';
                } else if (combinedStatus.includes('devolvida') || combinedStatus.includes('reembolso') || combinedStatus.includes('reembolsada') || combinedStatus.includes('refund')) {
                    mappedStatus = 'refunded';
                } else if (statusCodeIsRefund) {
                    // StatusCode says refund/chargeback - KEEP IT, don't let text override
                    console.log(`🔒 Preserving ${mappedStatus} from statusCode=${statusCode} (vendaStatus: "${vendaData.status}")`);
                } else if (vendaStatus.includes('cancelada') || vendaStatus.includes('cancel')) {
                    mappedStatus = 'cancelled';
                } else if (vendaStatus.includes('aguardando') || vendaStatus.includes('pending')) {
                    mappedStatus = 'pending_payment';
                } else if (vendaStatus.includes('finalizada') || vendaStatus.includes('aprovada')) {
                    mappedStatus = 'approved';
                    if (!isFinalized) {
                        console.log(`⚠️ REPROCESS: vendaStatus says "${vendaData.status}" but dataFinalizada invalid - trusting as approved`);
                    }
                } else if (statusCode === '2' || statusCode === '6') {
                    mappedStatus = 'approved';
                    if (!isFinalized) {
                        console.log(`⚠️ REPROCESS: statusCode=${statusCode} (approved) but dataFinalizada invalid - trusting statusCode`);
                    }
                }
                
                if (!email || !transactionId) {
                    console.log(`⚠️ Skipping sale without email or ID:`, transactionId);
                    skipped++;
                    continue;
                }
                
                // Insert or update transaction with real sale date and funnel_source
                await pool.query(`
                    INSERT INTO transactions (
                        transaction_id, email, phone, name, product, value, 
                        monetizze_status, status, raw_data, funnel_language, funnel_source, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, COALESCE($12, NOW()))
                    ON CONFLICT (transaction_id) 
                    DO UPDATE SET 
                        monetizze_status = $7,
                        status = $8,
                        raw_data = $9,
                        funnel_language = $10,
                        funnel_source = $11,
                        updated_at = NOW()
                `, [
                    transactionId,
                    email,
                    phone,
                    name,
                    productName,
                    value,
                    String(statusCode),
                    mappedStatus,
                    JSON.stringify(item),
                    funnelLanguage,
                    funnelSource,
                    saleDate
                ]);
                
                // Also create refund_requests entry for refunds/chargebacks in manual sync
                if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
                    try {
                        const refundProtocol = `MON-${String(transactionId).substring(0, 12).toUpperCase()}`;
                        const refundType = mappedStatus === 'chargeback' ? 'chargeback' : 'refund';
                        
                        const existing = await pool.query(
                            'SELECT id FROM refund_requests WHERE transaction_id = $1',
                            [String(transactionId)]
                        );
                        
                        if (existing.rows.length === 0) {
                            await pool.query(`
                                INSERT INTO refund_requests (
                                    protocol, full_name, email, phone, product, reason, 
                                    status, source, refund_type, transaction_id, value, funnel_language, created_at
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, COALESCE($13, NOW()))
                            `, [
                                refundProtocol,
                                name || 'N/A',
                                email,
                                phone || null,
                                productName,
                                refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                                'approved',
                                'monetizze',
                                refundType,
                                String(transactionId),
                                parseFloat(value) || 0,
                                funnelLanguage,
                                saleDate
                            ]);
                            console.log(`📥 MANUAL SYNC: ${refundType.toUpperCase()} registered: ${refundProtocol} - ${email}`);
                        } else {
                            await pool.query(`
                                UPDATE refund_requests SET refund_type = $1, status = 'approved', updated_at = NOW()
                                WHERE transaction_id = $2 AND refund_type != $1
                            `, [refundType, String(transactionId)]);
                        }
                    } catch (refundError) {
                        console.error(`⚠️ Error registering refund_request during manual sync: ${refundError.message}`);
                    }
                }
                
                console.log(`✅ Synced: ${transactionId} - ${email} - ${productName}`);
                synced++;
                
            } catch (saleError) {
                console.error(`❌ Error processing sale:`, saleError.message);
                errors.push({ sale: item?.venda?.codigo || 'unknown', error: saleError.message });
                skipped++;
            }
        }
        
        console.log(`🎉 Sync complete: ${synced} synced, ${skipped} skipped`);
        
        res.json({
            success: true,
            message: 'Monetizze sync completed',
            synced,
            skipped,
            total: salesArray.length,
            errors: errors.length > 0 ? errors : undefined
        });
        
    } catch (error) {
        console.error('❌ Monetizze sync error:', error);
        res.status(500).json({ 
            error: 'Sync failed',
            message: error.message
        });
    }
});

// Diagnostic endpoint: show status of refunds/chargebacks in the database
app.get('/api/admin/refund-diagnostic', authenticateToken, async (req, res) => {
    try {
        // 1. Count transactions by status
        const txStatusCounts = await pool.query(`
            SELECT status, COUNT(*) as count FROM transactions GROUP BY status ORDER BY count DESC
        `);
        
        // 2. Count refund_requests by type and source
        const refundCounts = await pool.query(`
            SELECT refund_type, source, status, COUNT(*) as count 
            FROM refund_requests GROUP BY refund_type, source, status ORDER BY count DESC
        `);
        
        // 3. Get sample of refunded/chargeback transactions (raw_data included)
        const refundedTx = await pool.query(`
            SELECT transaction_id, email, name, product, value, status, monetizze_status, 
                   funnel_language, created_at,
                   raw_data->>'venda' as venda_json,
                   raw_data->'tipoEvento' as tipo_evento,
                   raw_data->'venda'->>'status' as venda_status_text
            FROM transactions 
            WHERE status IN ('refunded', 'chargeback')
            ORDER BY created_at DESC LIMIT 20
        `);
        
        // 4. Find orphaned refunds (in transactions but not in refund_requests)
        const orphans = await pool.query(`
            SELECT t.transaction_id, t.email, t.status, t.product, t.value, t.created_at
            FROM transactions t
            LEFT JOIN refund_requests rr ON rr.transaction_id = t.transaction_id
            WHERE t.status IN ('refunded', 'chargeback') AND rr.id IS NULL
        `);
        
        // 5. Check if there are transactions with "devolvida" in raw_data that are NOT marked as refunded
        const missedRefunds = await pool.query(`
            SELECT transaction_id, email, status, product, value,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao
            FROM transactions 
            WHERE status NOT IN ('refunded', 'chargeback')
              AND (
                  LOWER(raw_data->'venda'->>'status') LIKE '%devolv%'
                  OR LOWER(raw_data->'venda'->>'status') LIKE '%chargeback%'
                  OR LOWER(raw_data->'venda'->>'status') LIKE '%reembolso%'
                  OR LOWER(raw_data->'tipoEvento'->>'descricao') LIKE '%devolv%'
                  OR LOWER(raw_data->'tipoEvento'->>'descricao') LIKE '%chargeback%'
                  OR raw_data->'tipoEvento'->>'codigo' IN ('4', '8', '9')
              )
            LIMIT 20
        `);
        
        // 6. Get total refund_requests
        const totalRefundRequests = await pool.query('SELECT COUNT(*) FROM refund_requests');
        
        // 7. Sample of CANCELLED transactions to see if refunds are hiding as cancellations
        const cancelledSample = await pool.query(`
            SELECT transaction_id, email, status, monetizze_status, product, value,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao,
                   raw_data->'venda'->>'dataFinalizada' as data_finalizada,
                   created_at
            FROM transactions 
            WHERE status = 'cancelled'
            ORDER BY created_at DESC LIMIT 10
        `);
        
        // 8. Search ALL raw_data for any mention of refund keywords (broader search)
        const broadSearch = await pool.query(`
            SELECT transaction_id, email, status, product, value,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao,
                   monetizze_status
            FROM transactions 
            WHERE LOWER(CAST(raw_data AS text)) LIKE '%devolv%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%chargeback%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%reembolso%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%disputa%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%refund%'
               OR monetizze_status IN ('4', '8', '9')
            LIMIT 20
        `);
        
        // 9. Get distinct vendaStatus values to see all possible status texts
        const distinctStatuses = await pool.query(`
            SELECT raw_data->'venda'->>'status' as venda_status, 
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao,
                   COUNT(*) as count
            FROM transactions
            WHERE raw_data IS NOT NULL
            GROUP BY raw_data->'venda'->>'status', raw_data->'tipoEvento'->>'codigo', raw_data->'tipoEvento'->>'descricao'
            ORDER BY count DESC
        `);
        
        // 10. Check specific transaction IDs from Monetizze (the ones shown in the screenshot)
        const specificCheck = await pool.query(`
            SELECT transaction_id, email, status, monetizze_status, product, value,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo
            FROM transactions
            WHERE transaction_id LIKE '5580%' OR transaction_id LIKE '5579%' 
               OR transaction_id LIKE '5578%' OR transaction_id LIKE '5577%' OR transaction_id LIKE '5576%'
            ORDER BY created_at DESC LIMIT 20
        `);
        
        res.json({
            transactions_by_status: txStatusCounts.rows,
            refund_requests_summary: refundCounts.rows,
            total_refund_requests: parseInt(totalRefundRequests.rows[0].count),
            refunded_transactions: refundedTx.rows,
            orphaned_refunds: orphans.rows,
            missed_refunds_in_transactions: missedRefunds.rows,
            cancelled_sample: cancelledSample.rows,
            broad_refund_search: broadSearch.rows,
            distinct_api_statuses: distinctStatuses.rows,
            specific_monetizze_ids: specificCheck.rows,
            diagnostic_note: 'cancelled_sample shows recent cancelled transactions raw data. broad_refund_search finds any refund keywords in raw_data. distinct_api_statuses shows all unique status combinations from Monetizze API. specific_monetizze_ids checks for the transaction IDs visible in Monetizze dashboard.'
        });
    } catch (error) {
        console.error('Diagnostic error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Backfill: sync existing transactions with refunded/chargeback status to refund_requests table
// This is a one-time operation to catch transactions that were synced before the refund_requests fix
app.post('/api/admin/backfill-refunds', authenticateToken, requireAdmin, async (req, res) => {
    try {
        console.log('🔄 Starting refund backfill...');
        
        // Find all transactions with refunded/chargeback status that DON'T have a refund_requests entry
        const result = await pool.query(`
            SELECT t.transaction_id, t.email, t.phone, t.name, t.product, t.value, 
                   t.status, t.funnel_language, t.created_at
            FROM transactions t
            LEFT JOIN refund_requests rr ON rr.transaction_id = t.transaction_id
            WHERE t.status IN ('refunded', 'chargeback')
              AND rr.id IS NULL
            ORDER BY t.created_at DESC
        `);
        
        const missing = result.rows;
        console.log(`📦 Found ${missing.length} transactions without refund_requests entries`);
        
        let created = 0;
        let errors = 0;
        
        for (const tx of missing) {
            try {
                const refundProtocol = `MON-${String(tx.transaction_id).substring(0, 12).toUpperCase()}`;
                const refundType = tx.status === 'chargeback' ? 'chargeback' : 'refund';
                
                await pool.query(`
                    INSERT INTO refund_requests (
                        protocol, full_name, email, phone, product, reason, 
                        status, source, refund_type, transaction_id, value, funnel_language, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (protocol) DO NOTHING
                `, [
                    refundProtocol,
                    tx.name || 'N/A',
                    tx.email,
                    tx.phone || null,
                    tx.product,
                    refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                    'approved',
                    'monetizze',
                    refundType,
                    String(tx.transaction_id),
                    parseFloat(tx.value) || 0,
                    tx.funnel_language || 'en',
                    tx.created_at || new Date()
                ]);
                
                created++;
                console.log(`📥 Backfill: ${refundType.toUpperCase()} - ${refundProtocol} - ${tx.email}`);
            } catch (err) {
                errors++;
                console.error(`⚠️ Backfill error for ${tx.transaction_id}: ${err.message}`);
            }
        }
        
        console.log(`✅ Backfill complete: ${created} created, ${errors} errors, ${missing.length} total found`);
        
        res.json({
            success: true,
            message: `Backfill complete: ${created} refund_requests created`,
            found: missing.length,
            created,
            errors
        });
    } catch (error) {
        console.error('❌ Backfill error:', error);
        res.status(500).json({ error: 'Backfill failed', message: error.message });
    }
});

// Monetizze postback endpoint (public - no auth, uses token validation)
// Also accepts GET for testing
app.all('/api/postback/monetizze', async (req, res) => {
    // Handle GET request for testing
    if (req.method === 'GET') {
        return res.json({ 
            status: 'ok', 
            message: 'Postback endpoint is working! Use POST to send transaction data.',
            timestamp: new Date().toISOString()
        });
    }
    
    try {
        const rawBody = req.body || {};
        
        console.log('📥 Monetizze Postback received');
        console.log('📥 Content-Type:', req.headers['content-type']);
        console.log('📥 Body keys:', Object.keys(rawBody));
        console.log('📥 Raw body:', JSON.stringify(rawBody).substring(0, 1000));
        
        // ==================== HANDLE FLAT DOT-NOTATION KEYS ====================
        // Monetizze may send data as flat keys with dot notation:
        //   "produto.nome" = "X AI Monitor"
        //   "comprador.email" = "test@email.com"
        // We need to convert these to nested objects
        function unflattenObject(obj) {
            const result = {};
            for (const key of Object.keys(obj)) {
                if (key.includes('.')) {
                    const parts = key.split('.');
                    let current = result;
                    for (let i = 0; i < parts.length - 1; i++) {
                        if (!current[parts[i]] || typeof current[parts[i]] !== 'object') {
                            current[parts[i]] = {};
                        }
                        current = current[parts[i]];
                    }
                    current[parts[parts.length - 1]] = obj[key];
                } else {
                    // Don't overwrite nested objects already built
                    if (result[key] === undefined) {
                        result[key] = obj[key];
                    }
                }
            }
            // Merge non-dot keys that weren't set
            for (const key of Object.keys(obj)) {
                if (!key.includes('.') && result[key] === undefined) {
                    result[key] = obj[key];
                }
            }
            return result;
        }
        
        // Check if body has dot-notation keys (flat format from Monetizze)
        const hasDotKeys = Object.keys(rawBody).some(k => k.includes('.'));
        const body = hasDotKeys ? { ...unflattenObject(rawBody), ...rawBody } : rawBody;
        
        if (hasDotKeys) {
            console.log('📥 Detected flat dot-notation format, converted to nested:', JSON.stringify(body).substring(0, 500));
        }
        
        // Store postback for debugging (also persist to DB)
        try {
            const postbackEntry = {
                timestamp: new Date().toISOString(),
                method: req.method,
                contentType: req.headers['content-type'],
                body: body,
                rawBody: rawBody,
                bodyKeys: Object.keys(rawBody),
                hasDotKeys
            };
            recentPostbacks.unshift(postbackEntry);
            if (recentPostbacks.length > 50) recentPostbacks.pop();
            
            // Persist to database for debugging (non-blocking)
            pool.query(`
                INSERT INTO postback_logs (content_type, body, created_at) 
                VALUES ($1, $2, NOW())
            `, [req.headers['content-type'], JSON.stringify(rawBody)]).catch(err => {
                console.log('Postback log DB error (non-blocking):', err.message);
            });
        } catch (debugErr) {
            console.log('Debug storage error:', debugErr.message);
        }
        
        // Monetizze sends nested objects: produto, venda, comprador, tipoEvento, tipoPostback
        // Extract nested objects safely - handle both nested and flat formats
        const venda = (body.venda && typeof body.venda === 'object') ? body.venda : {};
        const comprador = (body.comprador && typeof body.comprador === 'object') ? body.comprador : {};
        const produto = (body.produto && typeof body.produto === 'object') ? body.produto : {};
        const tipoEvento = (body.tipoEvento && typeof body.tipoEvento === 'object') ? body.tipoEvento : {};
        
        // Transaction ID from chave_unica or venda.codigo - with dot-notation fallbacks
        const chave_unica = body.chave_unica || venda.codigo || body['venda.codigo'] || body['venda[codigo]'] || ('auto_' + Date.now());
        
        // Status code from tipoEvento.codigo (numeric) - this is the actual status
        // tipoEvento.codigo: 1=Aguardando, 2=Aprovada, 3=Cancelada, 4=Reembolso, etc.
        const statusCode = tipoEvento.codigo || body['tipoEvento.codigo'] || body['tipoEvento[codigo]'] || body.status || '2';
        
        // Value - prioritize commission value (valor líquido/comissão) over gross value
        // Monetizze fields: 
        //   venda.comissao or comissao - comissão líquida do produtor/afiliado
        //   venda.valorLiquido - valor líquido após taxas
        //   venda.valorRecebido - valor que será recebido
        //   venda.valor - valor bruto da venda
        const comissao = body.comissao || venda.comissao || body['venda.comissao'] || body['venda[comissao]'] || null;
        const valorLiquido = venda.valorLiquido || body['venda.valorLiquido'] || body['venda[valorLiquido]'] || null;
        const valorRecebido = venda.valorRecebido || body['venda.valorRecebido'] || body['venda[valorRecebido]'] || null;
        const valorBruto = venda.valor || body.valor || body['venda.valor'] || body['venda[valor]'] || '0';
        
        // Use GROSS sale value (valorBruto) for CAPI reporting (Facebook needs the total product price for ROAS)
        // Previously used commission, which underreported revenue to Facebook
        const valor = valorBruto || valorRecebido || valorLiquido || comissao;
        
        console.log('💰 Value breakdown:', { 
            comissao: comissao || 'N/A', 
            valorLiquido: valorLiquido || 'N/A', 
            valorRecebido: valorRecebido || 'N/A', 
            valorBruto,
            finalValue: valor 
        });
        
        // Buyer info from comprador object - with multiple fallbacks for different formats
        const email = comprador.email || body.email || body['comprador.email'] || body['comprador[email]'] || null;
        const telefone = comprador.telefone || body.telefone || body['comprador.telefone'] || body['comprador[telefone]'] || null;
        const nome = comprador.nome || body.nome || body['comprador.nome'] || body['comprador[nome]'] || null;
        
        // Product name from produto.nome - with fallbacks
        const productNameRaw = produto.nome || body['produto.nome'] || body['produto[nome]'] || body.produto_nome || 'Unknown Product';
        const productCode = produto.codigo || body['produto.codigo'] || body['produto[codigo]'] || body.produto_codigo || null;
        
        // Funnel language from venda.idioma
        const idioma = venda.idioma || body['venda.idioma'] || body['venda[idioma]'] || 'en';
        
        // Sale date - prioritize dataInicio (Data Pedido) for consistent ordering with Monetizze UI
        const dataInicioRaw = venda.dataInicio || body['venda.dataInicio'] || body['venda[dataInicio]'] || null;
        const dataFinalizadaRaw = venda.dataFinalizada || body['venda.dataFinalizada'] || body['venda[dataFinalizada]'] || null;
        const dataVenda = dataInicioRaw || dataFinalizadaRaw || venda.dataVenda || body['venda.dataVenda'] || body['venda[dataVenda]'] || 
                          venda.data || body.data || body['venda.data'] || null;
        
        console.log('📥 Extracted:', { 
            chave_unica, 
            statusCode, 
            valor, 
            email: email || 'none', 
            nome: nome || 'none',
            productNameRaw,
            productCode,
            idioma
        });
        
        // Validate postback (optional: check chave_unica against your secret)
        const postbackToken = process.env.MONETIZZE_POSTBACK_TOKEN;
        if (postbackToken && chave_unica !== postbackToken) {
            // Log but still process (Monetizze sends transaction ID as chave_unica)
            console.log('⚠️ Postback token check skipped (chave_unica is transaction ID)');
        }
        
        // Map Monetizze tipoEvento.codigo to our status
        // Based on official docs: https://apidoc.monetizze.com.br/postback/index.html
        // tipoEvento.codigo:
        // 1 = Aguardando pagamento
        // 2 = Finalizada / Aprovada ✅
        // 3 = Cancelada
        // 4 = Devolvida (Reembolso)
        // 5 = Bloqueada
        // 6 = Completa ✅
        // 7 = Abandono de Checkout
        // 101 = Assinatura - Ativa
        // 102 = Assinatura - Inadimplente
        // 103 = Assinatura - Cancelada
        // 104 = Assinatura - Aguardando pagamento
        // 105 = Recuperação Parcelada - Ativa
        // 106 = Recuperação Parcelada - Cancelada
        
        const statusMap = {
            '1': 'pending_payment',
            '2': 'approved',        // Finalizada / Aprovada
            '3': 'cancelled',
            '4': 'refunded',        // Reembolso solicitado pelo cliente
            '5': 'blocked',
            '6': 'approved',        // Completa (also counts as approved)
            '7': 'abandoned_checkout',
            '8': 'chargeback',      // Chargeback (disputa de cartão)
            '9': 'chargeback',      // Chargeback alternativo
            '70': 'tickets',
            '101': 'subscription_active',
            '102': 'subscription_overdue',
            '103': 'subscription_cancelled',
            '104': 'subscription_pending',
            '105': 'recovery_active',
            '106': 'recovery_cancelled',
            '120': 'shipping_update'
        };
        
        // Check for chargeback in description or other fields
        const eventoDescricao = (tipoEvento.descricao || '').toLowerCase();
        let finalStatus = statusMap[String(statusCode)] || 'unknown';
        
        // Detect chargeback from description if not caught by code
        if (eventoDescricao.includes('chargeback') || eventoDescricao.includes('disputa') || eventoDescricao.includes('contestação')) {
            finalStatus = 'chargeback';
        }
        
        // IMPORTANT: Check if sale is actually finalized
        // Monetizze can send status='2' (Finalizada) but without valid dataFinalizada
        // Use dataFinalizadaRaw (which has flat-format fallbacks) instead of venda.dataFinalizada
        const dataFinalizada = dataFinalizadaRaw || '';
        const isFinalized = dataFinalizada && 
                           dataFinalizada !== '0000-00-00 00:00:00' && 
                           dataFinalizada !== '0000-00-00' &&
                           !dataFinalizada.startsWith('0000-00-00');
        
        const statusStr = String(statusCode);
        console.log('🔍 Purchase check:', { statusStr, isFinalized, dataFinalizada: dataFinalizada || '(empty)', dataFinalizadaRaw: dataFinalizadaRaw || '(null)' });
        console.log('🔍 CAPI Decision:', { 
            willSendPurchase: statusStr === '2' || statusStr === '6',
            willSendInitiateCheckout: statusStr === '1' || statusStr === '7',
            statusCode, statusStr, 
            tipoEventoCodigo: tipoEvento.codigo || 'MISSING',
            tipoEventoDesc: tipoEvento.descricao || 'MISSING',
            bodyStatus: body.status || 'MISSING',
            vendaStatus: venda.status || 'MISSING'
        });
        
        // Check venda.status text for additional status info
        // IMPORTANT: NEVER override refunded/chargeback status with approved!
        const vendaStatus = (venda.status || '').toLowerCase();
        const isAlreadyRefundOrChargeback = (finalStatus === 'refunded' || finalStatus === 'chargeback');
        
        if (!isAlreadyRefundOrChargeback) {
            // Only apply venda.status text overrides if status is NOT already refund/chargeback
            if (vendaStatus.includes('cancelada') || vendaStatus.includes('cancel')) {
                finalStatus = 'cancelled';
            } else if (vendaStatus.includes('aguardando') || vendaStatus.includes('pending')) {
                finalStatus = 'pending_payment';
            } else if (vendaStatus.includes('finalizada') || vendaStatus.includes('aprovada')) {
                // If Monetizze says "Finalizada"/"Aprovada" in text, trust it as approved
                // Don't downgrade to pending_payment just because dataFinalizada is missing
                finalStatus = 'approved';
                if (!isFinalized) {
                    console.log(`⚠️ POSTBACK: vendaStatus says "${venda.status}" but dataFinalizada invalid - trusting vendaStatus as approved`);
                }
            } else if (statusStr === '2' || statusStr === '6') {
                // Status code 2/6 = Monetizze confirmed payment - always mark as approved
                finalStatus = 'approved';
                if (!isFinalized) {
                    console.log(`⚠️ POSTBACK: statusCode=${statusStr} (approved) but dataFinalizada invalid - trusting statusCode`);
                }
            }
        } else {
            console.log(`🔒 POSTBACK: Preserving ${finalStatus} status (not overriding with vendaStatus="${venda.status}")`);
        }
        
        // Also check venda.status text for refund/chargeback keywords (may catch additional cases)
        if (vendaStatus.includes('chargeback') || vendaStatus.includes('disputa') || vendaStatus.includes('contestação') || vendaStatus.includes('contestacao')) {
            finalStatus = 'chargeback';
        } else if (vendaStatus.includes('devolvida') || vendaStatus.includes('reembolso') || vendaStatus.includes('reembolsada') || vendaStatus.includes('refund')) {
            finalStatus = 'refunded';
        }
        
        const mappedStatus = finalStatus;
        const buyerEmail = email;
        const buyerPhone = telefone;
        const buyerName = nome;
        const productName = productNameRaw;
        const transactionValue = valor;
        
        console.log('📥 Final values:', { mappedStatus, buyerEmail: buyerEmail || 'none', productName, productCode: productCode || 'none', transactionValue });
        
        // Determine funnel language
        // Monetizze sends venda.idioma ('es', 'pt', 'en' etc)
        // Also check product codes and names as fallback
        // Spanish: Main (349260-349267) + Affiliates (338375, 341452-341454)
        const spanishProductCodes = ['349260', '349261', '349266', '349267', '338375', '341452', '341453', '341454'];
        const spanishProductKeywords = ['Infidelidad', 'Recuperación', 'Visión Total', 'VIP Sin Esperas'];
        // English: Main (341972, 349241-349243) + Affiliates (330254, 341443-341448)
        const englishProductCodes = ['341972', '349241', '349242', '349243', '330254', '341443', '341444', '341448'];
        
        let funnelLanguage = 'en'; // default to English
        
        // First check idioma field from Monetizze
        if (idioma === 'es') {
            funnelLanguage = 'es';
        } else if (productCode && spanishProductCodes.includes(String(productCode))) {
            funnelLanguage = 'es';
        } else if (productName && spanishProductKeywords.some(kw => productName.includes(kw))) {
            funnelLanguage = 'es';
        }
        
        // Identify product type (front/upsell1/upsell2/upsell3)
        let productType = 'front';
        const productNameLower = (productName || '').toLowerCase();
        const productCodeStr = String(productCode || '');
        
        // English products - Main: 341972=Front, 349241=UP1, 349242=UP2, 349243=UP3
        //                  - Affiliates: 330254=Front, 341443=UP1, 341444=UP2, 341448=UP3
        if (productNameLower.includes('message vault') || ['349241', '341443'].includes(productCodeStr)) {
            productType = 'upsell1';
        } else if (productNameLower.includes('360') || productNameLower.includes('tracker') || ['349242', '341444'].includes(productCodeStr)) {
            productType = 'upsell2';
        } else if (productNameLower.includes('instant access') || ['349243', '341448'].includes(productCodeStr)) {
            productType = 'upsell3';
        }
        // Spanish products - Main: 349260=Front, 349261=UP1, 349266=UP2, 349267=UP3
        //                  - Affiliates: 338375=Front, 341452=UP1, 341453=UP2, 341454=UP3
        else if (productNameLower.includes('recuperación total') || ['349261', '341452'].includes(productCodeStr)) {
            productType = 'upsell1';
        } else if (productNameLower.includes('visión total') || ['349266', '341453'].includes(productCodeStr)) {
            productType = 'upsell2';
        } else if (productNameLower.includes('sin esperas') || ['349267', '341454'].includes(productCodeStr)) {
            productType = 'upsell3';
        }
        // Front products (if none of the upsells matched)
        // English Main: X AI Monitor (341972), Affiliates: (330254)
        // Spanish Main: Detector de Infidelidad (349260), Affiliates: (338375)
        
        // Determine funnel source (main vs affiliate)
        const affiliateProductCodes = [
            '330254', '341443', '341444', '341448',  // English Affiliates
            '338375', '341452', '341453', '341454'   // Spanish Affiliates
        ];
        const funnelSource = (productCode && affiliateProductCodes.includes(String(productCode))) ? 'affiliate' : 'main';
        
        console.log(`🌐 Funnel language: ${funnelLanguage}, source: ${funnelSource} (product: ${productName}, code: ${productCode}, type: ${productType})`);
        
        // Generate a transaction ID if none provided
        const transactionId = chave_unica || `monetizze_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        
        // If no email found via standard extraction, try deep scan
        let finalEmail = buyerEmail;
        if (!finalEmail) {
            console.log('⚠️ No buyer email found via standard extraction');
            console.log('⚠️ Available body keys:', Object.keys(rawBody));
            console.log('⚠️ comprador object:', JSON.stringify(comprador));
            console.log('⚠️ Flat email keys:', Object.keys(rawBody).filter(k => k.toLowerCase().includes('email')));
            
            // Deep scan: search all values recursively for an email
            const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/;
            function findEmailInObj(obj) {
                for (const [key, val] of Object.entries(obj)) {
                    if (typeof val === 'string' && emailRegex.test(val)) {
                        return val.match(emailRegex)[0];
                    }
                    if (typeof val === 'object' && val !== null) {
                        const found = findEmailInObj(val);
                        if (found) return found;
                    }
                }
                return null;
            }
            
            finalEmail = findEmailInObj(rawBody);
            if (finalEmail) {
                console.log(`🔍 Found email via deep scan: ${finalEmail}`);
            } else {
                console.log('❌ No email found anywhere in postback data');
                // Log to DB for debugging
                pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('ERROR_NO_EMAIL', $1, NOW())`, 
                    [JSON.stringify({ rawKeys: Object.keys(rawBody), rawBody: rawBody })]).catch(() => {});
                return res.status(200).json({ status: 'ok', message: 'No email found, skipped' });
            }
        }
        
        console.log(`💾 Saving transaction: ${transactionId} for ${finalEmail || buyerEmail}`);
        
        // Determine created_at: use real sale date if available, otherwise NOW()
        // Uses parseMonetizzeDate helper to handle both Brazilian (DD/MM/YYYY) and ISO formats
        const saleDate = parseMonetizzeDate(dataVenda);
        console.log(`📅 Sale date: ${saleDate ? saleDate.toISOString() : 'Using NOW()'} (raw: ${dataVenda || 'none'})`);
        
        // ==================== WHATSAPP FROM LEADS (OPTION C) ====================
        // Try to get the correct WhatsApp from leads table (captured in funnel)
        // This is more reliable than phone from Monetizze checkout (users often fill wrong DDI)
        let finalPhone = buyerPhone; // Default to Monetizze phone
        const emailForLeadLookup = finalEmail || buyerEmail;
        
        if (emailForLeadLookup) {
            try {
                const leadResult = await pool.query(
                    `SELECT whatsapp, name FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                    [emailForLeadLookup]
                );
                if (leadResult.rows.length > 0 && leadResult.rows[0].whatsapp) {
                    finalPhone = leadResult.rows[0].whatsapp;
                    console.log(`📱 Using WhatsApp from lead: ${finalPhone} (instead of Monetizze: ${buyerPhone || 'none'})`);
                } else {
                    console.log(`📱 No lead WhatsApp found for ${emailForLeadLookup}, using Monetizze phone: ${buyerPhone || 'none'}`);
                }
            } catch (leadErr) {
                console.log(`⚠️ Error looking up lead WhatsApp: ${leadErr.message}`);
            }
        }
        
        // ==================== EXTRACT TRACKING PARAMS (before transaction save) ====================
        // Extract custom tracking params from postback (passed via checkout URL)
        // Monetizze may return these as venda.src, body.vid, UTM params, or flat fields
        const postbackVid = body.vid || venda.vid || body['venda.vid'] || body['venda[vid]'] || 
                           body.zs_vid || venda.zs_vid || null;
        let postbackFbc = body.zs_fbc || venda.zs_fbc || body['venda.zs_fbc'] || body['venda[zs_fbc]'] || null;
        const postbackFbp = body.zs_fbp || venda.zs_fbp || body['venda.zs_fbp'] || body['venda[zs_fbp]'] || null;
        
        // Build fbc from fbclid if zs_fbc not available
        if (!postbackFbc) {
            const postbackFbclid = body.fbclid || venda.fbclid || body['venda.fbclid'] || body['venda[fbclid]'] || null;
            if (postbackFbclid) {
                postbackFbc = `fb.1.${Date.now()}.${postbackFbclid}`;
            }
        }
        
        // Also try to extract vid from UTM fields (Monetizze sometimes maps custom params to UTM fields)
        const utmSource = body.utm_source || venda.utm_source || body['venda.utm_source'] || null;
        const utmContent = body.utm_content || venda.utm_content || body['venda.utm_content'] || null;
        // vid might be passed inside src field as a composite
        const srcField = venda.src || body.src || body['venda.src'] || body['venda[src]'] || '';
        const vidFromSrc = srcField.includes('vid=') ? new URLSearchParams(srcField.split('?').pop()).get('vid') : null;
        
        const resolvedVid = postbackVid || vidFromSrc || null;
        
        // Store transaction in database with funnel_language and funnel_source
        try {
            await pool.query(`
                INSERT INTO transactions (
                    transaction_id, email, phone, name, product, value, 
                    monetizze_status, status, raw_data, funnel_language, funnel_source, created_at,
                    fbc, fbp, visitor_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, COALESCE($12, NOW()), $13, $14, $15)
                ON CONFLICT (transaction_id) 
                DO UPDATE SET 
                    monetizze_status = $7,
                    status = $8,
                    raw_data = $9,
                    funnel_language = $10,
                    funnel_source = $11,
                    phone = COALESCE($3, transactions.phone),
                    fbc = COALESCE($13, transactions.fbc),
                    fbp = COALESCE($14, transactions.fbp),
                    visitor_id = COALESCE($15, transactions.visitor_id),
                    updated_at = NOW()
            `, [
                transactionId,
                finalEmail || buyerEmail,
                finalPhone,  // Use WhatsApp from lead if available
                buyerName,
                productName,
                transactionValue,
                String(statusCode),
                mappedStatus,
                JSON.stringify(req.body),
                funnelLanguage,
                funnelSource,
                saleDate,
                postbackFbc || null,
                postbackFbp || null,
                resolvedVid || null
            ]);
            console.log(`✅ Transaction saved: ${transactionId}`);
        } catch (dbError) {
            console.error(`❌ DB ERROR saving transaction: ${dbError.message}`);
            console.error(`❌ DB ERROR details:`, { transactionId, email: finalEmail || buyerEmail, product: productName, value: transactionValue });
            // Log error to postback_logs
            pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('DB_ERROR', $1, NOW())`, 
                [JSON.stringify({ error: dbError.message, transactionId, email: finalEmail || buyerEmail, rawBody })]).catch(() => {});
            throw dbError; // Re-throw to be caught by outer catch
        }
        
        // Try to match with existing lead and update status + products
        const emailForLead = finalEmail || buyerEmail;
        if (emailForLead) {
            const purchaseValue = parseFloat(transactionValue) || 0;
            
            // Build product identifier (type + truncated name)
            const productIdentifier = `${productType}:${(productName || '').substring(0, 50)}`;
            
            const leadUpdate = await pool.query(`
                UPDATE leads 
                SET status = CASE 
                    WHEN $1 = 'approved' THEN 'converted'
                    WHEN $1 IN ('cancelled', 'refunded', 'chargeback') AND status != 'converted' THEN 'lost'
                    WHEN $1 = 'pending_payment' AND status NOT IN ('converted', 'lost') THEN 'contacted'
                    ELSE status
                END,
                notes = COALESCE(notes, '') || E'\n[Monetizze] ' || $2 || ' - ' || NOW()::text,
                products_purchased = CASE 
                    WHEN $1 = 'approved' THEN 
                        CASE 
                            WHEN products_purchased IS NULL THEN ARRAY[$4]::TEXT[]
                            WHEN NOT ($4 = ANY(products_purchased)) THEN array_append(products_purchased, $4)
                            ELSE products_purchased
                        END
                    ELSE products_purchased
                END,
                total_spent = CASE 
                    WHEN $1 = 'approved' THEN COALESCE(total_spent, 0) + $5
                    WHEN $1 IN ('refunded', 'chargeback') THEN GREATEST(COALESCE(total_spent, 0) - $5, 0)
                    ELSE total_spent
                END,
                first_purchase_at = CASE 
                    WHEN $1 = 'approved' AND first_purchase_at IS NULL THEN NOW()
                    ELSE first_purchase_at
                END,
                last_purchase_at = CASE 
                    WHEN $1 = 'approved' THEN NOW()
                    ELSE last_purchase_at
                END,
                updated_at = NOW()
                WHERE LOWER(email) = LOWER($3)
                RETURNING id, email, status, products_purchased, total_spent
            `, [mappedStatus, mappedStatus, emailForLead, productIdentifier, purchaseValue]);
            
            if (leadUpdate.rows.length > 0) {
                const lead = leadUpdate.rows[0];
                console.log(`✅ Lead updated: ${emailForLead} -> ${mappedStatus} | Products: ${lead.products_purchased?.join(', ') || 'none'} | Total: R$${lead.total_spent}`);
            } else {
                console.log(`⚠️ No matching lead found for: ${emailForLead}`);
                // Auto-create lead from postback if approved (customer bought without going through lead capture)
                if (mappedStatus === 'approved') {
                    try {
                        const newLead = await pool.query(`
                            INSERT INTO leads (email, name, whatsapp, status, funnel_language, funnel_source, 
                                products_purchased, total_spent, first_purchase_at, last_purchase_at, 
                                created_at, updated_at)
                            VALUES (LOWER($1), $2, $3, 'converted', $4, $5, 
                                ARRAY[$6]::TEXT[], $7, NOW(), NOW(), NOW(), NOW())
                            RETURNING id, email
                        `, [emailForLead, buyerName || '', finalPhone || '', funnelLanguage, funnelSource, 
                            productIdentifier, purchaseValue]);
                        console.log(`✅ New lead auto-created from Monetizze postback: ${emailForLead} (id: ${newLead.rows[0]?.id})`);
                    } catch (insertErr) {
                        console.error(`⚠️ Error auto-creating lead from Monetizze postback: ${insertErr.message}`);
                    }
                }
            }
        }
        
        // ==================== FACEBOOK CONVERSIONS API EVENTS ====================
        
        if (resolvedVid || postbackFbc || postbackFbp) {
            console.log(`📊 CAPI: Custom tracking params from postback: vid=${resolvedVid || 'none'}, fbc=${postbackFbc ? 'Yes' : 'No'}, fbp=${postbackFbp ? 'Yes' : 'No'}`);
        }
        
        // Try to get enriched data from the lead record (IP, userAgent, fbc, fbp, country, city)
        // Quality score depends heavily on fbc, fbp, ip, user_agent - we need the lead to have them
        // ENHANCED: 5-level fallback matching for maximum attribution coverage
        let leadData = null;
        let matchMethod = 'none';
        const emailForCAPI = finalEmail || buyerEmail;
        
        try {
            // ===== LEVEL 1: Match by email in leads table =====
            if (emailForCAPI) {
                const leadResult = await pool.query(
                    `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id, funnel_language, referrer 
                     FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                    [emailForCAPI]
                );
                if (leadResult.rows.length > 0) {
                    leadData = leadResult.rows[0];
                    matchMethod = 'email';
                }
            }
            
            // ===== LEVEL 2: Match by phone in leads table =====
            if (!leadData && buyerPhone) {
                const digitsOnly = (s) => (s || '').replace(/\D/g, '');
                const buyerDigits = digitsOnly(buyerPhone);
                if (buyerDigits.length >= 10) {
                    const phoneResult = await pool.query(
                        `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id, funnel_language, referrer 
                         FROM leads 
                         WHERE REGEXP_REPLACE(COALESCE(whatsapp, ''), '\\D', '', 'g') = $1 
                            OR REGEXP_REPLACE(COALESCE(whatsapp, ''), '\\D', '', 'g') LIKE $2 
                         ORDER BY created_at DESC LIMIT 1`,
                        [buyerDigits, '%' + buyerDigits.slice(-10)]
                    );
                    if (phoneResult.rows.length > 0) {
                        leadData = phoneResult.rows[0];
                        matchMethod = 'phone';
                    }
                }
            }
            
            // ===== LEVEL 3: Match by visitor_id in leads table =====
            if (!leadData && resolvedVid) {
                const vidLeadResult = await pool.query(
                    `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id, funnel_language, referrer 
                     FROM leads WHERE visitor_id = $1 ORDER BY created_at DESC LIMIT 1`,
                    [resolvedVid]
                );
                if (vidLeadResult.rows.length > 0) {
                    leadData = vidLeadResult.rows[0];
                    matchMethod = 'visitor_id_lead';
                }
            }
            
            // ===== LEVEL 4: Match by visitor_id in funnel_events (get fbc/fbp from page visits) =====
            if (!leadData && resolvedVid) {
                const vidEventResult = await pool.query(
                    `SELECT ip_address, user_agent, fbc, fbp 
                     FROM funnel_events 
                     WHERE visitor_id = $1 AND (fbc IS NOT NULL OR fbp IS NOT NULL)
                     ORDER BY created_at DESC LIMIT 1`,
                    [resolvedVid]
                );
                if (vidEventResult.rows.length > 0) {
                    const eventRow = vidEventResult.rows[0];
                    leadData = {
                        ip_address: eventRow.ip_address,
                        user_agent: eventRow.user_agent,
                        fbc: eventRow.fbc,
                        fbp: eventRow.fbp,
                        country_code: null, city: null, state: null,
                        name: buyerName, target_gender: null,
                        whatsapp: buyerPhone, visitor_id: resolvedVid,
                        funnel_language: funnelLanguage, referrer: null
                    };
                    matchMethod = 'visitor_id_events';
                }
            }
            
            // ===== LEVEL 5: Match by IP address in funnel_events (last 48h) =====
            if (!leadData) {
                const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
                // Only try IP matching if we have a real IP (not localhost)
                if (clientIp && clientIp !== '127.0.0.1' && clientIp !== '::1') {
                    // Try to find a recent funnel event from the same IP
                    const ipEventResult = await pool.query(
                        `SELECT ip_address, user_agent, fbc, fbp, visitor_id 
                         FROM funnel_events 
                         WHERE ip_address = $1 AND (fbc IS NOT NULL OR fbp IS NOT NULL)
                         AND created_at > NOW() - INTERVAL '48 hours'
                         ORDER BY created_at DESC LIMIT 1`,
                        [clientIp]
                    );
                    if (ipEventResult.rows.length > 0) {
                        const eventRow = ipEventResult.rows[0];
                        leadData = {
                            ip_address: eventRow.ip_address,
                            user_agent: eventRow.user_agent,
                            fbc: eventRow.fbc,
                            fbp: eventRow.fbp,
                            country_code: null, city: null, state: null,
                            name: buyerName, target_gender: null,
                            whatsapp: buyerPhone, visitor_id: eventRow.visitor_id,
                            funnel_language: funnelLanguage, referrer: null
                        };
                        matchMethod = 'ip_events';
                    }
                }
            }
            
            // ===== LEVEL 6: Use fbc/fbp from checkout URL params (passed via Monetizze) =====
            if (!leadData && (postbackFbc || postbackFbp)) {
                leadData = {
                    ip_address: null,
                    user_agent: null,
                    fbc: postbackFbc,
                    fbp: postbackFbp,
                    country_code: null, city: null, state: null,
                    name: buyerName, target_gender: null,
                    whatsapp: buyerPhone, visitor_id: resolvedVid,
                    funnel_language: funnelLanguage, referrer: null
                };
                matchMethod = 'postback_params';
            }
            
            // ===== ENRICHMENT: If lead found but missing fbc/fbp, try to fill from other sources =====
            if (leadData && (!leadData.fbc || !leadData.fbp)) {
                // Try postback params first
                if (!leadData.fbc && postbackFbc) leadData.fbc = postbackFbc;
                if (!leadData.fbp && postbackFbp) leadData.fbp = postbackFbp;
                
                // Try funnel_events by visitor_id
                if ((!leadData.fbc || !leadData.fbp) && leadData.visitor_id) {
                    const enrichResult = await pool.query(
                        `SELECT fbc, fbp FROM funnel_events 
                         WHERE visitor_id = $1 AND (fbc IS NOT NULL OR fbp IS NOT NULL)
                         ORDER BY created_at DESC LIMIT 1`,
                        [leadData.visitor_id]
                    );
                    if (enrichResult.rows.length > 0) {
                        if (!leadData.fbc && enrichResult.rows[0].fbc) leadData.fbc = enrichResult.rows[0].fbc;
                        if (!leadData.fbp && enrichResult.rows[0].fbp) leadData.fbp = enrichResult.rows[0].fbp;
                        console.log(`📊 CAPI: Enriched lead with fbc/fbp from funnel_events (visitor_id: ${leadData.visitor_id})`);
                    }
                }
            }
            
            if (leadData) {
                console.log(`📊 CAPI: Lead matched via [${matchMethod}] - IP=${leadData.ip_address ? 'Yes' : 'No'}, UA=${leadData.user_agent ? 'Yes' : 'No'}, fbc=${leadData.fbc ? 'Yes' : 'No'}, fbp=${leadData.fbp ? 'Yes' : 'No'}, Country=${leadData.country_code || 'No'}, Gender=${leadData.target_gender || 'No'}, VisitorID=${leadData.visitor_id || 'No'}`);
            } else {
                console.log(`📊 CAPI: No lead found after all 6 fallback levels. Purchase will be sent with minimal parameters.`);
            }
        } catch (leadErr) {
            console.error('⚠️ Error fetching lead data for CAPI:', leadErr.message);
        }
        
        // User data for Facebook CAPI - enriched with lead data (more params = better match quality)
        const fbUserData = {
            email: emailForCAPI,
            phone: leadData?.whatsapp || finalPhone || buyerPhone,  // Prefer WhatsApp from lead (has country code)
            firstName: leadData?.name || buyerName,
            // Add enriched data from lead record
            ip: leadData?.ip_address || null,
            userAgent: leadData?.user_agent || null,
            fbc: leadData?.fbc || null,
            fbp: leadData?.fbp || null,
            country: leadData?.country_code || null,
            city: leadData?.city || null,
            state: leadData?.state || null,  // State/province for better matching
            gender: leadData?.target_gender || null,  // Gender for better matching
            externalId: leadData?.visitor_id || null,  // Cross-device tracking
            referrer: leadData?.referrer || null  // HTTP referrer for attribution
        };
        
        // Check if this is a new or returning customer
        let customerSegmentation = 'new_customer_to_business';
        if (emailForCAPI) {
            try {
                const prevPurchases = await pool.query(
                    `SELECT COUNT(*) as count FROM transactions WHERE LOWER(email) = LOWER($1) AND status = 'approved'`,
                    [emailForCAPI]
                );
                if (parseInt(prevPurchases.rows[0]?.count || 0) > 0) {
                    customerSegmentation = 'existing_customer_to_business';
                }
            } catch (segErr) {
                // Non-blocking
            }
        }
        
        // Convert Monetizze value (BRL) to USD so pixel and Facebook see one currency (same as frontend)
        const brlToUsdRate = parseFloat(process.env.CONVERSION_BRL_TO_USD || '0.18');
        const valueBRL = parseFloat(transactionValue) || 0;
        const valueUSD = Math.round((valueBRL * brlToUsdRate) * 100) / 100;
        if (valueBRL > 0) console.log(`📤 CAPI: Converting value R$${valueBRL} → $${valueUSD} USD (rate ${brlToUsdRate})`);

        // Custom data for Facebook CAPI - enriched with all available data (USD = same as frontend pixel)
        const fbCustomData = {
            content_name: productName,
            content_ids: [productCode || chave_unica],
            content_type: 'product',
            content_category: productType || 'digital_product',  // Product category
            value: valueUSD,
            currency: 'USD',
            order_id: chave_unica,  // Transaction ID for tracking
            num_items: 1,  // Number of items
            customer_segmentation: customerSegmentation  // New vs returning customer
        };
        
        // Build event_source_url based on funnel language AND source
        // MUST match the domain where the pixel fires (frontend uses window.location.href)
        let eventSourceUrl;
        if (funnelSource === 'perfectpay') {
            eventSourceUrl = funnelLanguage === 'es' 
                ? 'https://perfect.zappdetect.com/espanhol/' 
                : 'https://perfect.zappdetect.com/ingles/';
        } else if (funnelSource === 'affiliate') {
            eventSourceUrl = funnelLanguage === 'es' 
                ? 'https://afiliado.whatstalker.com/espanhol/' 
                : 'https://afiliado.whatstalker.com/ingles/';
        } else {
            eventSourceUrl = funnelLanguage === 'es' 
                ? 'https://espanhol.zappdetect.com/' 
                : 'https://ingles.zappdetect.com/';
        }
        
        // Generate event_id for deduplication (transaction-based)
        // For Purchase: use SAME event_id for status 2 and 6 to prevent Facebook counting twice
        const eventId = `monetizze_${chave_unica}_${statusCode}`;
        const purchaseEventIdFixed = `monetizze_${chave_unica}_purchase`; // Status-agnostic for dedup
        
        try {
            // statusStr already defined above (before isFinalized check)
            
            // Options with language for correct pixel selection
            // Include actual sale time so Facebook gets the correct event_time
            const capiOptions = { language: funnelLanguage, eventTime: saleDate || null };
            
            // Status 7 = Abandono de Checkout -> InitiateCheckout event
            if (statusStr === '7') {
                console.log(`📤 Sending InitiateCheckout to Facebook CAPI (${funnelLanguage})...`);
                await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_checkout`, capiOptions);
            }
            
            // Status 1 = Aguardando pagamento -> Also InitiateCheckout (they started checkout)
            if (statusStr === '1') {
                console.log(`📤 Sending InitiateCheckout (pending) to Facebook CAPI (${funnelLanguage})...`);
                await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_pending`, capiOptions);
            }
            
            // Status 2 or 6 = Aprovada/Completa -> DELAYED Purchase event
            // IMPORTANT: We delay sending CAPI Purchase by 30 seconds to give the enrichPurchase
            // mechanism (from upsell page) time to provide fbc/fbp data.
            // Flow: Postback arrives → transaction saved → buyer redirected to upsell → 
            //       enrichPurchase captures _fbc/_fbp from browser → sends to backend →
            //       backend updates transaction/lead → THEN the delayed catch-up sends CAPI with fbc/fbp
            // If enrichPurchase arrives within 30s, it triggers an IMMEDIATE catch-up (faster than the delay)
            if (statusStr === '2' || statusStr === '6') {
                
                // DEDUP CHECK: Skip if we already sent a Purchase for this transaction
                let alreadySent = false;
                try {
                    const dupCheck = await pool.query(
                        `SELECT id FROM capi_purchase_logs WHERE transaction_id = $1 AND capi_success = true LIMIT 1`,
                        [chave_unica]
                    );
                    alreadySent = dupCheck.rows.length > 0;
                } catch (dupErr) {
                    // Non-blocking - if check fails, proceed with delayed send
                }
                
                if (alreadySent) {
                    console.log(`⚠️ CAPI: Purchase already sent for transaction ${chave_unica} (status ${statusStr}) - SKIPPING to avoid duplicate`);
                } else {
                    if (!isFinalized) {
                        console.log(`⚠️ Status ${statusStr} but dataFinalizada invalid (${dataFinalizada || 'empty'}) - will send Purchase via delayed catch-up`);
                    }
                    
                    // DON'T send CAPI immediately! Schedule a delayed catch-up instead.
                    // This gives enrichPurchase (from upsell page) time to provide fbc/fbp
                    // The enrichPurchase endpoint triggers an immediate catch-up when it enriches,
                    // so if enrichPurchase arrives within 30s, CAPI is sent even faster.
                    const delayedTxId = chave_unica;
                    const delayedEmail = emailForCAPI;
                    console.log(`⏳ CAPI: Scheduling delayed Purchase for ${delayedTxId} (${delayedEmail}) in 30s - waiting for enrichPurchase from upsell page...`);
                    console.log(`⏳ CAPI: Current lead data: fbc=${leadData?.fbc ? 'Yes' : 'No'}, fbp=${leadData?.fbp ? 'Yes' : 'No'}, match=${matchMethod}`);
                    
                    setTimeout(() => {
                        console.log(`⏰ CAPI: Delayed trigger for ${delayedTxId} (${delayedEmail}) - running catch-up now...`);
                        sendMissingCAPIPurchases().catch(err => console.error('Delayed CAPI catch-up error:', err.message));
                    }, 30000);
                }
            }
            
            // Status 3 = Cancelled -> Send custom Cancel event
            if (statusStr === '3' || finalStatus === 'cancelled') {
                console.log('📤 Transaction cancelled - no Facebook event sent');
            }
            
            // Status 4 = Refund -> Refund event (custom)
            if (statusStr === '4' || finalStatus === 'refunded') {
                console.log(`📤 Sending Refund to Facebook CAPI (${funnelLanguage})...`);
                await sendToFacebookCAPI('Refund', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_refund`, capiOptions);
            }
            
            // Status 8/9 = Chargeback -> Refund event (custom, same as refund for CAPI tracking)
            if (statusStr === '8' || statusStr === '9' || finalStatus === 'chargeback') {
                console.log(`📤 Sending Refund (chargeback) to Facebook CAPI (${funnelLanguage})...`);
                await sendToFacebookCAPI('Refund', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_chargeback`, capiOptions);
            }
            
        } catch (capiError) {
            console.error('CAPI error (non-blocking):', capiError.message);
        }
        
        // Register refund/chargeback in refund_requests table for consolidated tracking
        if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
            try {
                const refundProtocol = `MON-${chave_unica.substring(0, 12).toUpperCase()}`;
                const refundType = mappedStatus === 'chargeback' ? 'chargeback' : 'refund';
                
                // Check if already exists
                const existing = await pool.query(
                    'SELECT id FROM refund_requests WHERE transaction_id = $1',
                    [chave_unica]
                );
                
                if (existing.rows.length === 0) {
                    await pool.query(`
                        INSERT INTO refund_requests (
                            protocol, full_name, email, phone, product, reason, 
                            status, source, refund_type, transaction_id, value, funnel_language, created_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
                    `, [
                        refundProtocol,
                        buyerName || 'N/A',
                        finalEmail,
                        buyerPhone || null,
                        productName,
                        refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                        'approved', // Monetizze already processed it
                        'monetizze',
                        refundType,
                        chave_unica,
                        parseFloat(transactionValue) || 0,
                        funnelLanguage
                    ]);
                    
                    console.log(`📥 ${refundType.toUpperCase()} registered: ${refundProtocol} - ${finalEmail} - ${productName}`);
                }
            } catch (refundError) {
                console.error('Error registering refund:', refundError.message);
            }
        }
        
        // Summary log for easy tracking in Railway
        console.log(`📋 POSTBACK SUMMARY: tx=${chave_unica} | email=${finalEmail || 'none'} | status=${statusStr} (${mappedStatus}) | product=${productName} | value=R$${transactionValue} | lang=${funnelLanguage} | source=${funnelSource} | match=${matchMethod} | CAPI_Purchase=${statusStr === '2' || statusStr === '6' ? 'YES' : 'NO'} | CAPI_IC=${statusStr === '1' || statusStr === '7' ? 'YES' : 'NO'}`);
        
        // Return success (Monetizze expects 200 OK)
        res.status(200).send('OK');
        
    } catch (error) {
        console.error('❌ Postback CRITICAL error:', error.message);
        console.error('❌ Stack:', error.stack);
        console.error('❌ Raw body was:', JSON.stringify(rawBody || req.body || {}).substring(0, 500));
        
        // Log error to DB for persistent debugging
        try {
            await pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('CRITICAL_ERROR', $1, NOW())`, 
                [JSON.stringify({ 
                    error: error.message, 
                    stack: error.stack?.substring(0, 500),
                    bodyKeys: Object.keys(rawBody || req.body || {}),
                    rawBody: rawBody || req.body || {},
                    contentType: req.headers['content-type']
                })]);
        } catch (logErr) {
            console.error('Failed to log error to DB:', logErr.message);
        }
        
        // Still return 200 to prevent Monetizze from retrying
        res.status(200).send('OK');
    }
});

// ==================== PERFECTPAY WEBHOOK API ====================
// Docs: https://support.perfectpay.com.br/doc/perfect-pay/postback/integracao-via-webhook-com-a-perfect-pay

// Store last 20 PerfectPay webhooks for debugging
const recentPerfectPayWebhooks = [];

app.all('/api/postback/perfectpay', async (req, res) => {
    // Handle GET request for testing
    if (req.method === 'GET') {
        return res.json({ 
            status: 'ok', 
            message: 'PerfectPay webhook endpoint is working! Use POST to send transaction data.',
            timestamp: new Date().toISOString()
        });
    }
    
    try {
        const body = req.body || {};
        
        console.log('📥 PerfectPay Webhook received');
        console.log('📥 Content-Type:', req.headers['content-type']);
        console.log('📥 Body keys:', Object.keys(body));
        console.log('📥 Raw body:', JSON.stringify(body).substring(0, 1000));
        
        // Store webhook for debugging (also persist to DB)
        try {
            const webhookEntry = {
                timestamp: new Date().toISOString(),
                method: req.method,
                contentType: req.headers['content-type'],
                body: body,
                bodyKeys: Object.keys(body)
            };
            recentPerfectPayWebhooks.unshift(webhookEntry);
            if (recentPerfectPayWebhooks.length > 50) recentPerfectPayWebhooks.pop();
            
            // Persist to database for debugging (non-blocking)
            pool.query(`
                INSERT INTO postback_logs (content_type, body, created_at) 
                VALUES ($1, $2, NOW())
            `, ['perfectpay_webhook', JSON.stringify(body)]).catch(err => {
                console.log('PerfectPay webhook log DB error (non-blocking):', err.message);
            });
        } catch (debugErr) {
            console.log('Debug storage error:', debugErr.message);
        }
        
        // ==================== EXTRACT PERFECTPAY FIELDS ====================
        // PerfectPay webhook JSON structure:
        // token, code, sale_amount, sale_status_enum, sale_status_detail
        // product: { code, name, external_reference, guarantee }
        // plan: { code, name, quantity }
        // customer: { full_name, email, phone_area_code, phone_number, country, state, city }
        // metadata: { src, utm_source, utm_medium, utm_campaign, utm_term, utm_content }
        // commission: [{ affiliation_code, affiliation_type_enum, name, email, commission_amount }]
        
        const webhookToken = body.token || null;
        const transactionCode = body.code || `pp_auto_${Date.now()}`;
        const saleAmount = body.sale_amount || 0;
        const statusEnum = String(body.sale_status_enum || '0');
        const statusDetail = body.sale_status_detail || '';
        const installments = body.installments || 1;
        const paymentType = body.payment_type_enum || 0;
        const dateCreated = body.date_created || null;
        const dateApproved = body.date_approved || null;
        
        // Product info
        const product = body.product || {};
        const productCode = product.code || null;
        const productName = product.name || 'Unknown Product';
        const productExternalRef = product.external_reference || null;
        
        // Plan info
        const plan = body.plan || {};
        const planCode = plan.code || null;
        const planName = plan.name || null;
        
        // Customer info
        const customer = body.customer || {};
        const buyerName = customer.full_name || null;
        const buyerEmail = customer.email || null;
        const buyerPhoneArea = customer.phone_area_code || '';
        const buyerPhoneNumber = customer.phone_number || '';
        const buyerPhone = buyerPhoneArea && buyerPhoneNumber 
            ? `+${buyerPhoneArea}${buyerPhoneNumber}` 
            : (buyerPhoneNumber || null);
        const buyerCountry = customer.country || null;
        const buyerState = customer.state || null;
        const buyerCity = customer.city || null;
        
        // Metadata (UTMs)
        const metadata = body.metadata || {};
        const utmSource = metadata.utm_source || null;
        const utmMedium = metadata.utm_medium || null;
        const utmCampaign = metadata.utm_campaign || null;
        const utmContent = metadata.utm_content || null;
        const utmTerm = metadata.utm_term || null;
        const metadataSrc = metadata.src || null;
        
        console.log('📥 PerfectPay Extracted:', { 
            transactionCode, 
            statusEnum, 
            statusDetail,
            saleAmount, 
            email: buyerEmail || 'none', 
            name: buyerName || 'none',
            productName,
            productCode,
            paymentType
        });
        
        // ==================== VALIDATE TOKEN ====================
        const perfectPayToken = process.env.PERFECTPAY_WEBHOOK_TOKEN;
        if (perfectPayToken && webhookToken && webhookToken !== perfectPayToken) {
            console.log('⚠️ PerfectPay token mismatch - processing anyway');
        }
        
        // ==================== MAP STATUS ====================
        // PerfectPay sale_status_enum:
        // 0 = none
        // 1 = pending (boleto pendente)
        // 2 = approved (venda aprovada)
        // 3 = in_process (revisão manual)
        // 4 = in_mediation (moderação)
        // 5 = rejected (rejeitado)
        // 6 = cancelled (cancelado)
        // 7 = refunded (devolvido)
        // 8 = authorized (autorizada)
        // 9 = charged_back (chargeback)
        // 10 = completed (30 dias após aprovação)
        // 11 = checkout_error
        // 12 = precheckout (abandono)
        // 13 = expired (boleto expirado)
        // 16 = in_review (em análise)
        
        const ppStatusMap = {
            '0': 'unknown',
            '1': 'pending_payment',
            '2': 'approved',
            '3': 'pending_payment',    // in_process -> treat as pending
            '4': 'pending_payment',    // in_mediation -> treat as pending
            '5': 'cancelled',          // rejected
            '6': 'cancelled',
            '7': 'refunded',
            '8': 'approved',           // authorized -> treat as approved
            '9': 'chargeback',
            '10': 'approved',          // completed -> treat as approved
            '11': 'cancelled',         // checkout_error
            '12': 'abandoned_checkout',// precheckout
            '13': 'cancelled',         // expired
            '16': 'pending_payment'    // in_review
        };
        
        // Also check sale_status_detail for additional info
        let mappedStatus = ppStatusMap[statusEnum] || 'unknown';
        const detailLower = (statusDetail || '').toLowerCase();
        
        if (detailLower.includes('chargeback') || detailLower.includes('charged_back')) {
            mappedStatus = 'chargeback';
        } else if (detailLower.includes('refund') || detailLower.includes('devolvid')) {
            mappedStatus = 'refunded';
        }
        
        console.log(`📥 PerfectPay Status: enum=${statusEnum} detail="${statusDetail}" -> mapped="${mappedStatus}"`);
        
        // ==================== DETERMINE FUNNEL LANGUAGE & SOURCE ====================
        // PerfectPay funnel is English by default (perfect.zappdetect.com/ingles)
        let funnelLanguage = 'en';
        let funnelSource = 'perfectpay';
        
        // Check UTM or product name for language hints
        const productNameLower = (productName || '').toLowerCase();
        if (productNameLower.includes('infidelidad') || productNameLower.includes('recuperación') || 
            utmCampaign === 'es' || utmSource === 'es') {
            funnelLanguage = 'es';
        }
        
        // Identify product type (front/upsell1/upsell2/upsell3)
        // PerfectPay English: PPPBE8FE=Front, PPPBE8FH=UP1, PPPBE8FI=UP2, PPPBE8FJ=UP3
        let productType = 'front';
        const ppCodeStr = String(productCode || '').toUpperCase();
        if (ppCodeStr === 'PPPBE8FH' || productNameLower.includes('message vault') || productNameLower.includes('recover')) {
            productType = 'upsell1';
        } else if (ppCodeStr === 'PPPBE8FI' || productNameLower.includes('360') || productNameLower.includes('tracker') || productNameLower.includes('social')) {
            productType = 'upsell2';
        } else if (ppCodeStr === 'PPPBE8FJ' || productNameLower.includes('instant') || productNameLower.includes('vip') || productNameLower.includes('priority')) {
            productType = 'upsell3';
        }
        
        console.log(`🌐 PerfectPay Funnel: lang=${funnelLanguage}, source=${funnelSource}, type=${productType}`);
        
        // ==================== RESOLVE EMAIL ====================
        if (!buyerEmail) {
            console.log('❌ PerfectPay: No buyer email found in webhook');
            pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('PP_ERROR_NO_EMAIL', $1, NOW())`, 
                [JSON.stringify(body)]).catch(() => {});
            return res.status(200).json({ status: 'ok', message: 'No email found, skipped' });
        }
        
        // ==================== RESOLVE PHONE FROM LEADS ====================
        let finalPhone = buyerPhone;
        try {
            const leadResult = await pool.query(
                `SELECT whatsapp, name FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                [buyerEmail]
            );
            if (leadResult.rows.length > 0 && leadResult.rows[0].whatsapp) {
                finalPhone = leadResult.rows[0].whatsapp;
                console.log(`📱 PerfectPay: Using WhatsApp from lead: ${finalPhone}`);
            }
        } catch (leadErr) {
            console.log(`⚠️ PerfectPay: Error looking up lead WhatsApp: ${leadErr.message}`);
        }
        
        // ==================== EXTRACT TRACKING PARAMS ====================
        const postbackVid = metadataSrc || null; // src field may contain visitor ID
        let postbackFbc = null;
        let postbackFbp = null;
        
        // Try to extract fbc/fbp from UTM fields (may be passed via checkout URL)
        if (utmContent) {
            // Sometimes fbc/fbp are encoded in utm_content
            const fbcMatch = utmContent.match(/fb\.1\.\d+\.\w+/);
            if (fbcMatch) postbackFbc = fbcMatch[0];
        }
        
        // ==================== PARSE DATES ====================
        // PerfectPay date format: "2019-04-10 18:50:56"
        let saleDate = null;
        try {
            if (dateCreated) {
                saleDate = new Date(dateCreated);
                if (isNaN(saleDate.getTime())) saleDate = null;
            }
        } catch (e) { saleDate = null; }
        
        // ==================== SAVE TRANSACTION ====================
        const transactionId = `PP_${transactionCode}`;
        
        console.log(`💾 PerfectPay: Saving transaction: ${transactionId} for ${buyerEmail}`);
        
        try {
            await pool.query(`
                INSERT INTO transactions (
                    transaction_id, email, phone, name, product, value, 
                    monetizze_status, status, raw_data, funnel_language, funnel_source, created_at,
                    fbc, fbp, visitor_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, COALESCE($12, NOW()), $13, $14, $15)
                ON CONFLICT (transaction_id) 
                DO UPDATE SET 
                    monetizze_status = $7,
                    status = $8,
                    raw_data = $9,
                    funnel_language = $10,
                    funnel_source = $11,
                    phone = COALESCE($3, transactions.phone),
                    fbc = COALESCE($13, transactions.fbc),
                    fbp = COALESCE($14, transactions.fbp),
                    visitor_id = COALESCE($15, transactions.visitor_id),
                    updated_at = NOW()
            `, [
                transactionId,
                buyerEmail,
                finalPhone,
                buyerName,
                productName,
                saleAmount,
                `pp_${statusEnum}`,  // Store original PerfectPay status with prefix
                mappedStatus,
                JSON.stringify(body),
                funnelLanguage,
                funnelSource,
                saleDate,
                postbackFbc || null,
                postbackFbp || null,
                postbackVid || null
            ]);
            console.log(`✅ PerfectPay: Transaction saved: ${transactionId}`);
        } catch (dbError) {
            console.error(`❌ PerfectPay DB ERROR: ${dbError.message}`);
            pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('PP_DB_ERROR', $1, NOW())`, 
                [JSON.stringify({ error: dbError.message, transactionId, email: buyerEmail, body })]).catch(() => {});
            throw dbError;
        }
        
        // ==================== UPDATE LEAD ====================
        if (buyerEmail) {
            const purchaseValue = parseFloat(saleAmount) || 0;
            const productIdentifier = `${productType}:${(productName || '').substring(0, 50)}`;
            
            try {
                const leadUpdate = await pool.query(`
                    UPDATE leads 
                    SET status = CASE 
                        WHEN $1 = 'approved' THEN 'converted'
                        WHEN $1 IN ('cancelled', 'refunded', 'chargeback') AND status != 'converted' THEN 'lost'
                        WHEN $1 = 'pending_payment' AND status NOT IN ('converted', 'lost') THEN 'contacted'
                        ELSE status
                    END,
                    notes = COALESCE(notes, '') || E'\n[PerfectPay] ' || $2 || ' - ' || NOW()::text,
                    products_purchased = CASE 
                        WHEN $1 = 'approved' THEN 
                            CASE 
                                WHEN products_purchased IS NULL THEN ARRAY[$4]::TEXT[]
                                WHEN NOT ($4 = ANY(products_purchased)) THEN array_append(products_purchased, $4)
                                ELSE products_purchased
                            END
                        ELSE products_purchased
                    END,
                    total_spent = CASE 
                        WHEN $1 = 'approved' THEN COALESCE(total_spent, 0) + $5
                        WHEN $1 IN ('refunded', 'chargeback') THEN GREATEST(COALESCE(total_spent, 0) - $5, 0)
                        ELSE total_spent
                    END,
                    first_purchase_at = CASE 
                        WHEN $1 = 'approved' AND first_purchase_at IS NULL THEN NOW()
                        ELSE first_purchase_at
                    END,
                    last_purchase_at = CASE 
                        WHEN $1 = 'approved' THEN NOW()
                        ELSE last_purchase_at
                    END,
                    updated_at = NOW()
                    WHERE LOWER(email) = LOWER($3)
                    RETURNING id, email, status, products_purchased, total_spent
                `, [mappedStatus, mappedStatus, buyerEmail, productIdentifier, purchaseValue]);
                
                if (leadUpdate.rows.length > 0) {
                    const lead = leadUpdate.rows[0];
                    console.log(`✅ PerfectPay Lead updated: ${buyerEmail} -> ${mappedStatus} | Products: ${lead.products_purchased?.join(', ') || 'none'} | Total: $${lead.total_spent}`);
                } else {
                    console.log(`⚠️ PerfectPay: No matching lead found for: ${buyerEmail}`);
                    // Auto-create lead from PerfectPay postback if approved
                    if (mappedStatus === 'approved') {
                        try {
                            const newLead = await pool.query(`
                                INSERT INTO leads (email, name, whatsapp, status, funnel_language, funnel_source,
                                    products_purchased, total_spent, first_purchase_at, last_purchase_at,
                                    created_at, updated_at)
                                VALUES (LOWER($1), $2, $3, 'converted', $4, $5,
                                    ARRAY[$6]::TEXT[], $7, NOW(), NOW(), NOW(), NOW())
                                RETURNING id, email
                            `, [buyerEmail, buyerName || '', finalPhone || '', funnelLanguage, funnelSource,
                                productIdentifier, purchaseValue]);
                            console.log(`✅ New lead auto-created from PerfectPay postback: ${buyerEmail} (id: ${newLead.rows[0]?.id})`);
                        } catch (insertErr) {
                            console.error(`⚠️ PerfectPay: Error auto-creating lead: ${insertErr.message}`);
                        }
                    }
                }
            } catch (leadErr) {
                console.error(`⚠️ PerfectPay: Error updating lead: ${leadErr.message}`);
            }
        }
        
        // ==================== FACEBOOK CAPI ====================
        try {
            // Try to get enriched data from the lead record
            let leadData = null;
            let matchMethod = 'none';
            
            if (buyerEmail) {
                const leadResult = await pool.query(
                    `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id, funnel_language, referrer 
                     FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                    [buyerEmail]
                );
                if (leadResult.rows.length > 0) {
                    leadData = leadResult.rows[0];
                    matchMethod = 'email';
                }
            }
            
            // Enrichment from funnel_events if needed
            if (leadData && (!leadData.fbc || !leadData.fbp) && leadData.visitor_id) {
                const enrichResult = await pool.query(
                    `SELECT fbc, fbp FROM funnel_events 
                     WHERE visitor_id = $1 AND (fbc IS NOT NULL OR fbp IS NOT NULL)
                     ORDER BY created_at DESC LIMIT 1`,
                    [leadData.visitor_id]
                );
                if (enrichResult.rows.length > 0) {
                    if (!leadData.fbc && enrichResult.rows[0].fbc) leadData.fbc = enrichResult.rows[0].fbc;
                    if (!leadData.fbp && enrichResult.rows[0].fbp) leadData.fbp = enrichResult.rows[0].fbp;
                }
            }
            
            if (leadData) {
                console.log(`📊 PerfectPay CAPI: Lead matched via [${matchMethod}] - fbc=${leadData.fbc ? 'Yes' : 'No'}, fbp=${leadData.fbp ? 'Yes' : 'No'}`);
            }
            
            const fbUserData = {
                email: buyerEmail,
                phone: leadData?.whatsapp || finalPhone || buyerPhone,
                firstName: leadData?.name || buyerName,
                ip: leadData?.ip_address || null,
                userAgent: leadData?.user_agent || null,
                fbc: leadData?.fbc || postbackFbc || null,
                fbp: leadData?.fbp || postbackFbp || null,
                country: leadData?.country_code || buyerCountry || null,
                city: leadData?.city || buyerCity || null,
                state: leadData?.state || buyerState || null,
                gender: leadData?.target_gender || null,
                externalId: leadData?.visitor_id || postbackVid || null
            };
            
            // Convert to USD (PerfectPay may send in USD already for international products)
            const valueRaw = parseFloat(saleAmount) || 0;
            // Check if currency is BRL (currency_enum 1) or assume USD for international
            const currencyEnum = body.currency_enum || 0;
            const isBRL = currencyEnum === 1;
            const brlToUsdRate = parseFloat(process.env.CONVERSION_BRL_TO_USD || '0.18');
            const valueUSD = isBRL ? Math.round((valueRaw * brlToUsdRate) * 100) / 100 : valueRaw;
            
            const fbCustomData = {
                content_name: productName,
                content_ids: [productCode || transactionCode],
                content_type: 'product',
                content_category: productType || 'digital_product',
                value: valueUSD,
                currency: 'USD',
                order_id: transactionId,
                num_items: 1
            };
            
            const eventSourceUrl = 'https://perfect.zappdetect.com/ingles/';
            const eventId = `perfectpay_${transactionCode}_${statusEnum}`;
            const capiOptions = { language: funnelLanguage, eventTime: saleDate || null };
            
            // Status 12 = precheckout (abandono) -> InitiateCheckout
            if (statusEnum === '12') {
                console.log(`📤 PerfectPay: Sending InitiateCheckout to Facebook CAPI...`);
                await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_checkout`, capiOptions);
            }
            
            // Status 1 = pending -> InitiateCheckout
            if (statusEnum === '1') {
                console.log(`📤 PerfectPay: Sending InitiateCheckout (pending) to Facebook CAPI...`);
                await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_pending`, capiOptions);
            }
            
            // Status 2, 8, 10 = approved/authorized/completed -> Purchase
            if (statusEnum === '2' || statusEnum === '8' || statusEnum === '10') {
                // Dedup check
                let alreadySent = false;
                try {
                    const dupCheck = await pool.query(
                        `SELECT id FROM capi_purchase_logs WHERE transaction_id = $1 AND capi_success = true LIMIT 1`,
                        [transactionId]
                    );
                    alreadySent = dupCheck.rows.length > 0;
                } catch (dupErr) {}
                
                if (alreadySent) {
                    console.log(`⚠️ PerfectPay CAPI: Purchase already sent for ${transactionId} - SKIPPING`);
                } else {
                    // Schedule delayed CAPI (same pattern as Monetizze - wait for enrichPurchase)
                    console.log(`⏳ PerfectPay CAPI: Scheduling delayed Purchase for ${transactionId} in 30s...`);
                    setTimeout(() => {
                        console.log(`⏰ PerfectPay CAPI: Delayed trigger for ${transactionId} - running catch-up...`);
                        sendMissingCAPIPurchases().catch(err => console.error('PerfectPay delayed CAPI error:', err.message));
                    }, 30000);
                }
            }
            
            // Status 7 = refunded
            if (statusEnum === '7' || mappedStatus === 'refunded') {
                console.log(`📤 PerfectPay: Sending Refund to Facebook CAPI...`);
                await sendToFacebookCAPI('Refund', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_refund`, capiOptions);
            }
            
            // Status 9 = chargeback
            if (statusEnum === '9' || mappedStatus === 'chargeback') {
                console.log(`📤 PerfectPay: Sending Refund (chargeback) to Facebook CAPI...`);
                await sendToFacebookCAPI('Refund', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_chargeback`, capiOptions);
            }
            
        } catch (capiError) {
            console.error('PerfectPay CAPI error (non-blocking):', capiError.message);
        }
        
        // ==================== REGISTER REFUND/CHARGEBACK ====================
        if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
            try {
                const refundProtocol = `PP-${transactionCode.substring(0, 12).toUpperCase()}`;
                const refundType = mappedStatus === 'chargeback' ? 'chargeback' : 'refund';
                
                const existing = await pool.query(
                    'SELECT id FROM refund_requests WHERE transaction_id = $1',
                    [transactionId]
                );
                
                if (existing.rows.length === 0) {
                    await pool.query(`
                        INSERT INTO refund_requests (
                            protocol, full_name, email, phone, product, reason, 
                            status, source, refund_type, transaction_id, value, funnel_language, created_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
                    `, [
                        refundProtocol,
                        buyerName || 'N/A',
                        buyerEmail,
                        buyerPhone || null,
                        productName,
                        refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via PerfectPay',
                        'approved',
                        'perfectpay',
                        refundType,
                        transactionId,
                        parseFloat(saleAmount) || 0,
                        funnelLanguage
                    ]);
                    console.log(`📥 PerfectPay ${refundType.toUpperCase()} registered: ${refundProtocol}`);
                }
            } catch (refundError) {
                console.error('PerfectPay refund registration error:', refundError.message);
            }
        }
        
        // Summary log
        console.log(`📋 PERFECTPAY SUMMARY: tx=${transactionId} | email=${buyerEmail || 'none'} | status=${statusEnum} (${mappedStatus}) | detail=${statusDetail} | product=${productName} | value=$${saleAmount} | lang=${funnelLanguage} | type=${productType}`);
        
        // Return success (PerfectPay expects 200 OK)
        res.status(200).json({ status: 'ok' });
        
    } catch (error) {
        console.error('❌ PerfectPay Webhook CRITICAL error:', error.message);
        console.error('❌ Stack:', error.stack);
        
        try {
            await pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('PP_CRITICAL_ERROR', $1, NOW())`, 
                [JSON.stringify({ 
                    error: error.message, 
                    stack: error.stack?.substring(0, 500),
                    body: req.body || {}
                })]);
        } catch (logErr) {
            console.error('Failed to log PerfectPay error to DB:', logErr.message);
        }
        
        // Still return 200 to prevent PerfectPay from retrying
        res.status(200).json({ status: 'ok' });
    }
});

// Admin: Debug PerfectPay webhooks
app.get('/api/admin/debug/perfectpay-webhooks', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const dbLogs = await pool.query(`
            SELECT id, content_type, body, created_at 
            FROM postback_logs 
            WHERE content_type LIKE 'perfectpay%' OR content_type LIKE 'PP_%'
            ORDER BY created_at DESC 
            LIMIT 30
        `);
        
        res.json({
            memoryCount: recentPerfectPayWebhooks.length,
            dbLogCount: dbLogs.rows.length,
            info: 'PerfectPay webhook debug. Configure webhook URL in PerfectPay dashboard.',
            webhookUrl: 'https://zapspy-funnel-production.up.railway.app/api/postback/perfectpay',
            alternateUrl: 'https://painel.xaimonitor.com/api/postback/perfectpay',
            recentWebhooks: recentPerfectPayWebhooks,
            dbLogs: dbLogs.rows
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Admin: Test Monetizze postback → Initiate Checkout / Purchase (sends to Meta Test Events only)
// Docs: https://apidoc.monetizze.com.br/postback/index.html
app.post('/api/admin/test-postback', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const { statusCode, email, language = 'en' } = req.body;
        // statusCode: 1=Aguardando, 2=Aprovada, 6=Completa, 7=Abandono
        const code = String(statusCode || '7');
        const validCodes = ['1', '2', '6', '7'];
        if (!validCodes.includes(code)) {
            return res.status(400).json({
                success: false,
                error: 'statusCode must be 1, 2, 6 or 7',
                mapping: { '1': 'InitiateCheckout (aguardando)', '2': 'Purchase (aprovada)', '6': 'Purchase (completa)', '7': 'InitiateCheckout (abandono)' }
            });
        }
        const testEmail = email || `test-postback-${Date.now()}@test.local`;
        const chave_unica = `test_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
        const funnelLanguage = (language === 'es' ? 'es' : 'en');

        let leadData = null;
        try {
            const leadResult = await pool.query(
                `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id 
                 FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                [testEmail]
            );
            if (leadResult.rows.length > 0) leadData = leadResult.rows[0];
        } catch (e) { /* ignore */ }

        const fbUserData = {
            email: testEmail,
            phone: leadData?.whatsapp || null,
            firstName: leadData?.name || 'Test',
            ip: leadData?.ip_address || null,
            userAgent: leadData?.user_agent || null,
            fbc: leadData?.fbc || null,
            fbp: leadData?.fbp || null,
            country: leadData?.country_code || null,
            city: leadData?.city || null,
            state: leadData?.state || null,
            gender: leadData?.target_gender || null,
            externalId: leadData?.visitor_id || null
        };
        const fbCustomData = {
            content_name: 'Test Product',
            content_ids: ['TEST'],
            content_type: 'product',
            value: funnelLanguage === 'es' ? 27 : 37,
            currency: 'USD',
            order_id: chave_unica,
            num_items: 1,
            customer_segmentation: 'new_customer_to_business'
        };
        const eventSourceUrl = funnelLanguage === 'es' ? 'https://espanhol.zappdetect.com/' : 'https://ingles.zappdetect.com/';
        const eventId = `test_${chave_unica}`;
        const testCode = funnelLanguage === 'es' ? (process.env.FB_TEST_CODE_ES || 'TEST96875') : (process.env.FB_TEST_CODE_EN || 'TEST23104');
        const capiOptions = { language: funnelLanguage, testEventCode: testCode };

        let eventSent = 'none';
        if (code === '7' || code === '1') {
            await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_checkout`, capiOptions);
            eventSent = 'InitiateCheckout';
        } else if (code === '2' || code === '6') {
            await sendToFacebookCAPI('Purchase', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_purchase`, capiOptions);
            eventSent = 'Purchase';
        }

        return res.json({
            success: true,
            message: `Postback de teste processado. Evento enviado: ${eventSent}`,
            statusCode: code,
            eventSent,
            eventName: eventSent,
            testEventCode: capiOptions.testEventCode,
            hint: 'Verifique no Gerenciador de Eventos do Meta (Test Events) se o evento apareceu.'
        });
    } catch (error) {
        console.error('Test postback error:', error);
        return res.status(500).json({ success: false, error: error.message });
    }
});

// ==================== REFUND REQUESTS API ====================

// Submit refund request (public)
app.post('/api/refund', async (req, res) => {
    try {
        const {
            fullName,
            email,
            phone,
            countryCode,
            purchaseDate,
            product,
            reason,
            details,
            protocol,
            visitorId  // NEW: visitorId from FingerprintJS
        } = req.body;

        // Validation
        if (!email || !fullName || !reason) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        const ipAddress = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        const userAgent = req.headers['user-agent'] || null;

        // ==================== CROSS-REFERENCE DATA ====================
        // Try to find this person in our leads and transactions to enrich the refund data
        // Priority: 1. visitorId (most reliable), 2. email
        let detectedLanguage = null;
        let detectedValue = null;
        let matchedTransactionId = null;
        
        try {
            // 1. FIRST: Try to find by visitorId (most reliable method)
            if (visitorId) {
                console.log(`🔗 Refund cross-ref: Searching by visitorId: ${visitorId}`);
                
                // Check transactions by visitorId
                const txByVisitorResult = await pool.query(`
                    SELECT transaction_id, value, funnel_language, product, status, email
                    FROM transactions 
                    WHERE visitor_id = $1 AND status = 'approved'
                    ORDER BY created_at DESC 
                    LIMIT 1
                `, [visitorId]);
                
                if (txByVisitorResult.rows.length > 0) {
                    const tx = txByVisitorResult.rows[0];
                    detectedLanguage = tx.funnel_language || null;
                    detectedValue = tx.value || null;
                    matchedTransactionId = tx.transaction_id || null;
                    console.log(`🔗 Refund cross-ref: Found transaction by visitorId! -> lang: ${detectedLanguage}, value: R$${detectedValue}, txId: ${matchedTransactionId}`);
                }
                
                // Also check leads by visitorId if no transaction found
                if (!detectedLanguage) {
                    const leadByVisitorResult = await pool.query(`
                        SELECT funnel_language
                        FROM leads 
                        WHERE visitor_id = $1
                        ORDER BY created_at DESC 
                        LIMIT 1
                    `, [visitorId]);
                    
                    if (leadByVisitorResult.rows.length > 0) {
                        detectedLanguage = leadByVisitorResult.rows[0].funnel_language || null;
                        console.log(`🔗 Refund cross-ref: Found lead by visitorId -> lang: ${detectedLanguage}`);
                    }
                }
                
                // Check funnel_events by visitorId
                if (!detectedLanguage || !detectedValue) {
                    const eventByVisitorResult = await pool.query(`
                        SELECT metadata->>'funnelLanguage' as funnel_language
                        FROM funnel_events 
                        WHERE visitor_id = $1
                        AND metadata->>'funnelLanguage' IS NOT NULL
                        ORDER BY created_at DESC 
                        LIMIT 1
                    `, [visitorId]);
                    
                    if (eventByVisitorResult.rows.length > 0 && !detectedLanguage) {
                        detectedLanguage = eventByVisitorResult.rows[0].funnel_language || null;
                        console.log(`🔗 Refund cross-ref: Found funnel event by visitorId -> lang: ${detectedLanguage}`);
                    }
                }
            }
            
            // 2. FALLBACK: If not found by visitorId, try by email
            if (!matchedTransactionId) {
                console.log(`🔗 Refund cross-ref: Fallback to email search: ${email}`);
                
                const txResult = await pool.query(`
                    SELECT transaction_id, value, funnel_language, product, status 
                    FROM transactions 
                    WHERE LOWER(email) = LOWER($1) AND status = 'approved'
                    ORDER BY created_at DESC 
                    LIMIT 1
                `, [email]);
                
                if (txResult.rows.length > 0) {
                    const tx = txResult.rows[0];
                    detectedLanguage = detectedLanguage || tx.funnel_language || null;
                    detectedValue = detectedValue || tx.value || null;
                    matchedTransactionId = matchedTransactionId || tx.transaction_id || null;
                    console.log(`🔗 Refund cross-ref: Found transaction by email -> lang: ${detectedLanguage}, value: R$${detectedValue}, txId: ${matchedTransactionId}`);
                }
            }
            
            // 3. If still no language, check leads by email
            if (!detectedLanguage) {
                const leadResult = await pool.query(`
                    SELECT l.id, l.funnel_language
                    FROM leads l
                    WHERE LOWER(l.email) = LOWER($1)
                    ORDER BY l.created_at DESC 
                    LIMIT 1
                `, [email]);
                
                if (leadResult.rows.length > 0) {
                    detectedLanguage = leadResult.rows[0].funnel_language || null;
                    console.log(`🔗 Refund cross-ref: Found lead by email -> lang: ${detectedLanguage}`);
                }
            }
            
            // 4. If still no language, check funnel_events by email
            if (!detectedLanguage) {
                const eventResult = await pool.query(`
                    SELECT metadata->>'funnelLanguage' as funnel_language
                    FROM funnel_events 
                    WHERE LOWER(metadata->>'email') = LOWER($1)
                    AND metadata->>'funnelLanguage' IS NOT NULL
                    ORDER BY created_at DESC 
                    LIMIT 1
                `, [email]);
                
                if (eventResult.rows.length > 0) {
                    detectedLanguage = eventResult.rows[0].funnel_language || null;
                    console.log(`🔗 Refund cross-ref: Found funnel event by email -> lang: ${detectedLanguage}`);
                }
            }
            
            console.log(`🔗 Refund cross-ref final: visitorId=${visitorId || 'none'}, email=${email}, lang=${detectedLanguage || 'unknown'}, value=${detectedValue || 'unknown'}, txId=${matchedTransactionId || 'none'}`);
            
        } catch (crossRefError) {
            console.error('⚠️ Cross-reference error (non-blocking):', crossRefError.message);
        }

        // Check for existing refund request from same email (prevent duplicates)
        const existingRefund = await pool.query(`
            SELECT id, protocol, status, created_at FROM refund_requests 
            WHERE LOWER(email) = LOWER($1) 
              AND (source IS NULL OR source = 'form')
            ORDER BY created_at DESC LIMIT 1
        `, [email]);
        
        if (existingRefund.rows.length > 0) {
            const existing = existingRefund.rows[0];
            console.log(`⚠️ Duplicate refund request blocked: ${email} already has ${existing.protocol} (status: ${existing.status})`);
            
            // Return the existing protocol instead of creating a duplicate
            return res.status(200).json({
                success: true,
                message: 'Refund request already exists',
                protocol: existing.protocol,
                existing: true
            });
        }

        // Store refund request with enriched data
        await pool.query(`
            INSERT INTO refund_requests (
                protocol, full_name, email, phone, country_code,
                purchase_date, product, reason, details,
                ip_address, user_agent, status, source, refund_type,
                funnel_language, value, transaction_id, visitor_id, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending', 'form', 'refund',
                $12, $13, $14, $15, NOW())
        `, [
            protocol,
            fullName,
            email,
            phone,
            countryCode,
            purchaseDate,
            product,
            reason,
            details,
            ipAddress,
            userAgent,
            detectedLanguage,
            detectedValue,
            matchedTransactionId,
            visitorId || null
        ]);

        console.log(`📥 Refund request received: ${protocol} - ${email} - ${product} (lang: ${detectedLanguage || 'unknown'})`);

        res.status(201).json({
            success: true,
            message: 'Refund request submitted successfully',
            protocol
        });

    } catch (error) {
        console.error('Error submitting refund:', error);
        res.status(500).json({ error: 'Failed to submit refund request' });
    }
});

// ==================== RECOVERY CENTER API ====================

// Helper function to calculate recovery score
function calculateRecoveryScore(lead, segment) {
    let score = 0;
    const now = new Date();
    
    // 1. Time Score (30%) - More recent = higher score
    const eventTime = new Date(lead.last_event_at || lead.created_at);
    const hoursAgo = (now - eventTime) / (1000 * 60 * 60);
    let timeScore = 0;
    if (hoursAgo <= 1) timeScore = 30;
    else if (hoursAgo <= 6) timeScore = 27;
    else if (hoursAgo <= 24) timeScore = 24;
    else if (hoursAgo <= 48) timeScore = 20;
    else if (hoursAgo <= 72) timeScore = 15;
    else if (hoursAgo <= 168) timeScore = 10; // 7 days
    else timeScore = 5;
    
    // 2. Engagement Score (25%) - Based on funnel events
    const eventCount = parseInt(lead.event_count || 0);
    let engagementScore = Math.min(25, eventCount * 3);
    
    // 3. Value Score (20%) - Based on potential value
    const value = parseFloat(lead.potential_value || 47);
    let valueScore = 0;
    if (value >= 200) valueScore = 20;
    else if (value >= 100) valueScore = 17;
    else if (value >= 50) valueScore = 14;
    else if (value >= 30) valueScore = 10;
    else valueScore = 7;
    
    // 4. History Score (15%) - Has previous purchases
    const hasPurchase = lead.has_purchase === true || lead.has_purchase === 't';
    let historyScore = hasPurchase ? 15 : 5;
    
    // 5. Attempts Score (10%) - Fewer attempts = higher score
    const attempts = parseInt(lead.contact_attempts || 0);
    let attemptsScore = Math.max(0, 10 - (attempts * 2));
    
    score = timeScore + engagementScore + valueScore + historyScore + attemptsScore;
    
    return {
        total: Math.min(100, Math.round(score)),
        breakdown: {
            time: timeScore,
            engagement: engagementScore,
            value: valueScore,
            history: historyScore,
            attempts: attemptsScore
        }
    };
}

// Get recovery segments summary
app.get('/api/admin/recovery/segments', authenticateToken, async (req, res) => {
    try {
        const { language, startDate, endDate } = req.query;
        
        let dateParams = [];
        let feDateFilter = '';
        let plainDateFilter = '';
        if (startDate && endDate) {
            dateParams = [startDate, endDate + ' 23:59:59'];
            feDateFilter = `AND fe.created_at >= $1 AND fe.created_at <= $2`;
            plainDateFilter = `AND created_at >= $1 AND created_at <= $2`;
        }
        
        // 1. Lost Visitors - Entered funnel but never reached checkout (exclude already contacted)
        const lostVisitors = await pool.query(`
            SELECT COUNT(DISTINCT fe.visitor_id) as count
            FROM funnel_events fe
            LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
            WHERE fe.event IN ('page_view', 'landing_visit', 'phone_submitted', 'cta_clicked')
            ${language ? `AND l.funnel_language = '${language}'` : ''}
            AND NOT EXISTS (
                SELECT 1 FROM funnel_events fe2
                WHERE fe2.visitor_id = fe.visitor_id
                AND fe2.event = 'checkout_clicked'
            )
            AND NOT EXISTS (
                SELECT 1 FROM transactions t
                WHERE LOWER(t.email) = LOWER(COALESCE(l.email, ''))
                AND t.status = 'approved'
            )
            AND NOT EXISTS (
                SELECT 1 FROM recovery_contacts rc
                WHERE LOWER(rc.lead_email) = LOWER(COALESCE(l.email, ''))
                AND rc.lead_email != ''
            )
            ${feDateFilter}
        `, dateParams);
        
        // 2. Checkout Abandoned - Clicked checkout but didn't buy (exclude already contacted)
        const checkoutAbandoned = await pool.query(`
            SELECT COUNT(DISTINCT fe.visitor_id) as count
            FROM funnel_events fe
            LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
            WHERE fe.event = 'checkout_clicked'
            ${language ? `AND l.funnel_language = '${language}'` : ''}
            AND NOT EXISTS (
                SELECT 1 FROM transactions t 
                WHERE LOWER(t.email) = LOWER(COALESCE(l.email, ''))
                AND t.status = 'approved'
            )
            AND NOT EXISTS (
                SELECT 1 FROM recovery_contacts rc
                WHERE LOWER(rc.lead_email) = LOWER(COALESCE(l.email, ''))
                AND rc.lead_email != ''
            )
            ${feDateFilter}
        `, dateParams);
        
        // 3. Payment Failed - Cancelled/refused/pending transactions (DISTINCT by email, exclude already contacted)
        const paymentFailed = await pool.query(`
            SELECT COUNT(*) as count, COALESCE(SUM(max_value), 0) as total_value
            FROM (
                SELECT DISTINCT ON (LOWER(t.email)) t.email, CAST(t.value AS DECIMAL) as max_value
                FROM transactions t
                WHERE t.status IN ('cancelled', 'refused', 'pending', 'waiting_payment')
                AND t.email IS NOT NULL AND t.email != ''
                ${language ? `AND t.funnel_language = '${language}'` : ''}
                AND NOT EXISTS (
                    SELECT 1 FROM recovery_contacts rc
                    WHERE LOWER(rc.lead_email) = LOWER(t.email)
                )
                ${plainDateFilter}
                ORDER BY LOWER(t.email), t.created_at DESC
            ) sub
        `, dateParams);
        
        // 4. Refund Requests - Pending refund requests
        const refundRequests = await pool.query(`
            SELECT COUNT(*) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total_value
            FROM refund_requests
            WHERE status IN ('pending', 'handling', 'processing')
            ${language ? `AND funnel_language = '${language}'` : ''}
            ${plainDateFilter}
        `, dateParams);
        
        // 5. Upsell Declined - Bought front-end but declined upsells
        const upsellDeclined = await pool.query(`
            SELECT COUNT(DISTINCT fe.visitor_id) as count
            FROM funnel_events fe
            INNER JOIN leads l ON fe.visitor_id = l.visitor_id
            WHERE fe.event LIKE '%_declined'
            ${language ? `AND l.funnel_language = '${language}'` : ''}
            AND EXISTS (
                SELECT 1 FROM transactions t 
                WHERE LOWER(t.email) = LOWER(l.email)
                AND t.status = 'approved'
            )
            ${feDateFilter}
        `, dateParams);
        
        const USD_TO_BRL = 5.50;
        const frontPrice = 47 * USD_TO_BRL;
        const upsellPrice = 67 * USD_TO_BRL;
        
        const lostCount = parseInt(lostVisitors.rows[0]?.count || 0);
        const checkoutCount = parseInt(checkoutAbandoned.rows[0]?.count || 0);
        const paymentCount = parseInt(paymentFailed.rows[0]?.count || 0);
        const refundCount = parseInt(refundRequests.rows[0]?.count || 0);
        const upsellCount = parseInt(upsellDeclined.rows[0]?.count || 0);
        const paymentValue = parseFloat(paymentFailed.rows[0]?.total_value || 0) * USD_TO_BRL;
        const refundValue = parseFloat(refundRequests.rows[0]?.total_value || 0) * USD_TO_BRL;
        
        res.json({
            segments: {
                lost_visitors: {
                    count: lostCount,
                    potential_value: lostCount * frontPrice,
                    label: 'Lost Visitors',
                    label_es: 'Visitantes Perdidos',
                    icon: '👻',
                    color: '#a855f7'
                },
                checkout_abandoned: {
                    count: checkoutCount,
                    potential_value: checkoutCount * frontPrice,
                    label: 'Checkout Abandoned',
                    label_es: 'Checkout Abandonado',
                    icon: '🛒',
                    color: '#f59e0b'
                },
                payment_failed: {
                    count: paymentCount,
                    potential_value: paymentValue,
                    label: 'Payment Failed',
                    label_es: 'Pagamentos Recusados',
                    icon: '💳',
                    color: '#ef4444'
                },
                refund_requests: {
                    count: refundCount,
                    potential_value: refundValue,
                    label: 'Refund Requests',
                    label_es: 'Pedidos Reembolso',
                    icon: '💸',
                    color: '#f97316'
                },
                upsell_declined: {
                    count: upsellCount,
                    potential_value: upsellCount * upsellPrice,
                    label: 'Upsell Declined',
                    label_es: 'Recusas Upsell',
                    icon: '📦',
                    color: '#3b82f6'
                }
            },
            totals: {
                count: lostCount + checkoutCount + paymentCount + refundCount + upsellCount,
                potential_value: (lostCount * frontPrice) + (checkoutCount * frontPrice) + paymentValue + refundValue + (upsellCount * upsellPrice)
            }
        });
        
    } catch (error) {
        console.error('Error fetching recovery segments:', error);
        res.status(500).json({ error: 'Failed to fetch recovery segments' });
    }
});

// Get leads for a specific recovery segment
app.get('/api/admin/recovery/:segment', authenticateToken, async (req, res, next) => {
    // Skip known named routes so they can be handled by their specific handlers
    const reservedRoutes = ['segments', 'funnels', 'templates', 'stats', 'funnel', 'contact', 'dispatch-log', 'dispatch-resend'];
    if (reservedRoutes.includes(req.params.segment)) {
        return next();
    }
    
    try {
        const { segment } = req.params;
        const { language, startDate, endDate, minScore, contactStatus, sortBy, page = 1, limit = 20 } = req.query;
        const offset = (parseInt(page) - 1) * parseInt(limit);
        
        let leads = [];
        let totalCount = 0;
        
        // Build date filters with table prefixes to avoid ambiguity in JOINs
        let feDateFilter = '';   // for funnel_events queries (prefix: fe.)
        let tDateFilter = '';    // for transactions queries (prefix: t.)
        let rDateFilter = '';    // for refund_requests queries (prefix: r.)
        let plainDateFilter = ''; // for single-table/count queries
        if (startDate && endDate) {
            feDateFilter = `AND fe.created_at >= '${startDate}' AND fe.created_at <= '${endDate} 23:59:59'`;
            tDateFilter = `AND t.created_at >= '${startDate}' AND t.created_at <= '${endDate} 23:59:59'`;
            rDateFilter = `AND r.created_at >= '${startDate}' AND r.created_at <= '${endDate} 23:59:59'`;
            plainDateFilter = `AND created_at >= '${startDate}' AND created_at <= '${endDate} 23:59:59'`;
        }
        
        // Ensure recovery_contacts table exists before queries
        await pool.query(`
            CREATE TABLE IF NOT EXISTS recovery_contacts (
                id SERIAL PRIMARY KEY,
                lead_email VARCHAR(255) NOT NULL,
                segment VARCHAR(50) NOT NULL,
                template_used VARCHAR(100),
                channel VARCHAR(20) DEFAULT 'whatsapp',
                message TEXT,
                status VARCHAR(20) DEFAULT 'sent',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        
        if (segment === 'lost_visitors') {
            // Lost visitors - entered funnel but never reached checkout
            const result = await pool.query(`
                SELECT DISTINCT ON (COALESCE(l.email, fe.visitor_id))
                    COALESCE(l.id, 0) as id,
                    COALESCE(l.email, '') as email,
                    COALESCE(l.name, 'Visitor') as name,
                    COALESCE(l.whatsapp, '') as phone,
                    COALESCE(l.country, '') as country,
                    COALESCE(l.country_code, '') as country_code,
                    l.funnel_language as language,
                    fe.created_at as last_event_at,
                    fe.event,
                    (CASE WHEN l.funnel_language = 'es' THEN 27.00 * 5.50 ELSE 37.00 * 5.50 END) as potential_value,
                    'X AI Monitor' as product,
                    (SELECT COUNT(*) FROM funnel_events fe3 WHERE fe3.visitor_id = fe.visitor_id) as event_count,
                    false as has_purchase,
                    0 as contact_attempts,
                    NULL as last_contact
                FROM funnel_events fe
                LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
                WHERE fe.event IN ('page_view', 'landing_visit', 'phone_submitted', 'cta_clicked')
                ${language ? `AND l.funnel_language = '${language}'` : ''}
                AND NOT EXISTS (
                    SELECT 1 FROM funnel_events fe2
                    WHERE fe2.visitor_id = fe.visitor_id
                    AND fe2.event = 'checkout_clicked'
                )
                AND NOT EXISTS (
                    SELECT 1 FROM transactions t
                    WHERE LOWER(t.email) = LOWER(COALESCE(l.email, ''))
                    AND t.status = 'approved'
                )
                AND NOT EXISTS (
                    SELECT 1 FROM recovery_contacts rc
                    WHERE LOWER(rc.lead_email) = LOWER(COALESCE(l.email, ''))
                    AND rc.lead_email != ''
                )
                ${feDateFilter}
                ORDER BY COALESCE(l.email, fe.visitor_id), fe.created_at DESC
                LIMIT $1 OFFSET $2
            `, [parseInt(limit), offset]);
            
            leads = result.rows;
            
            const countResult = await pool.query(`
                SELECT COUNT(DISTINCT COALESCE(l.email, fe.visitor_id)) as count
                FROM funnel_events fe
                LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
                WHERE fe.event IN ('page_view', 'landing_visit', 'phone_submitted', 'cta_clicked')
                ${language ? `AND l.funnel_language = '${language}'` : ''}
                AND NOT EXISTS (
                    SELECT 1 FROM funnel_events fe2
                    WHERE fe2.visitor_id = fe.visitor_id
                    AND fe2.event = 'checkout_clicked'
                )
                AND NOT EXISTS (
                    SELECT 1 FROM transactions t
                    WHERE LOWER(t.email) = LOWER(COALESCE(l.email, ''))
                    AND t.status = 'approved'
                )
                AND NOT EXISTS (
                    SELECT 1 FROM recovery_contacts rc
                    WHERE LOWER(rc.lead_email) = LOWER(COALESCE(l.email, ''))
                    AND rc.lead_email != ''
                )
                ${feDateFilter}
            `);
            totalCount = parseInt(countResult.rows[0]?.count || 0);
            
        } else if (segment === 'checkout_abandoned') {
            // Checkout abandoned leads
            const result = await pool.query(`
                SELECT DISTINCT ON (COALESCE(l.email, fe.visitor_id))
                    COALESCE(l.id, 0) as id,
                    COALESCE(l.email, '') as email,
                    COALESCE(l.name, 'Visitor') as name,
                    COALESCE(l.whatsapp, '') as phone,
                    COALESCE(l.country, '') as country,
                    COALESCE(l.country_code, '') as country_code,
                    l.funnel_language as language,
                    fe.created_at as last_event_at,
                    'checkout_clicked' as event,
                    (CASE WHEN l.funnel_language = 'es' THEN 27.00 * 5.50 ELSE 37.00 * 5.50 END) as potential_value,
                    'X AI Monitor' as product,
                    1 as event_count,
                    false as has_purchase,
                    0 as contact_attempts,
                    NULL as last_contact
                FROM funnel_events fe
                LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
                WHERE fe.event = 'checkout_clicked'
                ${language ? `AND l.funnel_language = '${language}'` : ''}
                AND NOT EXISTS (
                    SELECT 1 FROM transactions t 
                    WHERE LOWER(t.email) = LOWER(COALESCE(l.email, ''))
                    AND t.status = 'approved'
                )
                AND NOT EXISTS (
                    SELECT 1 FROM recovery_contacts rc
                    WHERE LOWER(rc.lead_email) = LOWER(COALESCE(l.email, ''))
                    AND rc.lead_email != ''
                )
                ${feDateFilter}
                ORDER BY COALESCE(l.email, fe.visitor_id), fe.created_at DESC
                LIMIT $1 OFFSET $2
            `, [parseInt(limit), offset]);
            
            leads = result.rows;
            
            // Get total count
            const countResult = await pool.query(`
                SELECT COUNT(DISTINCT COALESCE(l.email, fe.visitor_id)) as count
                FROM funnel_events fe
                LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
                WHERE fe.event = 'checkout_clicked'
                ${language ? `AND l.funnel_language = '${language}'` : ''}
                AND NOT EXISTS (
                    SELECT 1 FROM transactions t 
                    WHERE LOWER(t.email) = LOWER(COALESCE(l.email, ''))
                    AND t.status = 'approved'
                )
                AND NOT EXISTS (
                    SELECT 1 FROM recovery_contacts rc
                    WHERE LOWER(rc.lead_email) = LOWER(COALESCE(l.email, ''))
                    AND rc.lead_email != ''
                )
                ${feDateFilter}
            `);
            totalCount = parseInt(countResult.rows[0]?.count || 0);
            
        } else if (segment === 'payment_failed') {
            // Payment failed leads - DISTINCT by email, most recent transaction per person
            const result = await pool.query(`
                SELECT DISTINCT ON (LOWER(t.email))
                    t.id,
                    t.email,
                    t.name,
                    COALESCE(l.whatsapp, t.phone, '') as phone,
                    COALESCE(l.country, '') as country,
                    COALESCE(l.country_code, '') as country_code,
                    t.funnel_language as language,
                    t.created_at as last_event_at,
                    t.status as event,
                    CAST(t.value AS DECIMAL) * 5.50 as potential_value,
                    t.product,
                    (SELECT COUNT(*) FROM transactions t2 WHERE LOWER(t2.email) = LOWER(t.email) AND t2.status IN ('cancelled', 'refused', 'pending', 'waiting_payment')) as event_count,
                    false as has_purchase,
                    0 as contact_attempts,
                    NULL as last_contact
                FROM transactions t
                LEFT JOIN leads l ON LOWER(t.email) = LOWER(l.email)
                WHERE t.status IN ('cancelled', 'refused', 'pending', 'waiting_payment')
                AND t.email IS NOT NULL AND t.email != ''
                ${language ? `AND t.funnel_language = '${language}'` : ''}
                AND NOT EXISTS (
                    SELECT 1 FROM recovery_contacts rc
                    WHERE LOWER(rc.lead_email) = LOWER(t.email)
                )
                ${tDateFilter}
                ORDER BY LOWER(t.email), t.created_at DESC
            `, []);
            
            // Apply pagination in JS since DISTINCT ON + ORDER BY + LIMIT is tricky
            totalCount = result.rows.length;
            leads = result.rows.slice(offset, offset + parseInt(limit));
            
        } else if (segment === 'refund_requests') {
            // Refund requests - simplified query
            const result = await pool.query(`
                SELECT 
                    r.id,
                    r.email,
                    r.full_name as name,
                    r.phone,
                    '' as country,
                    '' as country_code,
                    r.funnel_language as language,
                    r.created_at as last_event_at,
                    r.reason as event,
                    CAST(r.value AS DECIMAL) as potential_value,
                    r.product,
                    1 as event_count,
                    true as has_purchase,
                    0 as contact_attempts,
                    NULL as last_contact,
                    r.status as refund_status,
                    r.protocol
                FROM refund_requests r
                WHERE r.status IN ('pending', 'handling', 'processing')
                ${language ? `AND r.funnel_language = '${language}'` : ''}
                ${rDateFilter}
                ORDER BY r.created_at DESC
                LIMIT $1 OFFSET $2
            `, [parseInt(limit), offset]);
            
            leads = result.rows;
            
            const countResult = await pool.query(`
                SELECT COUNT(*) as count FROM refund_requests
                WHERE status IN ('pending', 'handling', 'processing')
                ${language ? `AND funnel_language = '${language}'` : ''}
                ${plainDateFilter}
            `);
            totalCount = parseInt(countResult.rows[0]?.count || 0);
            
        } else if (segment === 'upsell_declined') {
            // Upsell declined - simplified query
            const result = await pool.query(`
                SELECT DISTINCT ON (l.email)
                    l.id,
                    l.email,
                    l.name,
                    COALESCE(l.whatsapp, '') as phone,
                    COALESCE(l.country, '') as country,
                    COALESCE(l.country_code, '') as country_code,
                    l.funnel_language as language,
                    fe.created_at as last_event_at,
                    fe.event,
                    67.00 as potential_value,
                    CASE 
                        WHEN fe.event LIKE 'upsell_1%' THEN 'Message Vault'
                        WHEN fe.event LIKE 'upsell_2%' THEN 'AI Vision'
                        WHEN fe.event LIKE 'upsell_3%' THEN 'VIP Priority'
                        ELSE 'Upsell'
                    END as product,
                    1 as event_count,
                    true as has_purchase,
                    0 as contact_attempts,
                    NULL as last_contact
                FROM funnel_events fe
                INNER JOIN leads l ON fe.visitor_id = l.visitor_id
                WHERE fe.event LIKE '%_declined'
                ${language ? `AND l.funnel_language = '${language}'` : ''}
                AND EXISTS (
                    SELECT 1 FROM transactions t 
                    WHERE LOWER(t.email) = LOWER(l.email)
                    AND t.status = 'approved'
                )
                ${feDateFilter}
                ORDER BY l.email, fe.created_at DESC
                LIMIT $1 OFFSET $2
            `, [parseInt(limit), offset]);
            
            leads = result.rows;
            
            const countResult = await pool.query(`
                SELECT COUNT(DISTINCT l.email) as count
                FROM funnel_events fe
                INNER JOIN leads l ON fe.visitor_id = l.visitor_id
                WHERE fe.event LIKE '%_declined'
                ${language ? `AND l.funnel_language = '${language}'` : ''}
                AND EXISTS (
                    SELECT 1 FROM transactions t 
                    WHERE LOWER(t.email) = LOWER(l.email)
                    AND t.status = 'approved'
                )
                ${feDateFilter}
            `);
            totalCount = parseInt(countResult.rows[0]?.count || 0);
            
        } else {
            return res.status(400).json({ error: 'Invalid segment' });
        }
        
        // Calculate scores for each lead
        const leadsWithScores = leads.map(lead => {
            const scoreData = calculateRecoveryScore(lead, segment);
            return {
                ...lead,
                score: scoreData.total,
                score_breakdown: scoreData.breakdown,
                time_ago: getTimeAgo(lead.last_event_at)
            };
        });
        
        // Filter by minimum score if specified
        let filteredLeads = leadsWithScores;
        if (minScore) {
            filteredLeads = leadsWithScores.filter(l => l.score >= parseInt(minScore));
        }
        
        // Filter by contact status if specified
        if (contactStatus === 'not_contacted') {
            filteredLeads = filteredLeads.filter(l => l.contact_attempts === 0);
        } else if (contactStatus === 'contacted') {
            filteredLeads = filteredLeads.filter(l => l.contact_attempts > 0);
        }
        
        // Sort leads
        if (sortBy === 'score') {
            filteredLeads.sort((a, b) => b.score - a.score);
        } else if (sortBy === 'value') {
            filteredLeads.sort((a, b) => b.potential_value - a.potential_value);
        }
        // Default is already sorted by time (most recent first)
        
        res.json({
            segment: segment,
            leads: filteredLeads,
            pagination: {
                page: parseInt(page),
                limit: parseInt(limit),
                total: totalCount,
                totalPages: Math.ceil(totalCount / parseInt(limit))
            }
        });
        
    } catch (error) {
        console.error('Error fetching recovery segment leads:', error);
        res.status(500).json({ error: 'Failed to fetch recovery leads' });
    }
});

// Helper function for time ago formatting
function getTimeAgo(date) {
    if (!date) return 'N/A';
    const now = new Date();
    const then = new Date(date);
    const diffMs = now - then;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);
    
    if (diffMins < 1) return 'Agora';
    if (diffMins < 60) return `${diffMins}min atrás`;
    if (diffHours < 24) return `${diffHours}h atrás`;
    if (diffDays < 7) return `${diffDays}d atrás`;
    if (diffDays < 30) return `${Math.floor(diffDays / 7)}sem atrás`;
    return `${Math.floor(diffDays / 30)}m atrás`;
}

// Register a recovery contact attempt
app.post('/api/admin/recovery/contact', authenticateToken, async (req, res) => {
    try {
        const { email, segment, template, channel, message } = req.body;
        
        if (!email || !segment) {
            return res.status(400).json({ error: 'Email and segment are required' });
        }
        
        const result = await pool.query(`
            INSERT INTO recovery_contacts (lead_email, segment, template_used, channel, message, status, created_at)
            VALUES ($1, $2, $3, $4, $5, 'sent', NOW())
            RETURNING *
        `, [email, segment, template || null, channel || 'whatsapp', message || null]);
        
        res.json({
            success: true,
            contact: result.rows[0]
        });
        
    } catch (error) {
        console.error('Error registering recovery contact:', error);
        res.status(500).json({ error: 'Failed to register contact' });
    }
});

// Update recovery contact status (responded, converted)
app.put('/api/admin/recovery/contact/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { status } = req.body;
        
        if (!['sent', 'responded', 'converted'].includes(status)) {
            return res.status(400).json({ error: 'Invalid status' });
        }
        
        const result = await pool.query(`
            UPDATE recovery_contacts SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING *
        `, [status, id]);
        
        res.json({ success: true, contact: result.rows[0] });
        
    } catch (error) {
        console.error('Error updating recovery contact:', error);
        res.status(500).json({ error: 'Failed to update contact' });
    }
});

// ==================== RECOVERY FUNNEL SYSTEM ====================

// Seed default recovery funnels if none exist
async function seedRecoveryFunnels() {
    try {
        // Ensure tables exist before seeding
        await pool.query(`CREATE TABLE IF NOT EXISTS recovery_funnels (
            id SERIAL PRIMARY KEY, segment VARCHAR(50) NOT NULL, name VARCHAR(100) NOT NULL,
            is_active BOOLEAN DEFAULT true, created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )`);
        await pool.query(`CREATE TABLE IF NOT EXISTS recovery_funnel_steps (
            id SERIAL PRIMARY KEY, funnel_id INTEGER REFERENCES recovery_funnels(id) ON DELETE CASCADE,
            step_number INTEGER NOT NULL, delay_hours INTEGER DEFAULT 24,
            template_en TEXT NOT NULL, template_es TEXT NOT NULL,
            channel VARCHAR(20) DEFAULT 'whatsapp', created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )`);
        await pool.query(`CREATE TABLE IF NOT EXISTS recovery_lead_progress (
            id SERIAL PRIMARY KEY, lead_email VARCHAR(255) NOT NULL,
            funnel_id INTEGER REFERENCES recovery_funnels(id) ON DELETE CASCADE,
            current_step INTEGER DEFAULT 0, status VARCHAR(20) DEFAULT 'active',
            next_contact_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(lead_email, funnel_id)
        )`);
        
        const existing = await pool.query('SELECT COUNT(*) as count FROM recovery_funnels');
        if (parseInt(existing.rows[0].count) > 0) return;
        
        const funnelSeeds = [
            {
                segment: 'lost_visitors',
                name: 'Lost Visitors Recovery',
                steps: [
                    { step: 1, delay: 0, en: "Hey {name}! 👋 I noticed you were checking out X AI Monitor earlier. Curious about what it does? It uses AI to monitor conversations and reveal what's really going on. Want me to show you how it works? 🔥", es: "¡Hola {name}! 👋 Vi que estabas mirando X AI Monitor. ¿Curioso por saber qué hace? Usa IA para monitorear conversaciones y revelar lo que realmente pasa. ¿Quieres que te muestre cómo funciona? 🔥" },
                    { step: 2, delay: 24, en: "Hi {name}! Just a quick follow-up — X AI Monitor has already helped thousands of people uncover hidden truths. Today only, you can try it for a special price. Want the link? 💰", es: "¡Hola {name}! Solo un seguimiento rápido — X AI Monitor ya ha ayudado a miles de personas a descubrir verdades ocultas. Solo por hoy, puedes probarlo a un precio especial. ¿Quieres el link? 💰" },
                    { step: 3, delay: 48, en: "Last chance, {name}! 🚨 We're closing registration for X AI Monitor soon. Don't miss your chance to discover the truth. This is your final opportunity at this price!", es: "¡Última oportunidad, {name}! 🚨 Estamos cerrando las inscripciones de X AI Monitor pronto. No pierdas tu chance de descubrir la verdad. ¡Esta es tu última oportunidad a este precio!" }
                ]
            },
            {
                segment: 'checkout_abandoned',
                name: 'Checkout Abandoned Recovery',
                steps: [
                    { step: 1, delay: 0, en: "Hey {name}! 👋 I noticed you were about to get X AI Monitor but didn't complete your purchase. Is there anything I can help with? Just wanted to let you know we have LIMITED spots available! 🔥", es: "¡Hola {name}! 👋 Vi que estabas por comprar X AI Monitor pero no completaste. ¿Hay algo en que pueda ayudarte? Solo quería avisarte que tenemos CUPOS LIMITADOS! 🔥" },
                    { step: 2, delay: 24, en: "Hi {name}! 🎁 I have a special offer just for you: Get 50% OFF on X AI Monitor for the next 24 hours! Use this exclusive link: [LINK]. Don't let this opportunity slip away!", es: "¡Hola {name}! 🎁 Tengo una oferta especial solo para ti: ¡50% DE DESCUENTO en X AI Monitor por las próximas 24 horas! Usa este link exclusivo: [LINK]. ¡No dejes escapar esta oportunidad!" },
                    { step: 3, delay: 48, en: "Hey {name}! 👋 Just checking in — is there anything stopping you from trying X AI Monitor? Any questions about how it works? I'm here to help! 😊", es: "¡Hola {name}! 👋 Solo quería saber — ¿hay algo que te impida probar X AI Monitor? ¿Alguna pregunta sobre cómo funciona? ¡Estoy aquí para ayudar! 😊" },
                    { step: 4, delay: 72, en: "Final notice, {name}! ⏰ Your exclusive discount for X AI Monitor expires TODAY. After this, it goes back to full price. This is your last chance — grab it now!", es: "¡Último aviso, {name}! ⏰ Tu descuento exclusivo de X AI Monitor expira HOY. Después de esto, vuelve al precio completo. Esta es tu última oportunidad — ¡aprovéchala ahora!" }
                ]
            },
            {
                segment: 'payment_failed',
                name: 'Payment Failed Recovery',
                steps: [
                    { step: 1, delay: 0, en: "Hi {name}! I noticed there was an issue with your payment for {product}. Sometimes this happens due to bank limits. Would you like to try again with a different card or payment method? I can help! 💳", es: "¡Hola {name}! Vi que hubo un problema con tu pago de {product}. A veces esto pasa por límites del banco. ¿Te gustaría intentar con otra tarjeta o método de pago? ¡Puedo ayudarte! 💳" },
                    { step: 2, delay: 24, en: "Hey {name}! Your payment for {product} didn't go through. No worries! We have alternative payment options (PayPal, different cards). Would you like me to send you a new payment link? 🔄", es: "¡Hola {name}! Tu pago de {product} no se procesó. ¡No te preocupes! Tenemos opciones de pago alternativas (PayPal, otras tarjetas). ¿Te gustaría que te envíe otro link de pago? 🔄" },
                    { step: 3, delay: 48, en: "Hi {name}! Just following up on your payment for {product}. I really don't want you to miss out! As a goodwill gesture, I can offer you a 20% discount if you complete your purchase today. Want the link? 🎁", es: "¡Hola {name}! Solo dando seguimiento a tu pago de {product}. ¡Realmente no quiero que te lo pierdas! Como gesto de buena voluntad, puedo ofrecerte un 20% de descuento si completas tu compra hoy. ¿Quieres el link? 🎁" }
                ]
            },
            {
                segment: 'refund_requests',
                name: 'Refund Prevention',
                steps: [
                    { step: 1, delay: 0, en: "Hi {name}! I received your refund request. Before we proceed, I'd love to understand what happened. Was there something that didn't meet your expectations? Maybe I can help solve it! 🤝", es: "¡Hola {name}! Recibí tu solicitud de reembolso. Antes de proceder, me gustaría entender qué pasó. ¿Hubo algo que no cumplió tus expectativas? ¡Tal vez pueda ayudar a resolverlo! 🤝" },
                    { step: 2, delay: 24, en: "Hey {name}! Many customers had similar concerns about {product} but after a quick tutorial, they loved the results! Would you give me 5 minutes to show you how to get the best out of it? I have some exclusive tips! 🎯", es: "¡Hola {name}! Muchos clientes tenían dudas similares sobre {product} pero después de un tutorial rápido, ¡amaron los resultados! ¿Me darías 5 minutos para mostrarte cómo aprovecharlo al máximo? ¡Tengo tips exclusivos! 🎯" },
                    { step: 3, delay: 48, en: "Hi {name}! I understand if {product} isn't for you. But before you go, would you consider a partial refund + VIP support access? I want to make sure you get real value from your investment. What do you think? 💬", es: "¡Hola {name}! Entiendo si {product} no es para ti. Pero antes de irte, ¿considerarías un reembolso parcial + acceso a soporte VIP? Quiero asegurarme de que obtengas valor real de tu inversión. ¿Qué te parece? 💬" }
                ]
            },
            {
                segment: 'upsell_declined',
                name: 'Upsell Recovery',
                steps: [
                    { step: 1, delay: 0, en: "Hi {name}! Congrats on your purchase! 🎉 I noticed you didn't add {product} to your order. Did you know it can unlock advanced features? I have a special 30% discount just for you!", es: "¡Hola {name}! ¡Felicidades por tu compra! 🎉 Vi que no agregaste {product} a tu pedido. ¿Sabías que puede desbloquear funciones avanzadas? ¡Tengo un descuento especial del 30% solo para ti!" },
                    { step: 2, delay: 48, en: "Hey {name}! Quick question: Would you be interested in adding {product} to your X AI Monitor for a special bundle price? It's way more powerful together! 🚀 Only a few spots left at this price.", es: "¡Hola {name}! Pregunta rápida: ¿Te interesaría agregar {product} a tu X AI Monitor por un precio especial de combo? ¡Es mucho más poderoso junto! 🚀 Solo quedan pocos cupos a este precio." },
                    { step: 3, delay: 96, en: "Last chance, {name}! The exclusive bundle deal for {product} expires soon. After this, it goes back to full price ($67). Grab it now at 30% OFF: [LINK] ⏰", es: "¡Última oportunidad, {name}! La oferta exclusiva de combo de {product} expira pronto. Después, vuelve al precio completo ($67). Aprovéchalo ahora con 30% DE DESCUENTO: [LINK] ⏰" }
                ]
            }
        ];
        
        for (const funnel of funnelSeeds) {
            const funnelResult = await pool.query(
                'INSERT INTO recovery_funnels (segment, name) VALUES ($1, $2) RETURNING id',
                [funnel.segment, funnel.name]
            );
            const funnelId = funnelResult.rows[0].id;
            
            for (const step of funnel.steps) {
                await pool.query(
                    'INSERT INTO recovery_funnel_steps (funnel_id, step_number, delay_hours, template_en, template_es) VALUES ($1, $2, $3, $4, $5)',
                    [funnelId, step.step, step.delay, step.en, step.es]
                );
            }
        }
        
        console.log('Recovery funnels seeded successfully');
    } catch (error) {
        console.error('Error seeding recovery funnels:', error);
    }
}

// Get all recovery funnels with steps
app.get('/api/admin/recovery/funnels', authenticateToken, async (req, res) => {
    try {
        await seedRecoveryFunnels();
        
        const funnels = await pool.query(`
            SELECT f.*, 
                   json_agg(json_build_object(
                       'id', s.id, 'step_number', s.step_number, 'delay_hours', s.delay_hours,
                       'template_en', s.template_en, 'template_es', s.template_es, 'channel', s.channel
                   ) ORDER BY s.step_number) as steps
            FROM recovery_funnels f
            LEFT JOIN recovery_funnel_steps s ON f.id = s.funnel_id
            GROUP BY f.id
            ORDER BY f.id
        `);
        
        res.json({ funnels: funnels.rows });
    } catch (error) {
        console.error('Error fetching recovery funnels:', error);
        res.status(500).json({ error: 'Failed to fetch funnels' });
    }
});

// Get funnel progress for a specific lead
app.get('/api/admin/recovery/funnel/progress/:email', authenticateToken, async (req, res) => {
    try {
        const { email } = req.params;
        
        const progress = await pool.query(`
            SELECT p.*, f.name as funnel_name, f.segment,
                   (SELECT COUNT(*) FROM recovery_funnel_steps WHERE funnel_id = f.id) as total_steps,
                   (SELECT json_agg(json_build_object(
                       'id', s.id, 'step_number', s.step_number, 'delay_hours', s.delay_hours,
                       'template_en', s.template_en, 'template_es', s.template_es
                   ) ORDER BY s.step_number) FROM recovery_funnel_steps s WHERE s.funnel_id = f.id) as steps
            FROM recovery_lead_progress p
            JOIN recovery_funnels f ON p.funnel_id = f.id
            WHERE p.lead_email = $1
            ORDER BY p.updated_at DESC
        `, [email]);
        
        // Also get contact history
        const contacts = await pool.query(`
            SELECT * FROM recovery_contacts
            WHERE lead_email = $1
            ORDER BY created_at DESC
        `, [email]);
        
        res.json({
            progress: progress.rows,
            contacts: contacts.rows
        });
    } catch (error) {
        console.error('Error fetching lead progress:', error);
        res.status(500).json({ error: 'Failed to fetch progress' });
    }
});

// Advance lead to next funnel step (1-click dispatch)
app.post('/api/admin/recovery/funnel/advance', authenticateToken, async (req, res) => {
    try {
        const { email, segment, name, phone, product, language } = req.body;
        
        if (!email || !segment) {
            return res.status(400).json({ error: 'Email e segmento são obrigatórios' });
        }
        
        // Ensure funnels are seeded
        await seedRecoveryFunnels();
        
        // Get the funnel for this segment
        const funnelResult = await pool.query(
            'SELECT * FROM recovery_funnels WHERE segment = $1 AND is_active = true LIMIT 1',
            [segment]
        );
        
        if (funnelResult.rows.length === 0) {
            return res.status(404).json({ error: 'Nenhum funil ativo para este segmento' });
        }
        
        const funnel = funnelResult.rows[0];
        
        // Get or create lead progress
        let progressResult = await pool.query(
            'SELECT * FROM recovery_lead_progress WHERE lead_email = $1 AND funnel_id = $2',
            [email, funnel.id]
        );
        
        let currentStep = 0;
        if (progressResult.rows.length > 0) {
            currentStep = progressResult.rows[0].current_step;
        }
        
        const nextStep = currentStep + 1;
        
        // Get the next step template
        const stepResult = await pool.query(
            'SELECT * FROM recovery_funnel_steps WHERE funnel_id = $1 AND step_number = $2',
            [funnel.id, nextStep]
        );
        
        if (stepResult.rows.length === 0) {
            return res.status(400).json({ error: 'Lead já completou todos os passos do funil', completed: true });
        }
        
        const step = stepResult.rows[0];
        const totalSteps = await pool.query('SELECT COUNT(*) as count FROM recovery_funnel_steps WHERE funnel_id = $1', [funnel.id]);
        
        // Get message in correct language
        const lang = language || 'en';
        let message = lang === 'es' ? step.template_es : step.template_en;
        message = message.replace(/\{name\}/g, name || 'there');
        message = message.replace(/\{product\}/g, product || 'X AI Monitor');
        
        // Update or insert progress
        const delayHours = parseInt(step.delay_hours) || 24;
        if (progressResult.rows.length > 0) {
            await pool.query(
                `UPDATE recovery_lead_progress SET current_step = $1, updated_at = NOW(), next_contact_at = NOW() + interval '1 hour' * $2 WHERE lead_email = $3 AND funnel_id = $4`,
                [nextStep, delayHours, email, funnel.id]
            );
        } else {
            await pool.query(
                `INSERT INTO recovery_lead_progress (lead_email, funnel_id, current_step, status, next_contact_at) VALUES ($1, $2, $3, 'active', NOW() + interval '1 hour' * $4)`,
                [email, funnel.id, nextStep, delayHours]
            );
        }
        
        // Send message automatically via Z-API
        const cleanPhone = (phone || '').replace(/\D/g, '');
        let sendResult = { sent: false, error: null };
        
        if (cleanPhone && cleanPhone.length >= 10) {
            try {
                const zapiHeaders = { 'Content-Type': 'application/json' };
                if (ZAPI_CLIENT_TOKEN) zapiHeaders['Client-Token'] = ZAPI_CLIENT_TOKEN;
                
                const zapiResponse = await fetch(`${ZAPI_BASE_URL}/send-text`, {
                    method: 'POST',
                    headers: zapiHeaders,
                    body: JSON.stringify({
                        phone: cleanPhone,
                        message: message,
                        delayMessage: 3
                    })
                });
                
                const zapiData = await zapiResponse.json();
                
                if (zapiResponse.ok && zapiData.messageId) {
                    sendResult = { sent: true, messageId: zapiData.messageId, zaapId: zapiData.zaapId };
                    console.log(`✅ Recovery message sent to ${cleanPhone} (Step ${nextStep}) - ID: ${zapiData.messageId}`);
                    
                    // Log in whatsapp_messages table
                    try {
                        await pool.query(`
                            INSERT INTO whatsapp_messages (phone, message, message_id, zaap_id, status, sent_by, created_at)
                            VALUES ($1, $2, $3, $4, 'sent', 'recovery_funnel', NOW())
                        `, [cleanPhone, message, zapiData.messageId, zapiData.zaapId]);
                    } catch (dbErr) {
                        console.log('WhatsApp message log skipped:', dbErr.message);
                    }
                } else {
                    sendResult = { sent: false, error: zapiData.error || zapiData.message || 'Erro Z-API' };
                    console.error(`❌ Z-API send error for ${cleanPhone}:`, zapiData);
                }
            } catch (zapiErr) {
                sendResult = { sent: false, error: zapiErr.message };
                console.error(`❌ Z-API connection error for ${cleanPhone}:`, zapiErr.message);
            }
        } else {
            sendResult = { sent: false, error: 'Número de telefone inválido ou ausente' };
        }
        
        // Record the contact with send status
        const contactStatus = sendResult.sent ? 'sent' : 'failed';
        await pool.query(
            `INSERT INTO recovery_contacts (lead_email, segment, template_used, channel, message, status) VALUES ($1, $2, $3, $4, $5, '${contactStatus}')`,
            [email, segment, `step_${nextStep}`, step.channel || 'whatsapp', message]
        );
        
        res.json({
            success: sendResult.sent,
            step: nextStep,
            total_steps: parseInt(totalSteps.rows[0].count),
            message: message,
            sent_via_zapi: sendResult.sent,
            messageId: sendResult.messageId || null,
            send_error: sendResult.error || null,
            completed: nextStep >= parseInt(totalSteps.rows[0].count)
        });
        
    } catch (error) {
        console.error('Error advancing funnel step:', error);
        res.status(500).json({ error: 'Falha ao avançar passo do funil: ' + error.message });
    }
});

// Bulk advance multiple leads
app.post('/api/admin/recovery/funnel/bulk-advance', authenticateToken, async (req, res) => {
    try {
        const { leads, segment } = req.body;
        
        if (!leads || !Array.isArray(leads) || leads.length === 0) {
            return res.status(400).json({ error: 'Lista de leads é obrigatória' });
        }
        
        await seedRecoveryFunnels();
        
        const results = [];
        for (const lead of leads) {
            try {
                const funnelResult = await pool.query(
                    'SELECT * FROM recovery_funnels WHERE segment = $1 AND is_active = true LIMIT 1',
                    [segment]
                );
                
                if (funnelResult.rows.length === 0) continue;
                const funnel = funnelResult.rows[0];
                
                let progressResult = await pool.query(
                    'SELECT * FROM recovery_lead_progress WHERE lead_email = $1 AND funnel_id = $2',
                    [lead.email, funnel.id]
                );
                
                let currentStep = progressResult.rows.length > 0 ? progressResult.rows[0].current_step : 0;
                const nextStep = currentStep + 1;
                
                const stepResult = await pool.query(
                    'SELECT * FROM recovery_funnel_steps WHERE funnel_id = $1 AND step_number = $2',
                    [funnel.id, nextStep]
                );
                
                if (stepResult.rows.length === 0) {
                    results.push({ email: lead.email, status: 'completed' });
                    continue;
                }
                
                const step = stepResult.rows[0];
                const lang = lead.language || 'en';
                let message = lang === 'es' ? step.template_es : step.template_en;
                message = message.replace(/\{name\}/g, lead.name || 'there');
                message = message.replace(/\{product\}/g, lead.product || 'X AI Monitor');
                
                if (progressResult.rows.length > 0) {
                    await pool.query(
                        'UPDATE recovery_lead_progress SET current_step = $1, updated_at = NOW() WHERE lead_email = $2 AND funnel_id = $3',
                        [nextStep, lead.email, funnel.id]
                    );
                } else {
                    await pool.query(
                        'INSERT INTO recovery_lead_progress (lead_email, funnel_id, current_step, status) VALUES ($1, $2, $3, \'active\')',
                        [lead.email, funnel.id, nextStep]
                    );
                }
                
                // Send via Z-API automatically
                const cleanPhone = (lead.phone || '').replace(/\D/g, '');
                let sent = false;
                
                if (cleanPhone && cleanPhone.length >= 10) {
                    try {
                        const zapiHeaders = { 'Content-Type': 'application/json' };
                        if (ZAPI_CLIENT_TOKEN) zapiHeaders['Client-Token'] = ZAPI_CLIENT_TOKEN;
                        
                        const zapiResponse = await fetch(`${ZAPI_BASE_URL}/send-text`, {
                            method: 'POST',
                            headers: zapiHeaders,
                            body: JSON.stringify({ phone: cleanPhone, message: message, delayMessage: 3 })
                        });
                        
                        const zapiData = await zapiResponse.json();
                        sent = zapiResponse.ok && !!zapiData.messageId;
                        
                        if (sent) {
                            console.log(`✅ Bulk recovery sent to ${cleanPhone} (Step ${nextStep})`);
                            try {
                                await pool.query(`
                                    INSERT INTO whatsapp_messages (phone, message, message_id, zaap_id, status, sent_by, created_at)
                                    VALUES ($1, $2, $3, $4, 'sent', 'recovery_bulk', NOW())
                                `, [cleanPhone, message, zapiData.messageId, zapiData.zaapId]);
                            } catch (dbErr) { /* ignore */ }
                        }
                    } catch (zapiErr) {
                        console.error(`❌ Bulk Z-API error for ${cleanPhone}:`, zapiErr.message);
                    }
                    
                    // Rate limit: 1s between sends
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
                
                const contactStatus = sent ? 'sent' : 'failed';
                await pool.query(
                    `INSERT INTO recovery_contacts (lead_email, segment, template_used, channel, message, status) VALUES ($1, $2, $3, 'whatsapp', $4, '${contactStatus}')`,
                    [lead.email, segment, `step_${nextStep}`, message]
                );
                
                results.push({
                    email: lead.email,
                    status: sent ? 'advanced' : 'send_failed',
                    step: nextStep,
                    sent_via_zapi: sent
                });
            } catch (err) {
                results.push({ email: lead.email, status: 'error', error: err.message });
            }
        }
        
        const sentCount = results.filter(r => r.sent_via_zapi).length;
        const failedCount = results.filter(r => r.status === 'send_failed').length;
        res.json({ success: true, results, sent: sentCount, failed: failedCount });
    } catch (error) {
        console.error('Error bulk advancing:', error);
        res.status(500).json({ error: 'Falha no disparo em massa' });
    }
});

// Resend a dispatch message via Z-API
app.post('/api/admin/recovery/dispatch-resend', authenticateToken, async (req, res) => {
    try {
        const { dispatch_id } = req.body;
        if (!dispatch_id) return res.status(400).json({ error: 'dispatch_id é obrigatório' });
        
        // Get original dispatch
        const dispatchResult = await pool.query(
            `SELECT rc.*, l.whatsapp as lead_phone, l.name as lead_name
             FROM recovery_contacts rc
             LEFT JOIN leads l ON LOWER(rc.lead_email) = LOWER(l.email)
             WHERE rc.id = $1`,
            [dispatch_id]
        );
        
        if (dispatchResult.rows.length === 0) {
            return res.status(404).json({ error: 'Disparo não encontrado' });
        }
        
        const dispatch = dispatchResult.rows[0];
        const phone = (dispatch.lead_phone || '').replace(/\D/g, '');
        
        if (!phone || phone.length < 10) {
            return res.status(400).json({ error: 'Número de telefone inválido' });
        }
        
        if (!dispatch.message) {
            return res.status(400).json({ error: 'Mensagem original não encontrada' });
        }
        
        // Send via Z-API
        const zapiHeaders = { 'Content-Type': 'application/json' };
        if (ZAPI_CLIENT_TOKEN) zapiHeaders['Client-Token'] = ZAPI_CLIENT_TOKEN;
        
        const zapiResponse = await fetch(`${ZAPI_BASE_URL}/send-text`, {
            method: 'POST',
            headers: zapiHeaders,
            body: JSON.stringify({ phone, message: dispatch.message, delayMessage: 3 })
        });
        
        const zapiData = await zapiResponse.json();
        const sent = zapiResponse.ok && !!zapiData.messageId;
        
        if (sent) {
            // Log new contact entry
            await pool.query(
                `INSERT INTO recovery_contacts (lead_email, segment, template_used, channel, message, status)
                 VALUES ($1, $2, $3, 'whatsapp', $4, 'sent')`,
                [dispatch.lead_email, dispatch.segment, (dispatch.template_used || '') + '_resend', dispatch.message]
            );
            
            // Log in whatsapp_messages
            try {
                await pool.query(`
                    INSERT INTO whatsapp_messages (phone, message, message_id, zaap_id, status, sent_by, created_at)
                    VALUES ($1, $2, $3, $4, 'sent', 'recovery_resend', NOW())
                `, [phone, dispatch.message, zapiData.messageId, zapiData.zaapId]);
            } catch (dbErr) { /* ignore */ }
            
            console.log(`🔄 Resend to ${phone} - ID: ${zapiData.messageId}`);
            res.json({ success: true, messageId: zapiData.messageId });
        } else {
            console.error(`❌ Resend failed for ${phone}:`, zapiData);
            res.status(500).json({ error: zapiData.error || zapiData.message || 'Falha ao reenviar via Z-API' });
        }
    } catch (error) {
        console.error('Error resending dispatch:', error);
        res.status(500).json({ error: 'Falha ao reenviar: ' + error.message });
    }
});

// Mark lead as recovered
app.post('/api/admin/recovery/funnel/mark-recovered', authenticateToken, async (req, res) => {
    try {
        const { email, segment } = req.body;
        
        // Update all progress entries for this lead
        await pool.query(
            'UPDATE recovery_lead_progress SET status = \'converted\', updated_at = NOW() WHERE lead_email = $1',
            [email]
        );
        
        // Update contact status
        await pool.query(
            'UPDATE recovery_contacts SET status = \'converted\', updated_at = NOW() WHERE lead_email = $1 AND segment = $2',
            [email, segment]
        );
        
        res.json({ success: true });
    } catch (error) {
        console.error('Error marking as recovered:', error);
        res.status(500).json({ error: 'Failed to mark as recovered' });
    }
});

// Get recovery dispatch log (message history)
app.get('/api/admin/recovery/dispatch-log', authenticateToken, async (req, res) => {
    try {
        // Ensure table exists
        await pool.query(`
            CREATE TABLE IF NOT EXISTS recovery_contacts (
                id SERIAL PRIMARY KEY, lead_email VARCHAR(255) NOT NULL, segment VARCHAR(50) NOT NULL,
                template_used VARCHAR(100), channel VARCHAR(20) DEFAULT 'whatsapp', message TEXT,
                status VARCHAR(20) DEFAULT 'sent', created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        `);
        
        const { segment, status, page = 1, limit = 25, search } = req.query;
        const offset = (parseInt(page) - 1) * parseInt(limit);
        
        let whereClause = 'WHERE 1=1';
        const params = [];
        let paramIndex = 1;
        
        if (segment) {
            whereClause += ` AND rc.segment = $${paramIndex}`;
            params.push(segment);
            paramIndex++;
        }
        
        if (status) {
            whereClause += ` AND rc.status = $${paramIndex}`;
            params.push(status);
            paramIndex++;
        }
        
        if (search) {
            whereClause += ` AND (rc.lead_email ILIKE $${paramIndex} OR rc.message ILIKE $${paramIndex})`;
            params.push(`%${search}%`);
            paramIndex++;
        }
        
        // Get total count
        const countResult = await pool.query(
            `SELECT COUNT(*) as total FROM recovery_contacts rc ${whereClause}`,
            params
        );
        const total = parseInt(countResult.rows[0].total);
        
        // Get stats summary
        const statsResult = await pool.query(`
            SELECT 
                COUNT(*) as total_dispatches,
                COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
                COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                COUNT(*) FILTER (WHERE status = 'converted') as converted_count,
                COUNT(DISTINCT lead_email) as unique_leads,
                COUNT(*) FILTER (WHERE created_at >= NOW() - interval '24 hours') as last_24h,
                COUNT(*) FILTER (WHERE created_at >= NOW() - interval '7 days') as last_7d
            FROM recovery_contacts
        `);
        
        // Get dispatches with lead info + funnel progress
        const dispatchParams = [...params, parseInt(limit), offset];
        const dispatches = await pool.query(`
            SELECT 
                rc.id, rc.lead_email, rc.segment, rc.template_used, rc.channel, 
                rc.message, rc.status, rc.created_at,
                l.name as lead_name, l.whatsapp as lead_phone, l.funnel_language as lead_language,
                l.whatsapp_verified, l.whatsapp_profile_pic,
                COALESCE(p.current_step, 0) as funnel_current_step,
                COALESCE(p.status, 'active') as funnel_status,
                (SELECT COUNT(*) FROM recovery_funnel_steps s 
                 JOIN recovery_funnels f2 ON s.funnel_id = f2.id 
                 WHERE f2.segment = rc.segment AND f2.is_active = true) as funnel_total_steps,
                (SELECT COUNT(*) FROM recovery_contacts rc2 
                 WHERE LOWER(rc2.lead_email) = LOWER(rc.lead_email)) as total_contacts_for_lead
            FROM recovery_contacts rc
            LEFT JOIN leads l ON LOWER(rc.lead_email) = LOWER(l.email)
            LEFT JOIN recovery_funnels f ON f.segment = rc.segment AND f.is_active = true
            LEFT JOIN recovery_lead_progress p ON LOWER(p.lead_email) = LOWER(rc.lead_email) AND p.funnel_id = f.id
            ${whereClause}
            ORDER BY rc.created_at DESC
            LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
        `, dispatchParams);
        
        console.log(`📋 Dispatch log: ${dispatches.rows.length} results, total: ${total}`);
        
        res.json({
            dispatches: dispatches.rows,
            total,
            page: parseInt(page),
            limit: parseInt(limit),
            total_pages: Math.ceil(total / parseInt(limit)),
            stats: statsResult.rows[0]
        });
        
    } catch (error) {
        console.error('Error fetching dispatch log:', error);
        res.status(500).json({ error: 'Falha ao carregar histórico de disparos: ' + error.message });
    }
});

// Get recovery templates (legacy + funnel-based)
app.get('/api/admin/recovery/templates', authenticateToken, async (req, res) => {
    try {
        const templates = {
            lost_visitors: [
                {
                    id: 'curiosity',
                    name: 'Curiosity Hook',
                    icon: '👻',
                    message_en: "Hey {name}! 👋 I noticed you were checking out X AI Monitor earlier. Curious about what it does? It uses AI to monitor conversations and reveal what's really going on. Want me to show you how it works? 🔥",
                    message_es: "¡Hola {name}! 👋 Vi que estabas mirando X AI Monitor. ¿Curioso por saber qué hace? Usa IA para monitorear conversaciones y revelar lo que realmente pasa. ¿Quieres que te muestre cómo funciona? 🔥"
                },
                {
                    id: 'social_proof',
                    name: 'Social Proof',
                    icon: '⭐',
                    message_en: "Hi {name}! Just wanted to share: X AI Monitor has already helped 10,000+ people uncover hidden truths. Today only, you can try it at a special price. Want the link? 💰",
                    message_es: "¡Hola {name}! Solo quería compartir: X AI Monitor ya ha ayudado a más de 10,000 personas a descubrir verdades ocultas. Solo por hoy, puedes probarlo a un precio especial. ¿Quieres el link? 💰"
                }
            ],
            checkout_abandoned: [
                {
                    id: 'urgency',
                    name: 'Urgency',
                    icon: '⏰',
                    message_en: "Hey {name}! 👋 I noticed you were checking out X AI Monitor but didn't complete your purchase. Just wanted to let you know we have LIMITED spots available. Don't miss out on discovering what's happening behind the scenes! 🔥",
                    message_es: "¡Hola {name}! 👋 Vi que estabas por comprar X AI Monitor pero no completaste. Solo quería avisarte que tenemos CUPOS LIMITADOS. ¡No te pierdas la oportunidad de descubrir qué está pasando! 🔥"
                },
                {
                    id: 'discount',
                    name: 'Special Discount',
                    icon: '💰',
                    message_en: "Hi {name}! 🎁 I have a special offer just for you: Get 50% OFF on X AI Monitor for the next 24 hours! Use this exclusive link: [LINK]. Don't let this opportunity slip away!",
                    message_es: "¡Hola {name}! 🎁 Tengo una oferta especial solo para ti: ¡50% DE DESCUENTO en X AI Monitor por las próximas 24 horas! Usa este link exclusivo: [LINK]. ¡No dejes escapar esta oportunidad!"
                },
                {
                    id: 'support',
                    name: 'Support',
                    icon: '🤝',
                    message_en: "Hey {name}! 👋 I noticed you were interested in X AI Monitor. Is there anything I can help you with? Any questions about how it works? I'm here to help! 😊",
                    message_es: "¡Hola {name}! 👋 Vi que te interesó X AI Monitor. ¿Hay algo en lo que pueda ayudarte? ¿Alguna pregunta sobre cómo funciona? ¡Estoy aquí para ayudar! 😊"
                }
            ],
            payment_failed: [
                {
                    id: 'retry',
                    name: 'Tentar Novamente',
                    icon: '🔄',
                    message_en: "Hi {name}! I noticed there was an issue with your payment for {product}. Sometimes this happens due to bank limits. Would you like to try again with a different card or payment method? I can help! 💳",
                    message_es: "¡Hola {name}! Vi que hubo un problema con tu pago de {product}. A veces esto pasa por límites del banco. ¿Te gustaría intentar con otra tarjeta o método de pago? ¡Puedo ayudarte! 💳",
                    message_pt: "Oi {name}! Vi que houve um problema com seu pagamento do {product}. Às vezes isso acontece por limites do banco. Quer tentar com outro cartão ou forma de pagamento? Posso ajudar! 💳"
                },
                {
                    id: 'alternative',
                    name: 'Pagamento Alternativo',
                    icon: '💳',
                    message_en: "Hey {name}! Your payment for {product} didn't go through. No worries! We have PIX and Boleto options available. Would you like me to send you an alternative payment link?",
                    message_es: "¡Hola {name}! Tu pago de {product} no se procesó. ¡No te preocupes! Tenemos opciones de pago alternativas disponibles. ¿Te gustaría que te envíe otro link de pago?",
                    message_pt: "Oi {name}! Seu pagamento do {product} não foi processado. Sem problemas! Temos opções de PIX e Boleto disponíveis. Quer que eu te envie um link de pagamento alternativo?"
                }
            ],
            refund_requests: [
                {
                    id: 'understand',
                    name: 'Entender Motivo',
                    icon: '💬',
                    message_en: "Hi {name}! I received your refund request. Before we proceed, I'd love to understand what happened. Was there something that didn't meet your expectations? Maybe I can help solve it! 🤝",
                    message_es: "¡Hola {name}! Recibí tu solicitud de reembolso. Antes de proceder, me gustaría entender qué pasó. ¿Hubo algo que no cumplió tus expectativas? ¡Tal vez pueda ayudar a resolverlo! 🤝",
                    message_pt: "Oi {name}! Recebi seu pedido de reembolso. Antes de prosseguir, gostaria de entender o que aconteceu. Teve algo que não atendeu suas expectativas? Talvez eu possa ajudar a resolver! 🤝"
                },
                {
                    id: 'offer_help',
                    name: 'Oferecer Ajuda',
                    icon: '🎯',
                    message_en: "Hey {name}! I saw you requested a refund for {product}. Many customers had similar concerns but after a quick tutorial, they loved the results! Would you give me 5 minutes to show you how to get the best out of it?",
                    message_es: "¡Hola {name}! Vi que pediste reembolso de {product}. ¡Muchos clientes tenían dudas similares pero después de un tutorial rápido, amaron los resultados! ¿Me darías 5 minutos para mostrarte cómo aprovecharlo al máximo?",
                    message_pt: "Oi {name}! Vi que você pediu reembolso do {product}. Muitos clientes tinham dúvidas parecidas mas depois de um tutorial rápido, amaram os resultados! Me dá 5 minutinhos pra te mostrar como aproveitar ao máximo?"
                }
            ],
            upsell_declined: [
                {
                    id: 'benefit',
                    name: 'Benefício Extra',
                    icon: '🎁',
                    message_en: "Hi {name}! Congrats on your purchase! 🎉 I noticed you didn't add {product} to your order. Did you know it can help you [BENEFIT]? I have a special 30% discount just for you!",
                    message_es: "¡Hola {name}! ¡Felicidades por tu compra! 🎉 Vi que no agregaste {product} a tu pedido. ¿Sabías que puede ayudarte a [BENEFICIO]? ¡Tengo un descuento especial del 30% solo para ti!",
                    message_pt: "Oi {name}! Parabéns pela compra! 🎉 Vi que você não adicionou o {product} no seu pedido. Sabia que ele pode te ajudar a [BENEFÍCIO]? Tenho um desconto especial de 30% só pra você!"
                },
                {
                    id: 'bundle',
                    name: 'Oferta Combo',
                    icon: '📦',
                    message_en: "Hey {name}! Quick question: Would you be interested in adding {product} to your X AI Monitor for a special bundle price? It's way more powerful together! 🚀",
                    message_es: "¡Hola {name}! Pregunta rápida: ¿Te interesaría agregar {product} a tu X AI Monitor por un precio especial de combo? ¡Es mucho más poderoso junto! 🚀",
                    message_pt: "Oi {name}! Pergunta rápida: Você teria interesse em adicionar o {product} ao seu X AI Monitor por um preço especial de combo? É muito mais poderoso junto! 🚀"
                }
            ]
        };
        
        res.json({ templates });
        
    } catch (error) {
        console.error('Error fetching templates:', error);
        res.status(500).json({ error: 'Failed to fetch templates' });
    }
});

// Get recovery stats summary (enhanced with funnel data)
app.get('/api/admin/recovery/stats', authenticateToken, async (req, res) => {
    try {
        // Get recovery rate (last 30 days)
        const recoveryStats = await pool.query(`
            SELECT 
                COUNT(*) FILTER (WHERE status = 'converted') as converted,
                COUNT(*) as total
            FROM recovery_contacts
            WHERE created_at >= NOW() - INTERVAL '30 days'
        `);
        
        const converted = parseInt(recoveryStats.rows[0]?.converted || 0);
        const total = parseInt(recoveryStats.rows[0]?.total || 0);
        const recoveryRate = total > 0 ? Math.round((converted / total) * 100) : 0;
        
        // Get best hour for contact
        const bestHour = await pool.query(`
            SELECT EXTRACT(HOUR FROM created_at) as hour, COUNT(*) as count
            FROM recovery_contacts
            WHERE status = 'converted'
            GROUP BY hour
            ORDER BY count DESC
            LIMIT 1
        `);
        
        const bestContactHour = bestHour.rows[0]?.hour ? `${bestHour.rows[0].hour}:00` : '10:00';
        
        // Get funnel progress stats
        let funnelStats = { active: 0, completed: 0, converted: 0 };
        try {
            const funnelProgress = await pool.query(`
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'active') as active,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed_funnels,
                    COUNT(*) FILTER (WHERE status = 'converted') as converted_funnels
                FROM recovery_lead_progress
            `);
            funnelStats = {
                active: parseInt(funnelProgress.rows[0]?.active || 0),
                completed: parseInt(funnelProgress.rows[0]?.completed_funnels || 0),
                converted: parseInt(funnelProgress.rows[0]?.converted_funnels || 0)
            };
        } catch(e) { /* table may not exist yet */ }
        
        // Get contacts by segment
        let segmentStats = {};
        try {
            const bySegment = await pool.query(`
                SELECT segment, 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'converted') as converted
                FROM recovery_contacts
                WHERE created_at >= NOW() - INTERVAL '30 days'
                GROUP BY segment
            `);
            bySegment.rows.forEach(row => {
                segmentStats[row.segment] = {
                    total: parseInt(row.total),
                    converted: parseInt(row.converted),
                    rate: parseInt(row.total) > 0 ? Math.round((parseInt(row.converted) / parseInt(row.total)) * 100) : 0
                };
            });
        } catch(e) { /* ignore */ }
        
        res.json({
            recovery_rate: recoveryRate,
            total_contacts: total,
            total_converted: converted,
            best_contact_hour: bestContactHour,
            funnel_stats: funnelStats,
            segment_stats: segmentStats
        });
        
    } catch (error) {
        console.error('Error fetching recovery stats:', error);
        res.status(500).json({ error: 'Failed to fetch recovery stats' });
    }
});

// ==================== REFUND REQUESTS API ====================

// Get all refund requests (protected) - now includes consolidated view
app.get('/api/admin/refunds', authenticateToken, async (req, res) => {
    try {
        const { status, source, type, language, startDate, endDate } = req.query;
        
        // Build dynamic query
        let conditions = [];
        let params = [];
        let paramIndex = 1;
        
        if (status) {
            conditions.push(`status = $${paramIndex++}`);
            params.push(status);
        }
        if (source) {
            conditions.push(`source = $${paramIndex++}`);
            params.push(source);
        }
        if (type) {
            conditions.push(`refund_type = $${paramIndex++}`);
            params.push(type);
        }
        if (language) {
            conditions.push(`funnel_language = $${paramIndex++}`);
            params.push(language);
        }
        if (startDate && endDate) {
            conditions.push(`(created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${paramIndex++}::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${paramIndex++}::date`);
            params.push(startDate, endDate);
        }
        
        const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
        
        // Use DISTINCT ON (email, source) to deduplicate - keep most recent entry per customer per source
        // This prevents showing the same customer multiple times in recovery/monetizze/chargeback tabs
        const result = await pool.query(`
            SELECT DISTINCT ON (LOWER(email), COALESCE(source, 'form')) *
            FROM refund_requests 
            ${whereClause}
            ORDER BY LOWER(email), COALESCE(source, 'form'), created_at DESC
        `, params);
        
        // Get stats by source (with date filter if provided)
        let statsConditions = [];
        let statsParams = [];
        let statsParamIndex = 1;
        
        if (startDate && endDate) {
            statsConditions.push(`(created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${statsParamIndex++}::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${statsParamIndex++}::date`);
            statsParams.push(startDate, endDate);
        }
        
        const statsWhereClause = statsConditions.length > 0 ? `WHERE ${statsConditions.join(' AND ')}` : '';
        
        const statsResult = await pool.query(`
            SELECT 
                COALESCE(source, 'form') as source,
                COALESCE(refund_type, 'refund') as refund_type,
                COALESCE(funnel_language, 'unknown') as funnel_language,
                COUNT(*) as count,
                status
            FROM refund_requests
            ${statsWhereClause}
            GROUP BY source, refund_type, funnel_language, status
        `, statsParams);
        
        // Calculate totals with language breakdown - includes new workflow statuses
        const stats = {
            form: { pending: 0, handling: 0, processing: 0, convinced: 0, refunded: 0, approved: 0, rejected: 0, total: 0, en: 0, es: 0 },
            monetizze_refund: { pending: 0, handling: 0, processing: 0, convinced: 0, refunded: 0, approved: 0, rejected: 0, total: 0, en: 0, es: 0 },
            monetizze_chargeback: { pending: 0, handling: 0, processing: 0, convinced: 0, refunded: 0, approved: 0, rejected: 0, total: 0, en: 0, es: 0 }
        };
        
        statsResult.rows.forEach(row => {
            const key = row.source === 'monetizze' 
                ? `monetizze_${row.refund_type || 'refund'}`
                : 'form';
            
            if (stats[key]) {
                stats[key][row.status] = (stats[key][row.status] || 0) + parseInt(row.count);
                stats[key].total += parseInt(row.count);
                
                // Count by language
                if (row.funnel_language === 'en' || row.funnel_language === 'es') {
                    stats[key][row.funnel_language] = (stats[key][row.funnel_language] || 0) + parseInt(row.count);
                }
            }
        });

        res.json({ 
            refunds: result.rows, 
            total: result.rows.length,
            stats
        });

    } catch (error) {
        console.error('Error fetching refunds:', error);
        res.status(500).json({ error: 'Failed to fetch refund requests' });
    }
});

// Get enriched refund details with cross-referenced lead/transaction data (protected)
app.get('/api/admin/refunds/:id/details', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        // Get the refund
        const refundResult = await pool.query('SELECT * FROM refund_requests WHERE id = $1', [id]);
        if (refundResult.rows.length === 0) {
            return res.status(404).json({ error: 'Refund not found' });
        }
        
        const refund = refundResult.rows[0];
        const email = refund.email;
        
        // Cross-reference with lead
        let leadData = null;
        if (email) {
            const leadResult = await pool.query(`
                SELECT id, email, name, whatsapp as phone, country, status, source,
                    products_purchased, total_spent, first_purchase_at, last_purchase_at,
                    funnel_language, created_at
                FROM leads 
                WHERE LOWER(email) = LOWER($1)
                ORDER BY created_at DESC LIMIT 1
            `, [email]);
            
            if (leadResult.rows.length > 0) {
                leadData = leadResult.rows[0];
            }
        }
        
        // Cross-reference with transactions
        let transactions = [];
        if (email) {
            const txResult = await pool.query(`
                SELECT transaction_id, product, value, status, monetizze_status, 
                    funnel_language, created_at
                FROM transactions 
                WHERE LOWER(email) = LOWER($1)
                ORDER BY created_at DESC
            `, [email]);
            transactions = txResult.rows;
        }
        
        // Cross-reference with funnel events
        let funnelEvents = [];
        if (email) {
            const eventsResult = await pool.query(`
                SELECT event_type, page, metadata, created_at
                FROM funnel_events 
                WHERE LOWER(metadata->>'email') = LOWER($1)
                ORDER BY created_at ASC
            `, [email]);
            funnelEvents = eventsResult.rows;
        }
        
        res.json({
            success: true,
            refund,
            crossReference: {
                lead: leadData,
                transactions,
                funnelEvents,
                summary: {
                    totalTransactions: transactions.length,
                    approvedTransactions: transactions.filter(t => t.status === 'approved').length,
                    totalSpent: transactions.filter(t => t.status === 'approved').reduce((sum, t) => sum + parseFloat(t.value || 0), 0),
                    productsBought: [...new Set(transactions.filter(t => t.status === 'approved').map(t => t.product))],
                    funnelSteps: funnelEvents.length,
                    detectedLanguage: transactions[0]?.funnel_language || leadData?.funnel_language || null
                }
            }
        });
        
    } catch (error) {
        console.error('Error fetching refund details:', error);
        res.status(500).json({ error: 'Failed to fetch refund details' });
    }
});

// Update refund status (protected) - Enhanced with workflow status
app.put('/api/admin/refunds/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { status, notes } = req.body;
        
        // Validate status - now includes 'convinced' for recovery workflow
        const validStatuses = ['pending', 'handling', 'processing', 'convinced', 'refunded', 'approved', 'rejected'];
        if (status && !validStatuses.includes(status)) {
            return res.status(400).json({ error: 'Invalid status' });
        }

        const result = await pool.query(`
            UPDATE refund_requests 
            SET status = $1, admin_notes = $2, updated_at = NOW()
            WHERE id = $3
            RETURNING *
        `, [status, notes, id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Refund request not found' });
        }
        
        console.log(`📝 Refund ${id} status updated to: ${status}`);

        res.json({ success: true, refund: result.rows[0] });

    } catch (error) {
        console.error('Error updating refund:', error);
        res.status(500).json({ error: 'Failed to update refund request' });
    }
});

// Send refund communication via Z-API and log it
app.post('/api/admin/refunds/:id/send-message', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { channel, template_key, message, phone, email } = req.body;
        
        if (!message) return res.status(400).json({ error: 'Mensagem é obrigatória' });
        
        const refundResult = await pool.query('SELECT * FROM refund_requests WHERE id = $1', [id]);
        if (refundResult.rows.length === 0) return res.status(404).json({ error: 'Reembolso não encontrado' });
        const refund = refundResult.rows[0];
        
        let sent = false;
        let messageId = null;
        let sendError = null;
        
        if (channel === 'whatsapp') {
            const cleanPhone = (phone || refund.phone || '').replace(/\D/g, '');
            if (!cleanPhone || cleanPhone.length < 10) {
                return res.status(400).json({ error: 'Número de telefone inválido' });
            }
            
            const zapiHeaders = { 'Content-Type': 'application/json' };
            if (ZAPI_CLIENT_TOKEN) zapiHeaders['Client-Token'] = ZAPI_CLIENT_TOKEN;
            
            try {
                const zapiResponse = await fetch(`${ZAPI_BASE_URL}/send-text`, {
                    method: 'POST',
                    headers: zapiHeaders,
                    body: JSON.stringify({ phone: cleanPhone, message, delayMessage: 3 })
                });
                const zapiData = await zapiResponse.json();
                sent = zapiResponse.ok && !!zapiData.messageId;
                messageId = zapiData.messageId;
                if (!sent) sendError = zapiData.error || zapiData.message || 'Falha Z-API';
                
                if (sent) {
                    try {
                        await pool.query(`
                            INSERT INTO whatsapp_messages (phone, message, message_id, zaap_id, status, sent_by, created_at)
                            VALUES ($1, $2, $3, $4, 'sent', 'refund_comm', NOW())
                        `, [cleanPhone, message, zapiData.messageId, zapiData.zaapId]);
                    } catch (dbErr) { /* ignore */ }
                }
            } catch (fetchErr) {
                sendError = fetchErr.message;
            }
        } else {
            sent = true;
        }
        
        // Log as note
        let notes = [];
        if (refund.notes && typeof refund.notes === 'object') {
            notes = Array.isArray(refund.notes) ? refund.notes : [];
        }
        notes.push({
            id: Date.now(),
            date: new Date().toISOString(),
            action: channel === 'whatsapp' ? 'whatsapp' : 'email',
            note: `[${template_key || 'custom'}] ${message.substring(0, 100)}...`,
            user: 'Admin',
            sent: sent,
            messageId
        });
        
        await pool.query(`UPDATE refund_requests SET notes = $1, updated_at = NOW() WHERE id = $2`, [JSON.stringify(notes), id]);
        
        // Auto-update status to 'handling' if pending
        if (refund.status === 'pending') {
            await pool.query(`UPDATE refund_requests SET status = 'handling' WHERE id = $1`, [id]);
        }
        
        if (sent) {
            console.log(`📨 Refund ${id} - ${channel} sent (${template_key})`);
            res.json({ success: true, messageId, channel });
        } else {
            console.error(`❌ Refund ${id} - ${channel} failed:`, sendError);
            res.status(500).json({ error: sendError || 'Falha no envio' });
        }
    } catch (error) {
        console.error('Error sending refund message:', error);
        res.status(500).json({ error: 'Falha ao enviar: ' + error.message });
    }
});

// Get refund communication history
app.get('/api/admin/refunds/:id/history', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const result = await pool.query('SELECT notes, full_name, email, phone, protocol, status FROM refund_requests WHERE id = $1', [id]);
        if (result.rows.length === 0) return res.status(404).json({ error: 'Reembolso não encontrado' });
        
        const refund = result.rows[0];
        let notes = [];
        if (refund.notes && typeof refund.notes === 'object') {
            notes = Array.isArray(refund.notes) ? refund.notes : [];
        }
        
        const communications = notes.filter(n => ['whatsapp', 'email', 'call'].includes(n.action));
        res.json({ success: true, communications, total: communications.length, refund: { name: refund.full_name, email: refund.email, phone: refund.phone, protocol: refund.protocol, status: refund.status } });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Add note to refund request (protected) - for recovery workflow
app.post('/api/admin/refunds/:id/notes', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { action, note } = req.body;
        
        if (!action || !note) {
            return res.status(400).json({ error: 'Action and note are required' });
        }
        
        // Get current refund
        const refundResult = await pool.query('SELECT * FROM refund_requests WHERE id = $1', [id]);
        if (refundResult.rows.length === 0) {
            return res.status(404).json({ error: 'Refund not found' });
        }
        
        const refund = refundResult.rows[0];
        
        // Get existing notes or initialize empty array
        let notes = [];
        if (refund.notes && typeof refund.notes === 'object') {
            notes = Array.isArray(refund.notes) ? refund.notes : [];
        }
        
        // Add new note
        const newNote = {
            id: Date.now(),
            date: new Date().toISOString(),
            action: action,
            note: note,
            user: 'Admin'
        };
        notes.push(newNote);
        
        // Update refund with new notes
        await pool.query(`
            UPDATE refund_requests 
            SET notes = $1, updated_at = NOW()
            WHERE id = $2
        `, [JSON.stringify(notes), id]);
        
        console.log(`📝 Note added to refund ${id}: ${action} - ${note.substring(0, 50)}...`);
        
        res.json({ success: true, note: newNote, allNotes: notes });
        
    } catch (error) {
        console.error('Error adding note:', error);
        res.status(500).json({ error: 'Failed to add note' });
    }
});

// Get notes for a refund (protected)
app.get('/api/admin/refunds/:id/notes', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        const result = await pool.query('SELECT notes FROM refund_requests WHERE id = $1', [id]);
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Refund not found' });
        }
        
        let notes = [];
        if (result.rows[0].notes && typeof result.rows[0].notes === 'object') {
            notes = Array.isArray(result.rows[0].notes) ? result.rows[0].notes : [];
        }
        
        res.json({ success: true, notes });
        
    } catch (error) {
        console.error('Error fetching notes:', error);
        res.status(500).json({ error: 'Failed to fetch notes' });
    }
});

// Get transactions (protected) - with pagination
app.get('/api/admin/transactions', authenticateToken, async (req, res) => {
    try {
        const { language, startDate, endDate, source, search, page = 1, limit = 10 } = req.query;
        
        const pageNum = parseInt(page) || 1;
        const limitNum = parseInt(limit) || 10;
        const offset = (pageNum - 1) * limitNum;
        
        let baseQuery = `FROM transactions WHERE 1=1`;
        let params = [];
        let paramIndex = 1;
        
        // Exclude test transactions
        baseQuery += ` AND transaction_id NOT LIKE 'TEST%' AND transaction_id NOT LIKE '%TEST%'`;
        baseQuery += ` AND email NOT LIKE '%test%@%' AND email NOT LIKE '%@test.%'`;
        
        // Search by email/name/transaction_id
        if (search) {
            baseQuery += ` AND (email ILIKE $${paramIndex} OR name ILIKE $${paramIndex} OR transaction_id ILIKE $${paramIndex})`;
            params.push(`%${search}%`);
            paramIndex++;
        }
        
        if (language === 'en' || language === 'es') {
            baseQuery += ` AND (funnel_language = $${paramIndex} OR (funnel_language IS NULL AND $${paramIndex} = 'en'))`;
            params.push(language);
            paramIndex++;
        }
        
        // Filter by funnel source (main/affiliate/perfectpay)
        if (source === 'main' || source === 'affiliate' || source === 'perfectpay') {
            baseQuery += ` AND (funnel_source = $${paramIndex} OR (funnel_source IS NULL AND $${paramIndex} = 'main'))`;
            params.push(source);
            paramIndex++;
        }
        
        if (startDate && endDate) {
            baseQuery += ` AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${paramIndex}::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${paramIndex + 1}::date`;
            params.push(startDate, endDate);
            paramIndex += 2;
        }
        
        // Get total count for pagination
        const countResult = await pool.query(`SELECT COUNT(*) ${baseQuery}`, params);
        const total = parseInt(countResult.rows[0].count);
        const totalPages = Math.ceil(total / limitNum);
        
        // Get paginated results
        const dataQuery = `SELECT * ${baseQuery} ORDER BY created_at DESC LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
        params.push(limitNum, offset);
        
        const result = await pool.query(dataQuery, params);
        
        res.json({ 
            transactions: result.rows, 
            language: language || 'all', 
            source: source || 'all',
            pagination: {
                page: pageNum,
                limit: limitNum,
                total,
                totalPages
            }
        });
        
    } catch (error) {
        console.error('Error fetching transactions:', error);
        res.status(500).json({ error: 'Failed to fetch transactions' });
    }
});

// Delete transaction (protected)
app.delete('/api/admin/transactions/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        const result = await pool.query(
            'DELETE FROM transactions WHERE id = $1 RETURNING *',
            [id]
        );
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Transaction not found' });
        }
        
        console.log(`Transaction deleted: ${id}`);
        res.json({ success: true, message: 'Transaction deleted' });
        
    } catch (error) {
        console.error('Error deleting transaction:', error);
        res.status(500).json({ error: 'Failed to delete transaction' });
    }
});

// Get sales stats (protected)
app.get('/api/admin/sales', authenticateToken, async (req, res) => {
    try {
        const { language, startDate, endDate, source } = req.query;
        
        console.log('[Sales API] Params:', { language, startDate, endDate, source });
        
        // Build language filter
        let langCondition = '';
        let langParams = [];
        if (language === 'en' || language === 'es') {
            langCondition = `AND (funnel_language = $1 OR (funnel_language IS NULL AND $1 = 'en'))`;
            langParams = [language];
        }
        
        // Build source filter (main/affiliate/perfectpay)
        let sourceCondition = '';
        if (source === 'main' || source === 'affiliate' || source === 'perfectpay') {
            const sourceIdx = langParams.length + 1;
            sourceCondition = ` AND (funnel_source = $${sourceIdx} OR (funnel_source IS NULL AND $${sourceIdx} = 'main'))`;
            langParams.push(source);
        }
        
        // Build date filter - Use Brazil timezone (UTC-3) for date comparisons
        // This ensures dates match Monetizze's Brazil-based timestamps
        let dateCondition = '';
        if (startDate && endDate) {
            const startIdx = langParams.length + 1;
            const endIdx = langParams.length + 2;
            dateCondition = ` AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${startIdx}::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${endIdx}::date`;
            langParams.push(startDate, endDate);
        }
        
        const [totalResult, approvedResult, refundedResult, revenueResult, cancelledResult, lostRevenueResult, upsellRevenueResult, totalAttemptsResult, approvedAttemptsResult] = await Promise.all([
            // Count ALL transactions (each transaction = 1 sale attempt, even if same buyer)
            pool.query(`SELECT COUNT(*) FROM transactions WHERE 1=1 ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            // Count ALL approved transactions (each approved tx = 1 sale, even if same buyer bought multiple products)
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            // Count actual refunded/chargeback transactions (not unique customers)
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status IN ('refunded', 'chargeback') ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            // Cancelled/rejected - count unique CUSTOMERS (by email) who have cancelled transactions
            // but NEVER had an approved transaction - this matches Monetizze's "lost sales" count
            pool.query(`
                SELECT COUNT(DISTINCT email) 
                FROM transactions t 
                WHERE t.status = 'cancelled'
                ${langCondition}${sourceCondition}${dateCondition}
                AND NOT EXISTS (
                    SELECT 1 FROM transactions t2 
                    WHERE t2.email = t.email 
                    AND t2.status = 'approved'
                )
            `, langParams),
            // Lost revenue - sum the HIGHEST value attempt per customer (not all attempts)
            pool.query(`
                SELECT COALESCE(SUM(max_value), 0) as total
                FROM (
                    SELECT email, MAX(CAST(value AS DECIMAL)) as max_value
                    FROM transactions t
                    WHERE t.status = 'cancelled'
                    ${langCondition}${sourceCondition}${dateCondition}
                    AND NOT EXISTS (
                        SELECT 1 FROM transactions t2 
                        WHERE t2.email = t.email 
                        AND t2.status = 'approved'
                    )
                    GROUP BY email
                ) unique_customers
            `, langParams),
            // Upsell revenue (for average upsell ticket)
            pool.query(`SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved' AND (product ILIKE '%Message Vault%' OR product ILIKE '%Vault%' OR product ILIKE '%360%' OR product ILIKE '%Tracker%' OR product ILIKE '%Instant%' OR product ILIKE '%Recuperación%' OR product ILIKE '%Visión%' OR product ILIKE '%VIP%') ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            // Count unique CUSTOMERS who attempted payment (for approval rate calculation)
            // This counts people, not payment attempts
            pool.query(`SELECT COUNT(DISTINCT email) FROM transactions WHERE 1=1 ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            // Count unique CUSTOMERS with approved payment
            pool.query(`SELECT COUNT(DISTINCT email) FROM transactions WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}`, langParams)
        ]);
        
        // Get today and this week (using Brazil timezone) - count ALL approved transactions (not unique customers)
        const [todayResult, weekResult] = await Promise.all([
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '7 days')::date ${langCondition}${sourceCondition}${dateCondition}`, langParams)
        ]);
        
        // Calculate checkout abandonment (clicked checkout but no approved transaction)
        // Build language condition for funnel_events
        let funnelLangCondition = '';
        if (language === 'en' || language === 'es') {
            funnelLangCondition = ` AND (metadata->>'funnelLanguage' = '${language}' OR (metadata->>'funnelLanguage' IS NULL AND '${language}' = 'en'))`;
        }
        
        // Build source condition for funnel_events
        let funnelSourceCondition = '';
        if (source === 'main' || source === 'affiliate' || source === 'perfectpay') {
            funnelSourceCondition = ` AND (metadata->>'funnelSource' = '${source}' OR (metadata->>'funnelSource' IS NULL AND '${source}' = 'main'))`;
        }
        
        // Build date condition for funnel_events (using Brazil timezone)
        let funnelDateCondition = '';
        if (startDate && endDate) {
            funnelDateCondition = ` AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= '${startDate}'::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= '${endDate}'::date`;
        }
        
        // Count unique visitors who clicked checkout
        const checkoutClickedResult = await pool.query(`
            SELECT COUNT(DISTINCT visitor_id) as count 
            FROM funnel_events 
            WHERE event = 'checkout_clicked'${funnelLangCondition}${funnelSourceCondition}${funnelDateCondition}
        `);
        
        // Count unique customers who attempted payment (any status)
        // This is more reliable than trying to match visitor_id to leads to transactions
        const paymentAttemptsResult = await pool.query(`
            SELECT COUNT(DISTINCT LOWER(email)) as count 
            FROM transactions 
            WHERE 1=1 ${langCondition}${sourceCondition}${dateCondition}
        `, langParams);
        
        const paymentAttempts = parseInt(paymentAttemptsResult.rows[0].count) || 0;
        
        // Checkout abandoned = people who clicked checkout but never attempted payment
        // This is calculated as: checkoutClicked - paymentAttempts
        // Note: This is an approximation since we can't directly link visitor_id to transactions
        
        // Count unique emails with approved transactions (for other metrics)
        const approvedEmailsResult = await pool.query(`
            SELECT COUNT(DISTINCT LOWER(email)) as count 
            FROM transactions 
            WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}
        `, langParams);
        
        const checkoutClicked = parseInt(checkoutClickedResult.rows[0].count) || 0;
        // Checkout abandoned = clicked checkout minus those who attempted payment
        // If more people attempted payment than clicked checkout (direct link access), set to 0
        const checkoutAbandoned = Math.max(0, checkoutClicked - paymentAttempts);
        const approvedEmails = parseInt(approvedEmailsResult.rows[0].count) || 0;
        
        // Calculate conversion rate (leads -> sales) - filtered by language, source AND date
        // Build leads filter conditions
        let leadsLangCondition = '';
        let leadsSourceCondition = '';
        let leadsDateCondition = '';
        let leadsParams = [];
        
        if (language) {
            leadsLangCondition = ` AND (funnel_language = $${leadsParams.length + 1} OR (funnel_language IS NULL AND $${leadsParams.length + 1} = 'en'))`;
            leadsParams.push(language);
        }
        if (source === 'main' || source === 'affiliate' || source === 'perfectpay') {
            leadsSourceCondition = ` AND (funnel_source = $${leadsParams.length + 1} OR (funnel_source IS NULL AND $${leadsParams.length + 1} = 'main'))`;
            leadsParams.push(source);
        }
        if (startDate && endDate) {
            const startIdx = leadsParams.length + 1;
            const endIdx = leadsParams.length + 2;
            leadsDateCondition = ` AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${startIdx}::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${endIdx}::date`;
            leadsParams.push(startDate, endDate);
        }
        
        const leadsCount = await pool.query(`SELECT COUNT(*) FROM leads WHERE 1=1 ${leadsLangCondition}${leadsSourceCondition}${leadsDateCondition}`, leadsParams);
        const conversionRate = parseInt(leadsCount.rows[0].count) > 0 
            ? ((parseInt(approvedResult.rows[0].count) / parseInt(leadsCount.rows[0].count)) * 100).toFixed(2)
            : 0;
        
        // Get stats by product (filtered)
        const productStats = await pool.query(`
            SELECT 
                product,
                COUNT(*) FILTER (WHERE status = 'approved') as approved,
                COUNT(*) FILTER (WHERE status IN ('refunded', 'chargeback')) as refunded,
                COALESCE(SUM(CAST(value AS DECIMAL)) FILTER (WHERE status = 'approved'), 0) as revenue,
                COUNT(*) as total
            FROM transactions
            WHERE product IS NOT NULL ${langCondition}${sourceCondition}${dateCondition}
            GROUP BY product
            ORDER BY approved DESC
        `, langParams);
        
        // Calculate upsell take rates based on language
        // English products (Main: 341972, 349241-349243 + Affiliates: 330254, 341443-341448)
        const enFrontKeywords = "product ILIKE '%Monitor%' OR product ILIKE '%ZappDetect%' OR product ILIKE '%341972%' OR product ILIKE '%330254%'";
        const enUp1Keywords = "product ILIKE '%Message Vault%' OR product ILIKE '%349241%' OR product ILIKE '%341443%'";
        const enUp2Keywords = "product ILIKE '%360%' OR product ILIKE '%Tracker%' OR product ILIKE '%349242%' OR product ILIKE '%341444%'";
        const enUp3Keywords = "product ILIKE '%Instant Access%' OR product ILIKE '%349243%' OR product ILIKE '%341448%'";
        
        // Spanish products (Main: 349260-349267 + Affiliates: 338375, 341452-341454)
        const esFrontKeywords = "product ILIKE '%Infidelidad%' OR product ILIKE '%349260%' OR product ILIKE '%338375%'";
        const esUp1Keywords = "product ILIKE '%Recuperación%' OR product ILIKE '%349261%' OR product ILIKE '%341452%'";
        const esUp2Keywords = "product ILIKE '%Visión Total%' OR product ILIKE '%349266%' OR product ILIKE '%341453%'";
        const esUp3Keywords = "product ILIKE '%VIP Sin Esperas%' OR product ILIKE '%349267%' OR product ILIKE '%341454%'";
        
        // Select keywords based on language filter
        let frontKeywords, up1Keywords, up2Keywords, up3Keywords;
        if (language === 'es') {
            frontKeywords = esFrontKeywords;
            up1Keywords = esUp1Keywords;
            up2Keywords = esUp2Keywords;
            up3Keywords = esUp3Keywords;
        } else if (language === 'en') {
            frontKeywords = enFrontKeywords;
            up1Keywords = enUp1Keywords;
            up2Keywords = enUp2Keywords;
            up3Keywords = enUp3Keywords;
        } else {
            // All languages - combine both
            frontKeywords = `(${enFrontKeywords}) OR (${esFrontKeywords})`;
            up1Keywords = `(${enUp1Keywords}) OR (${esUp1Keywords})`;
            up2Keywords = `(${enUp2Keywords}) OR (${esUp2Keywords})`;
            up3Keywords = `(${enUp3Keywords}) OR (${esUp3Keywords})`;
        }
        
        const frontSales = await pool.query(`
            SELECT COUNT(DISTINCT email) as count 
            FROM transactions 
            WHERE status = 'approved' AND (${frontKeywords}) ${langCondition}${sourceCondition}${dateCondition}
        `, langParams);
        
        const upsell1Sales = await pool.query(`
            SELECT COUNT(DISTINCT email) as count 
            FROM transactions 
            WHERE status = 'approved' AND (${up1Keywords}) ${langCondition}${sourceCondition}${dateCondition}
        `, langParams);
        
        const upsell2Sales = await pool.query(`
            SELECT COUNT(DISTINCT email) as count 
            FROM transactions 
            WHERE status = 'approved' AND (${up2Keywords}) ${langCondition}${sourceCondition}${dateCondition}
        `, langParams);
        
        const upsell3Sales = await pool.query(`
            SELECT COUNT(DISTINCT email) as count 
            FROM transactions 
            WHERE status = 'approved' AND (${up3Keywords}) ${langCondition}${sourceCondition}${dateCondition}
        `, langParams);
        
        const frontCount = parseInt(frontSales.rows[0].count) || 0;
        const up1Count = parseInt(upsell1Sales.rows[0].count) || 0;
        const up2Count = parseInt(upsell2Sales.rows[0].count) || 0;
        const up3Count = parseInt(upsell3Sales.rows[0].count) || 0;
        
        const totalUpsellCount = up1Count + up2Count + up3Count;
        const upsellRevenue = parseFloat(upsellRevenueResult.rows[0].total) || 0;
        const avgUpsellTicket = totalUpsellCount > 0 ? upsellRevenue / totalUpsellCount : 0;
        
        res.json({
            total: parseInt(totalResult.rows[0].count),
            approved: parseInt(approvedResult.rows[0].count),
            refunded: parseInt(refundedResult.rows[0].count),
            cancelled: parseInt(cancelledResult.rows[0].count),
            lostRevenue: parseFloat(lostRevenueResult.rows[0].total) || 0,
            revenue: parseFloat(revenueResult.rows[0].total) || 0,
            upsellRevenue: upsellRevenue,
            avgUpsellTicket: avgUpsellTicket,
            checkoutAbandoned: checkoutAbandoned,
            checkoutClicked: checkoutClicked,
            today: parseInt(todayResult.rows[0].count),
            thisWeek: parseInt(weekResult.rows[0].count),
            // Real payment attempts for approval rate
            totalAttempts: parseInt(totalAttemptsResult.rows[0].count),
            approvedAttempts: parseInt(approvedAttemptsResult.rows[0].count),
            conversionRate: parseFloat(conversionRate),
            byProduct: productStats.rows,
            language: language || 'all',
            source: source || 'all',
            upsellStats: {
                front: frontCount,
                upsell1: up1Count,
                upsell2: up2Count,
                upsell3: up3Count,
                total: totalUpsellCount,
                takeRate1: frontCount > 0 ? ((up1Count / frontCount) * 100).toFixed(1) : 0,
                takeRate2: frontCount > 0 ? ((up2Count / frontCount) * 100).toFixed(1) : 0,
                takeRate3: frontCount > 0 ? ((up3Count / frontCount) * 100).toFixed(1) : 0
            }
        });
        
    } catch (error) {
        console.error('Error fetching sales stats:', error);
        res.status(500).json({ error: 'Failed to fetch sales stats' });
    }
});

// Migrate existing transactions to set funnel_source based on product codes
app.post('/api/admin/migrate-funnel-source', authenticateToken, async (req, res) => {
    try {
        // Affiliate product codes
        const affiliateCodes = ['330254', '341443', '341444', '341448', '338375', '341452', '341453', '341454'];
        
        // Update transactions that have affiliate product codes in raw_data
        let updated = 0;
        
        // Method 1: Check raw_data for product codes
        for (const code of affiliateCodes) {
            const result = await pool.query(`
                UPDATE transactions 
                SET funnel_source = 'affiliate' 
                WHERE (funnel_source IS NULL OR funnel_source = 'main')
                AND (
                    raw_data::text LIKE $1
                    OR product ILIKE $2
                )
            `, [`%"codigo":${code}%`, `%${code}%`]);
            updated += result.rowCount;
        }
        
        // Also fix any funnel_language that was set as 'en-aff' or 'es-aff'
        const fixLangAff = await pool.query(`
            UPDATE transactions 
            SET funnel_language = 'en', funnel_source = 'affiliate'
            WHERE funnel_language = 'en-aff'
        `);
        updated += fixLangAff.rowCount;
        
        const fixLangAffEs = await pool.query(`
            UPDATE transactions 
            SET funnel_language = 'es', funnel_source = 'affiliate'
            WHERE funnel_language = 'es-aff'
        `);
        updated += fixLangAffEs.rowCount;
        
        // Set remaining null funnel_source to 'main'
        const fixNull = await pool.query(`
            UPDATE transactions SET funnel_source = 'main' WHERE funnel_source IS NULL
        `);
        
        res.json({ 
            success: true, 
            message: `Migration complete`, 
            updated,
            nullFixed: fixNull.rowCount
        });
    } catch (error) {
        console.error('Error migrating funnel_source:', error);
        res.status(500).json({ error: 'Migration failed', details: error.message });
    }
});

// Migrate existing leads to set funnel_source based on related transactions
app.post('/api/admin/migrate-leads-funnel-source', authenticateToken, async (req, res) => {
    try {
        // Strategy: If a lead has a transaction from affiliate products, mark the lead as affiliate
        // This correlates leads with transactions by email
        
        // Affiliate product codes
        const affiliateCodes = ['330254', '341443', '341444', '341448', '338375', '341452', '341453', '341454'];
        
        let updated = 0;
        
        // Update leads that have transactions with affiliate product codes
        for (const code of affiliateCodes) {
            const result = await pool.query(`
                UPDATE leads 
                SET funnel_source = 'affiliate' 
                WHERE (funnel_source IS NULL OR funnel_source = 'main')
                AND LOWER(email) IN (
                    SELECT DISTINCT LOWER(email) FROM transactions 
                    WHERE product ILIKE $1
                )
            `, [`%${code}%`]);
            updated += result.rowCount;
        }
        
        // Also update leads that have transactions with funnel_source = 'affiliate'
        const fromTransactions = await pool.query(`
            UPDATE leads 
            SET funnel_source = 'affiliate' 
            WHERE (funnel_source IS NULL OR funnel_source = 'main')
            AND LOWER(email) IN (
                SELECT DISTINCT LOWER(email) FROM transactions 
                WHERE funnel_source = 'affiliate'
            )
        `);
        updated += fromTransactions.rowCount;
        
        // Set remaining null funnel_source to 'main' (these are leads that don't have affiliate transactions)
        const fixNull = await pool.query(`
            UPDATE leads SET funnel_source = 'main' WHERE funnel_source IS NULL
        `);
        
        // Count stats for response
        const stats = await pool.query(`
            SELECT funnel_source, COUNT(*) as count 
            FROM leads 
            GROUP BY funnel_source
        `);
        
        res.json({ 
            success: true, 
            message: `Leads migration complete`, 
            affiliateUpdated: updated,
            nullFixed: fixNull.rowCount,
            stats: stats.rows
        });
    } catch (error) {
        console.error('Error migrating leads funnel_source:', error);
        res.status(500).json({ error: 'Leads migration failed', details: error.message });
    }
});

// Debug endpoint to check transactions in database
app.get('/api/admin/debug-transactions', authenticateToken, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        
        // Build date filter (using Brazil timezone)
        let params = [];
        if (startDate && endDate) {
            params = [startDate, endDate];
        }
        
        // Get all approved transactions - Use Brazil timezone for date filtering
        const approved = await pool.query(`
            SELECT transaction_id, email, product, value, status, created_at,
                   (created_at AT TIME ZONE 'America/Sao_Paulo') as created_at_brazil
            FROM transactions 
            WHERE status = 'approved' ${startDate ? 'AND (created_at AT TIME ZONE \'America/Sao_Paulo\')::date >= $1::date AND (created_at AT TIME ZONE \'America/Sao_Paulo\')::date <= $2::date' : ''}
            ORDER BY created_at DESC
            LIMIT 50
        `, params);
        
        // Get sum of approved transactions - Use Brazil timezone
        const sumResult = await pool.query(`
            SELECT 
                COUNT(*) as count,
                COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total_value,
                COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
            FROM transactions 
            WHERE status = 'approved' ${startDate ? 'AND (created_at AT TIME ZONE \'America/Sao_Paulo\')::date >= $1::date AND (created_at AT TIME ZONE \'America/Sao_Paulo\')::date <= $2::date' : ''}
        `, params);
        
        // Get all unique statuses
        const statuses = await pool.query(`
            SELECT status, COUNT(*) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total
            FROM transactions
            GROUP BY status
        `);
        
        res.json({
            dateFilter: { startDate, endDate },
            summary: sumResult.rows[0],
            statusBreakdown: statuses.rows,
            recentApproved: approved.rows
        });
    } catch (error) {
        console.error('Debug transactions error:', error);
        res.status(500).json({ error: error.message });
    }
});

// DIAGNOSTIC: Complete panel health check - verifies all metrics consistency
app.get('/api/admin/diagnostic', authenticateToken, async (req, res) => {
    try {
        const today = new Date().toISOString().split('T')[0];
        const brazilNow = `(NOW() AT TIME ZONE 'America/Sao_Paulo')`;
        const brazilToday = `(${brazilNow})::date`;
        
        // 1. LEADS - Total counts
        const leadsTotal = await pool.query(`SELECT COUNT(*) as count FROM leads`);
        const leadsTodayBrazil = await pool.query(`
            SELECT COUNT(*) as count FROM leads 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        const leadsThisWeek = await pool.query(`
            SELECT COUNT(*) as count FROM leads 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= (${brazilNow} - INTERVAL '7 days')::date
        `);
        
        // 2. TRANSACTIONS - Total counts
        const txTotal = await pool.query(`SELECT COUNT(*) as count FROM transactions`);
        const txApproved = await pool.query(`SELECT COUNT(*) as count FROM transactions WHERE status = 'approved'`);
        const txTodayBrazil = await pool.query(`
            SELECT COUNT(*) as count FROM transactions 
            WHERE status = 'approved' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        const txRevenue = await pool.query(`
            SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved'
        `);
        const txRevenueTodayBrazil = await pool.query(`
            SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions 
            WHERE status = 'approved' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        
        // 3. TRANSACTIONS by status
        const txByStatus = await pool.query(`
            SELECT status, COUNT(*) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as value
            FROM transactions GROUP BY status ORDER BY count DESC
        `);
        
        // 4. FUNNEL EVENTS - Total counts
        const funnelTotal = await pool.query(`SELECT COUNT(*) as count FROM funnel_events`);
        const funnelTodayBrazil = await pool.query(`
            SELECT COUNT(*) as count FROM funnel_events 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        const funnelByEvent = await pool.query(`
            SELECT event, COUNT(*) as count FROM funnel_events 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
            GROUP BY event ORDER BY count DESC
        `);
        const funnelLandingToday = await pool.query(`
            SELECT COUNT(DISTINCT visitor_id) as count FROM funnel_events 
            WHERE event = 'page_view_landing' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        
        // 5. REFUNDS
        const refundsTotal = await pool.query(`SELECT COUNT(*) as count FROM refund_requests`);
        const refundsTodayBrazil = await pool.query(`
            SELECT COUNT(*) as count FROM refund_requests 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        
        // 6. Check server timezone
        const serverTime = await pool.query(`SELECT NOW() as utc, NOW() AT TIME ZONE 'America/Sao_Paulo' as brazil`);
        
        // 7. Recent transactions (last 5) to verify dates
        const recentTx = await pool.query(`
            SELECT transaction_id, email, status, value, 
                   created_at as utc_time,
                   (created_at AT TIME ZONE 'America/Sao_Paulo') as brazil_time,
                   (created_at AT TIME ZONE 'America/Sao_Paulo')::date as brazil_date
            FROM transactions ORDER BY created_at DESC LIMIT 5
        `);
        
        // 8. Recent leads (last 5) to verify dates
        const recentLeads = await pool.query(`
            SELECT id, email, 
                   created_at as utc_time,
                   (created_at AT TIME ZONE 'America/Sao_Paulo') as brazil_time,
                   (created_at AT TIME ZONE 'America/Sao_Paulo')::date as brazil_date
            FROM leads ORDER BY created_at DESC LIMIT 5
        `);
        
        res.json({
            serverInfo: {
                utcTime: serverTime.rows[0].utc,
                brazilTime: serverTime.rows[0].brazil,
                todayBrazil: today
            },
            leads: {
                total: parseInt(leadsTotal.rows[0].count),
                todayBrazil: parseInt(leadsTodayBrazil.rows[0].count),
                thisWeek: parseInt(leadsThisWeek.rows[0].count),
                recent: recentLeads.rows
            },
            transactions: {
                total: parseInt(txTotal.rows[0].count),
                approved: parseInt(txApproved.rows[0].count),
                todayApproved: parseInt(txTodayBrazil.rows[0].count),
                totalRevenue: parseFloat(txRevenue.rows[0].total),
                todayRevenue: parseFloat(txRevenueTodayBrazil.rows[0].total),
                byStatus: txByStatus.rows,
                recent: recentTx.rows
            },
            funnel: {
                totalEvents: parseInt(funnelTotal.rows[0].count),
                todayEvents: parseInt(funnelTodayBrazil.rows[0].count),
                todayLandingVisitors: parseInt(funnelLandingToday.rows[0].count),
                todayByEvent: funnelByEvent.rows
            },
            refunds: {
                total: parseInt(refundsTotal.rows[0].count),
                today: parseInt(refundsTodayBrazil.rows[0].count)
            },
            consistency: {
                message: 'Compare these numbers with what the panel shows. They should match.',
                tips: [
                    'If leads.todayBrazil != panel leads today, timezone issue remains',
                    'If transactions.todayApproved != panel sales today, timezone issue remains',
                    'If funnel.todayLandingVisitors is way higher than leads, check for old/test data'
                ]
            }
        });
    } catch (error) {
        console.error('Diagnostic error:', error);
        res.status(500).json({ error: error.message });
    }
});

// DATA CLEANUP: Find and fix corrupted data in the database
app.get('/api/admin/diagnostic/corrupted', authenticateToken, async (req, res) => {
    try {
        const issues = [];
        
        // 1. Find transactions with future dates (date > today + 1 day buffer)
        const futureTx = await pool.query(`
            SELECT transaction_id, email, status, value, created_at,
                   (created_at AT TIME ZONE 'America/Sao_Paulo') as brazil_time
            FROM transactions 
            WHERE created_at > NOW() + INTERVAL '1 day'
            ORDER BY created_at DESC
        `);
        
        if (futureTx.rows.length > 0) {
            issues.push({
                type: 'transactions_future_dates',
                severity: 'high',
                count: futureTx.rows.length,
                description: 'Transações com datas no futuro (corrompidas)',
                data: futureTx.rows,
                fix_action: 'delete_or_fix_date'
            });
        }
        
        // 2. Find transactions with invalid IDs (not numeric Monetizze IDs)
        const invalidIdTx = await pool.query(`
            SELECT transaction_id, email, status, value, created_at
            FROM transactions 
            WHERE transaction_id !~ '^[0-9]+$' 
            OR LENGTH(transaction_id) > 20
            OR value IS NULL 
            OR value = '0'
        `);
        
        if (invalidIdTx.rows.length > 0) {
            issues.push({
                type: 'transactions_invalid_id_or_value',
                severity: 'medium',
                count: invalidIdTx.rows.length,
                description: 'Transações com ID inválido ou valor nulo/zero',
                data: invalidIdTx.rows,
                fix_action: 'review_and_delete'
            });
        }
        
        // 3. Find duplicate transactions (same transaction_id)
        const duplicateTx = await pool.query(`
            SELECT transaction_id, COUNT(*) as count
            FROM transactions 
            GROUP BY transaction_id 
            HAVING COUNT(*) > 1
        `);
        
        if (duplicateTx.rows.length > 0) {
            issues.push({
                type: 'transactions_duplicates',
                severity: 'medium',
                count: duplicateTx.rows.length,
                description: 'Transaction IDs duplicados',
                data: duplicateTx.rows,
                fix_action: 'remove_duplicates'
            });
        }
        
        // 4. Find leads with invalid emails
        const invalidLeads = await pool.query(`
            SELECT id, email, created_at
            FROM leads 
            WHERE email IS NULL 
            OR email = ''
            OR email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            LIMIT 20
        `);
        
        if (invalidLeads.rows.length > 0) {
            issues.push({
                type: 'leads_invalid_email',
                severity: 'low',
                count: invalidLeads.rows.length,
                description: 'Leads com email inválido ou vazio',
                data: invalidLeads.rows,
                fix_action: 'review'
            });
        }
        
        // 5. Find very old funnel events that might be test data (before project start)
        const oldEvents = await pool.query(`
            SELECT COUNT(*) as count, MIN(created_at) as oldest
            FROM funnel_events 
            WHERE created_at < '2025-01-01'
        `);
        
        if (parseInt(oldEvents.rows[0].count) > 0) {
            issues.push({
                type: 'funnel_very_old_events',
                severity: 'low',
                count: parseInt(oldEvents.rows[0].count),
                description: 'Eventos do funil muito antigos (antes de 2025)',
                data: { oldest: oldEvents.rows[0].oldest },
                fix_action: 'review_and_delete'
            });
        }
        
        res.json({
            summary: {
                totalIssues: issues.length,
                highSeverity: issues.filter(i => i.severity === 'high').length,
                mediumSeverity: issues.filter(i => i.severity === 'medium').length,
                lowSeverity: issues.filter(i => i.severity === 'low').length
            },
            issues,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('Corrupted data check error:', error);
        res.status(500).json({ error: error.message });
    }
});

// DATA CLEANUP: Delete corrupted transactions
app.post('/api/admin/diagnostic/fix-corrupted', authenticateToken, async (req, res) => {
    try {
        const { action, transactionIds } = req.body;
        let result = { deleted: 0, fixed: 0, details: [] };
        
        if (action === 'delete_future_dates') {
            // Delete transactions with dates in the future
            const deleted = await pool.query(`
                DELETE FROM transactions 
                WHERE created_at > NOW() + INTERVAL '1 day'
                RETURNING transaction_id, email
            `);
            result.deleted = deleted.rowCount;
            result.details = deleted.rows;
        }
        
        if (action === 'delete_invalid') {
            // Delete transactions with invalid IDs or zero value
            const deleted = await pool.query(`
                DELETE FROM transactions 
                WHERE (transaction_id !~ '^[0-9]+$' OR LENGTH(transaction_id) > 20)
                AND status != 'approved'
                RETURNING transaction_id, email
            `);
            result.deleted = deleted.rowCount;
            result.details = deleted.rows;
        }
        
        if (action === 'delete_specific' && transactionIds && transactionIds.length > 0) {
            // Delete specific transaction IDs
            const deleted = await pool.query(`
                DELETE FROM transactions 
                WHERE transaction_id = ANY($1)
                RETURNING transaction_id, email
            `, [transactionIds]);
            result.deleted = deleted.rowCount;
            result.details = deleted.rows;
        }
        
        if (action === 'remove_duplicates') {
            // Remove duplicate transactions, keeping the newest one
            const deleted = await pool.query(`
                DELETE FROM transactions a
                USING transactions b
                WHERE a.transaction_id = b.transaction_id 
                AND a.created_at < b.created_at
                RETURNING a.transaction_id
            `);
            result.deleted = deleted.rowCount;
        }
        
        console.log(`🧹 Cleanup action "${action}": ${result.deleted} deleted, ${result.fixed} fixed`);
        
        res.json({
            success: true,
            action,
            result,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('Fix corrupted error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Fix leads status - convert leads with approved transactions to 'converted'
app.post('/api/admin/fix-leads-status', authenticateToken, async (req, res) => {
    try {
        // Find leads that have approved transactions but are NOT marked as 'converted'
        const result = await pool.query(`
            UPDATE leads 
            SET status = 'converted',
                updated_at = NOW(),
                notes = COALESCE(notes, '') || E'\n[Auto-fix] Status corrigido para converted - ' || NOW()::text
            WHERE status != 'converted'
            AND LOWER(email) IN (
                SELECT DISTINCT LOWER(email) 
                FROM transactions 
                WHERE status = 'approved'
            )
            RETURNING id, email, status
        `);
        
        console.log(`✅ Fixed ${result.rowCount} leads with incorrect status`);
        
        // Get summary stats
        const stats = await pool.query(`
            SELECT status, COUNT(*) as count 
            FROM leads 
            GROUP BY status
            ORDER BY count DESC
        `);
        
        res.json({ 
            success: true, 
            message: `${result.rowCount} leads corrigidos para 'converted'`,
            fixedLeads: result.rows.map(l => ({ id: l.id, email: l.email })),
            currentStats: stats.rows
        });
    } catch (error) {
        console.error('Error fixing leads status:', error);
        res.status(500).json({ error: 'Failed to fix leads status', details: error.message });
    }
});

// Auto-migrate database on startup
async function initDatabase() {
    try {
        console.log('🔄 Checking database...');
        
        // Create leads table if not exists
        await pool.query(`
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255) NOT NULL,
                whatsapp VARCHAR(50) NOT NULL,
                target_phone VARCHAR(50),
                target_gender VARCHAR(20),
                status VARCHAR(50) DEFAULT 'new',
                notes TEXT,
                ip_address VARCHAR(45),
                referrer TEXT,
                user_agent TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        
        // Add name column if it doesn't exist (for existing databases)
        await pool.query(`
            ALTER TABLE leads ADD COLUMN IF NOT EXISTS name VARCHAR(255);
        `);
        
        // Add funnel_language column
        await pool.query(`
            ALTER TABLE leads ADD COLUMN IF NOT EXISTS funnel_language VARCHAR(10) DEFAULT 'en';
        `);
        
        // Add visit tracking columns
        await pool.query(`
            ALTER TABLE leads ADD COLUMN IF NOT EXISTS visit_count INTEGER DEFAULT 1;
        `);
        await pool.query(`
            ALTER TABLE leads ADD COLUMN IF NOT EXISTS last_visit_at TIMESTAMP WITH TIME ZONE;
        `);
        
        // Add geolocation columns
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS country VARCHAR(100);`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS country_code VARCHAR(10);`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS city VARCHAR(100);`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS state VARCHAR(100);`);
        
        // Add customer journey tracking columns
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS visitor_id VARCHAR(100);`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS products_purchased TEXT[];`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS total_spent DECIMAL(10,2) DEFAULT 0;`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS first_purchase_at TIMESTAMP WITH TIME ZONE;`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS last_purchase_at TIMESTAMP WITH TIME ZONE;`);
        
        // Add funnel_source column to leads (main or affiliate)
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS funnel_source VARCHAR(20) DEFAULT 'main';`);
        
        // Add UTM tracking columns to leads
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_source VARCHAR(255);`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_medium VARCHAR(255);`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_campaign VARCHAR(500);`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_content VARCHAR(500);`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_term VARCHAR(255);`);
        
        // WhatsApp verification columns
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS whatsapp_verified BOOLEAN DEFAULT NULL;`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS whatsapp_verified_at TIMESTAMP WITH TIME ZONE;`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS whatsapp_profile_pic TEXT;`);
        
        // Facebook Pixel tracking columns (for CAPI enrichment on purchase)
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS fbc VARCHAR(255);`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS fbp VARCHAR(255);`);
        
        // Create funnel_events table for tracking
        await pool.query(`
            CREATE TABLE IF NOT EXISTS funnel_events (
                id SERIAL PRIMARY KEY,
                visitor_id VARCHAR(100) NOT NULL,
                event VARCHAR(100) NOT NULL,
                page VARCHAR(100),
                target_phone VARCHAR(50),
                target_gender VARCHAR(20),
                ip_address VARCHAR(45),
                user_agent TEXT,
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        
        // Create postback_logs table for debugging
        await pool.query(`
            CREATE TABLE IF NOT EXISTS postback_logs (
                id SERIAL PRIMARY KEY,
                content_type VARCHAR(255),
                body JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        
        // Create capi_purchase_logs table for tracking Purchase event attribution
        await pool.query(`
            CREATE TABLE IF NOT EXISTS capi_purchase_logs (
                id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(255),
                email VARCHAR(255),
                product VARCHAR(500),
                value DECIMAL(10,2),
                currency VARCHAR(10) DEFAULT 'USD',
                funnel_language VARCHAR(10),
                funnel_source VARCHAR(20),
                event_source_url TEXT,
                event_id VARCHAR(255),
                pixel_id VARCHAR(50),
                pixel_name VARCHAR(255),
                has_email BOOLEAN DEFAULT FALSE,
                has_fbc BOOLEAN DEFAULT FALSE,
                has_fbp BOOLEAN DEFAULT FALSE,
                has_ip BOOLEAN DEFAULT FALSE,
                has_user_agent BOOLEAN DEFAULT FALSE,
                has_external_id BOOLEAN DEFAULT FALSE,
                has_country BOOLEAN DEFAULT FALSE,
                has_phone BOOLEAN DEFAULT FALSE,
                lead_found BOOLEAN DEFAULT FALSE,
                capi_success BOOLEAN DEFAULT FALSE,
                capi_response JSONB,
                fb_events_received INTEGER DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        // Add unique constraint on transaction_id (prevents duplicate CAPI sends)
        // IMPORTANT: Must be a NON-PARTIAL index (no WHERE clause) for ON CONFLICT (transaction_id) to work
        try {
            // First drop the old partial index if it exists (partial indexes don't work with ON CONFLICT)
            await pool.query(`DROP INDEX IF EXISTS idx_capi_purchase_logs_tx_unique;`);
            // Remove any NULL transaction_ids before creating non-partial unique index
            await pool.query(`DELETE FROM capi_purchase_logs WHERE transaction_id IS NULL;`);
            // Remove duplicates keeping the latest entry
            const delResult = await pool.query(`
                DELETE FROM capi_purchase_logs a
                USING capi_purchase_logs b
                WHERE a.id < b.id AND a.transaction_id = b.transaction_id
            `);
            if (delResult.rowCount > 0) {
                console.log(`🧹 Removed ${delResult.rowCount} duplicate capi_purchase_logs entries`);
            }
            // Create NON-PARTIAL unique index (works with ON CONFLICT)
            await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_capi_purchase_logs_tx_nonpartial ON capi_purchase_logs(transaction_id);`);
            console.log('✅ capi_purchase_logs unique index ready (non-partial)');
        } catch (indexErr) {
            console.error('⚠️ capi_purchase_logs index error:', indexErr.message);
        }
        
        // Create transactions table for Monetizze postbacks
        await pool.query(`
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(100) UNIQUE NOT NULL,
                email VARCHAR(255),
                phone VARCHAR(50),
                name VARCHAR(255),
                product VARCHAR(255),
                value VARCHAR(50),
                monetizze_status VARCHAR(10),
                status VARCHAR(50),
                raw_data JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        
        // Add fbc/fbp columns to funnel_events for CAPI attribution matching
        await pool.query(`ALTER TABLE funnel_events ADD COLUMN IF NOT EXISTS fbc VARCHAR(255);`);
        await pool.query(`ALTER TABLE funnel_events ADD COLUMN IF NOT EXISTS fbp VARCHAR(255);`);
        
        // Add fbc/fbp columns to transactions for CAPI attribution (from postback params)
        await pool.query(`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS fbc VARCHAR(500);`);
        await pool.query(`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS fbp VARCHAR(255);`);
        await pool.query(`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS visitor_id VARCHAR(255);`);
        
        // Add match_method column to capi_purchase_logs for attribution monitoring
        await pool.query(`ALTER TABLE capi_purchase_logs ADD COLUMN IF NOT EXISTS match_method VARCHAR(50);`);
        
        // Create indexes for funnel_events
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_funnel_visitor ON funnel_events(visitor_id);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_funnel_event ON funnel_events(event);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_funnel_created ON funnel_events(created_at DESC);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_funnel_ip ON funnel_events(ip_address);`);
        
        // Create indexes for leads
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_leads_created_at ON leads(created_at DESC);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);`);
        
        // Add funnel_language to transactions if not exists
        await pool.query(`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS funnel_language VARCHAR(10);`);
        
        // Add funnel_source to transactions (main vs affiliate)
        await pool.query(`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS funnel_source VARCHAR(20) DEFAULT 'main';`);
        
        // Create indexes for transactions
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_transactions_email ON transactions(email);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions(created_at DESC);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_transactions_funnel_source ON transactions(funnel_source);`);
        
        // Auto-migrate: fix funnel_language values that were set as 'en-aff' or 'es-aff'
        await pool.query(`UPDATE transactions SET funnel_language = 'en', funnel_source = 'affiliate' WHERE funnel_language = 'en-aff'`);
        await pool.query(`UPDATE transactions SET funnel_language = 'es', funnel_source = 'affiliate' WHERE funnel_language = 'es-aff'`);
        // Set null funnel_source to 'main'
        await pool.query(`UPDATE transactions SET funnel_source = 'main' WHERE funnel_source IS NULL`);
        
        // Create refund_requests table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS refund_requests (
                id SERIAL PRIMARY KEY,
                protocol VARCHAR(50) UNIQUE NOT NULL,
                full_name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                phone VARCHAR(50),
                country_code VARCHAR(10),
                purchase_date DATE,
                product VARCHAR(255),
                reason VARCHAR(100),
                details TEXT,
                status VARCHAR(50) DEFAULT 'pending',
                admin_notes TEXT,
                ip_address VARCHAR(45),
                user_agent TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        
        // Create indexes for refund_requests
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_refunds_email ON refund_requests(email);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_refunds_status ON refund_requests(status);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_refunds_protocol ON refund_requests(protocol);`);
        
        // Add source column to refund_requests if not exists
        await pool.query(`ALTER TABLE refund_requests ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'form';`);
        await pool.query(`ALTER TABLE refund_requests ADD COLUMN IF NOT EXISTS refund_type VARCHAR(50) DEFAULT 'refund';`);
        await pool.query(`ALTER TABLE refund_requests ADD COLUMN IF NOT EXISTS transaction_id VARCHAR(100);`);
        await pool.query(`ALTER TABLE refund_requests ADD COLUMN IF NOT EXISTS value DECIMAL(10,2);`);
        await pool.query(`ALTER TABLE refund_requests ADD COLUMN IF NOT EXISTS funnel_language VARCHAR(10);`);
        await pool.query(`ALTER TABLE refund_requests ADD COLUMN IF NOT EXISTS notes JSONB DEFAULT '[]';`);
        await pool.query(`ALTER TABLE refund_requests ADD COLUMN IF NOT EXISTS admin_notes TEXT;`);
        await pool.query(`ALTER TABLE refund_requests ADD COLUMN IF NOT EXISTS visitor_id VARCHAR(100);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_refunds_visitor_id ON refund_requests(visitor_id);`);
        
        // Create recovery_contacts table for tracking contact attempts
        await pool.query(`
            CREATE TABLE IF NOT EXISTS recovery_contacts (
                id SERIAL PRIMARY KEY,
                lead_email VARCHAR(255) NOT NULL,
                segment VARCHAR(50) NOT NULL,
                template_used VARCHAR(100),
                channel VARCHAR(20) DEFAULT 'whatsapp',
                message TEXT,
                status VARCHAR(20) DEFAULT 'sent',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_recovery_contacts_email ON recovery_contacts(lead_email);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_recovery_contacts_segment ON recovery_contacts(segment);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_recovery_contacts_status ON recovery_contacts(status);`);
        
        // Recovery funnel system tables
        await pool.query(`
            CREATE TABLE IF NOT EXISTS recovery_funnels (
                id SERIAL PRIMARY KEY,
                segment VARCHAR(50) NOT NULL,
                name VARCHAR(100) NOT NULL,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        
        await pool.query(`
            CREATE TABLE IF NOT EXISTS recovery_funnel_steps (
                id SERIAL PRIMARY KEY,
                funnel_id INTEGER REFERENCES recovery_funnels(id) ON DELETE CASCADE,
                step_number INTEGER NOT NULL,
                delay_hours INTEGER DEFAULT 24,
                template_en TEXT NOT NULL,
                template_es TEXT NOT NULL,
                channel VARCHAR(20) DEFAULT 'whatsapp',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        
        await pool.query(`
            CREATE TABLE IF NOT EXISTS recovery_lead_progress (
                id SERIAL PRIMARY KEY,
                lead_email VARCHAR(255) NOT NULL,
                funnel_id INTEGER REFERENCES recovery_funnels(id) ON DELETE CASCADE,
                current_step INTEGER DEFAULT 0,
                status VARCHAR(20) DEFAULT 'active',
                next_contact_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(lead_email, funnel_id)
            );
        `);
        
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_recovery_funnels_segment ON recovery_funnels(segment);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_recovery_funnel_steps_funnel ON recovery_funnel_steps(funnel_id);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_recovery_lead_progress_email ON recovery_lead_progress(lead_email);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_recovery_lead_progress_funnel ON recovery_lead_progress(funnel_id);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_recovery_lead_progress_status ON recovery_lead_progress(status);`);
        
        // Create admin_users table for multi-user access
        await pool.query(`
            CREATE TABLE IF NOT EXISTS admin_users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                name VARCHAR(255),
                role VARCHAR(50) DEFAULT 'support',
                is_active BOOLEAN DEFAULT true,
                last_login TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER
            );
        `);
        
        // Create index for admin_users
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_admin_users_email ON admin_users(email);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_admin_users_username ON admin_users(username);`);
        
        // Create A/B tests table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ab_tests (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                funnel VARCHAR(50) NOT NULL,
                variant_a_name VARCHAR(100) DEFAULT 'Control',
                variant_a_param VARCHAR(100) DEFAULT 'control',
                variant_b_name VARCHAR(100) DEFAULT 'Test',
                variant_b_param VARCHAR(100) DEFAULT 'test',
                traffic_split INTEGER DEFAULT 50,
                status VARCHAR(20) DEFAULT 'draft',
                winner VARCHAR(10),
                started_at TIMESTAMP WITH TIME ZONE,
                ended_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER
            );
        `);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_ab_tests_funnel ON ab_tests(funnel);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_ab_tests_status ON ab_tests(status);`);
        
        // Add new columns for A/B test types and configs (if not exist)
        await pool.query(`
            DO $$ 
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='ab_tests' AND column_name='test_type') THEN
                    ALTER TABLE ab_tests ADD COLUMN test_type VARCHAR(20) DEFAULT 'vsl';
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='ab_tests' AND column_name='config_a') THEN
                    ALTER TABLE ab_tests ADD COLUMN config_a JSONB DEFAULT '{}';
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='ab_tests' AND column_name='config_b') THEN
                    ALTER TABLE ab_tests ADD COLUMN config_b JSONB DEFAULT '{}';
                END IF;
            END $$;
        `);
        
        // Create A/B test visitors table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ab_test_visitors (
                id SERIAL PRIMARY KEY,
                test_id INTEGER REFERENCES ab_tests(id) ON DELETE CASCADE,
                visitor_id VARCHAR(100) NOT NULL,
                variant VARCHAR(10) NOT NULL,
                ip_address VARCHAR(45),
                user_agent TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(test_id, visitor_id)
            );
        `);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_ab_visitors_test ON ab_test_visitors(test_id);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_ab_visitors_variant ON ab_test_visitors(variant);`);
        
        // Create A/B test conversions table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ab_test_conversions (
                id SERIAL PRIMARY KEY,
                test_id INTEGER REFERENCES ab_tests(id) ON DELETE CASCADE,
                visitor_id VARCHAR(100) NOT NULL,
                variant VARCHAR(10) NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                value DECIMAL(10,2) DEFAULT 0,
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_ab_conversions_test ON ab_test_conversions(test_id);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_ab_conversions_event ON ab_test_conversions(event_type);`);
        
        // Create financial_costs table for expense tracking
        await pool.query(`
            CREATE TABLE IF NOT EXISTS financial_costs (
                id SERIAL PRIMARY KEY,
                cost_date DATE NOT NULL,
                category VARCHAR(50) NOT NULL DEFAULT 'other',
                description TEXT NOT NULL,
                amount DECIMAL(12,2) NOT NULL,
                currency VARCHAR(3) NOT NULL DEFAULT 'BRL',
                amount_usd DECIMAL(12,2),
                exchange_rate DECIMAL(10,4),
                notes TEXT,
                created_by INTEGER,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_financial_costs_date ON financial_costs(cost_date);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_financial_costs_category ON financial_costs(category);`);
        
        // Add missing columns to admin_users if they don't exist (for existing tables)
        // Support both 'name' and 'full_name' columns for compatibility
        await pool.query(`ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS name VARCHAR(255);`);
        await pool.query(`ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS full_name VARCHAR(255);`);
        await pool.query(`ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS role VARCHAR(50) DEFAULT 'support';`);
        await pool.query(`ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT true;`);
        await pool.query(`ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS last_login TIMESTAMP WITH TIME ZONE;`);
        await pool.query(`ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS created_by INTEGER;`);
        
        // Remove NOT NULL constraint from full_name if it exists
        try {
            await pool.query(`ALTER TABLE admin_users ALTER COLUMN full_name DROP NOT NULL;`);
        } catch (e) { /* Column might not exist or already nullable */ }
        
        // Insert default admin user if not exists (using env vars)
        const adminEmail = process.env.ADMIN_EMAIL || 'admin@zapspy.ai';
        const adminPassword = process.env.ADMIN_PASSWORD || 'zapspy2024';
        const existingAdmin = await pool.query('SELECT id FROM admin_users WHERE role = $1', ['admin']);
        
        if (existingAdmin.rows.length === 0) {
            const hashedPassword = await bcrypt.hash(adminPassword, 10);
            await pool.query(`
                INSERT INTO admin_users (username, email, password_hash, name, role, is_active)
                VALUES ($1, $2, $3, $4, $5, true)
                ON CONFLICT (email) DO NOTHING
            `, ['admin', adminEmail, hashedPassword, 'Administrador', 'admin']);
            console.log('✅ Default admin user created');
        }
        
        console.log('✅ Database ready');
        
        // ==================== CLEANUP: Remove duplicate refund_requests ====================
        try {
            // Fix source values: normalize 'monetizze_deep_sync' and 'monetizze_postback_reprocess' to 'monetizze'
            const sourceFixResult = await pool.query(`
                UPDATE refund_requests 
                SET source = 'monetizze' 
                WHERE source IN ('monetizze_deep_sync', 'monetizze_postback_reprocess')
            `);
            if (sourceFixResult.rowCount > 0) {
                console.log(`🔧 Fixed ${sourceFixResult.rowCount} refund_requests with non-standard monetizze source`);
            }
            
            // Remove duplicate refund_requests: keep only the most recent per email + source combo
            const dupsResult = await pool.query(`
                DELETE FROM refund_requests 
                WHERE id NOT IN (
                    SELECT DISTINCT ON (LOWER(email), COALESCE(source, 'form')) id
                    FROM refund_requests
                    ORDER BY LOWER(email), COALESCE(source, 'form'), created_at DESC
                )
            `);
            if (dupsResult.rowCount > 0) {
                console.log(`🧹 Removed ${dupsResult.rowCount} duplicate refund_requests entries`);
            }
            
            // Also remove duplicates by transaction_id (keep most recent)
            const txDupsResult = await pool.query(`
                DELETE FROM refund_requests 
                WHERE transaction_id IS NOT NULL 
                  AND id NOT IN (
                    SELECT DISTINCT ON (transaction_id) id
                    FROM refund_requests
                    WHERE transaction_id IS NOT NULL
                    ORDER BY transaction_id, created_at DESC
                )
            `);
            if (txDupsResult.rowCount > 0) {
                console.log(`🧹 Removed ${txDupsResult.rowCount} duplicate refund_requests by transaction_id`);
            }
        } catch (cleanupError) {
            console.error('⚠️ Refund cleanup error (non-blocking):', cleanupError.message);
        }
        
        // ==================== BACKFILL: Cross-reference refunds with leads/transactions ====================
        try {
            // Find refunds without funnel_language and try to fill from transactions
            const unfilled = await pool.query(`
                SELECT r.id, r.email 
                FROM refund_requests r 
                WHERE r.funnel_language IS NULL AND r.email IS NOT NULL
            `);
            
            if (unfilled.rows.length > 0) {
                console.log(`🔄 Backfill: ${unfilled.rows.length} refunds without language, cross-referencing...`);
                let updated = 0;
                
                for (const refund of unfilled.rows) {
                    // Try transactions first
                    let lang = null;
                    let val = null;
                    let txId = null;
                    
                    const txResult = await pool.query(`
                        SELECT transaction_id, value, funnel_language 
                        FROM transactions 
                        WHERE LOWER(email) = LOWER($1) AND status = 'approved'
                        ORDER BY created_at DESC LIMIT 1
                    `, [refund.email]);
                    
                    if (txResult.rows.length > 0) {
                        lang = txResult.rows[0].funnel_language;
                        val = txResult.rows[0].value;
                        txId = txResult.rows[0].transaction_id;
                    }
                    
                    // If no language from tx, try leads table (direct column)
                    if (!lang) {
                        const leadResult = await pool.query(`
                            SELECT funnel_language
                            FROM leads 
                            WHERE LOWER(email) = LOWER($1) AND funnel_language IS NOT NULL
                            ORDER BY created_at DESC LIMIT 1
                        `, [refund.email]);
                        
                        if (leadResult.rows.length > 0) {
                            lang = leadResult.rows[0].funnel_language;
                        }
                    }
                    
                    // If no language from leads, try funnel_events
                    if (!lang) {
                        const eventResult = await pool.query(`
                            SELECT metadata->>'funnelLanguage' as funnel_language
                            FROM funnel_events 
                            WHERE LOWER(metadata->>'email') = LOWER($1) AND metadata->>'funnelLanguage' IS NOT NULL
                            ORDER BY created_at DESC LIMIT 1
                        `, [refund.email]);
                        
                        if (eventResult.rows.length > 0) {
                            lang = eventResult.rows[0].funnel_language;
                        }
                    }
                    
                    // Update if we found any data
                    if (lang || val || txId) {
                        await pool.query(`
                            UPDATE refund_requests 
                            SET funnel_language = COALESCE(funnel_language, $1),
                                value = COALESCE(value, $2),
                                transaction_id = COALESCE(transaction_id, $3)
                            WHERE id = $4
                        `, [lang, val, txId, refund.id]);
                        updated++;
                    }
                }
                
                console.log(`✅ Backfill complete: ${updated}/${unfilled.rows.length} refunds enriched with cross-referenced data`);
            }
        } catch (backfillError) {
            console.error('⚠️ Backfill error (non-blocking):', backfillError.message);
        }
        
    } catch (error) {
        console.error('❌ Database init error:', error.message);
    }
}

// Debug endpoint: Customer journey - see all events for a specific email or visitor_id
app.get('/api/admin/debug/customer-journey', authenticateToken, async (req, res) => {
    try {
        const { email, visitor_id } = req.query;
        if (!email && !visitor_id) {
            return res.status(400).json({ error: 'Provide email or visitor_id query parameter' });
        }
        
        // Find lead
        let lead = null;
        let vid = visitor_id;
        if (email) {
            const leadResult = await pool.query(
                `SELECT * FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                [email]
            );
            if (leadResult.rows.length > 0) {
                lead = leadResult.rows[0];
                vid = lead.visitor_id;
            }
        }
        
        // Get all funnel events for this visitor
        const events = vid ? await pool.query(
            `SELECT event, page, created_at, metadata FROM funnel_events WHERE visitor_id = $1 ORDER BY created_at ASC`,
            [vid]
        ) : { rows: [] };
        
        // Get all transactions for this email
        const transactions = email ? await pool.query(
            `SELECT transaction_id, product, value, status, monetizze_status, funnel_language, funnel_source, created_at FROM transactions WHERE LOWER(email) = LOWER($1) ORDER BY created_at ASC`,
            [email]
        ) : (lead ? await pool.query(
            `SELECT transaction_id, product, value, status, monetizze_status, funnel_language, funnel_source, created_at FROM transactions WHERE LOWER(email) = LOWER($1) ORDER BY created_at ASC`,
            [lead.email]
        ) : { rows: [] });
        
        // Get CAPI purchase logs
        const capiLogs = email ? await pool.query(
            `SELECT * FROM capi_purchase_logs WHERE LOWER(email) = LOWER($1) ORDER BY created_at ASC`,
            [email]
        ) : { rows: [] };
        
        res.json({
            lead: lead ? { id: lead.id, email: lead.email, name: lead.name, visitor_id: lead.visitor_id, status: lead.status, funnel_language: lead.funnel_language, products_purchased: lead.products_purchased, total_spent: lead.total_spent, created_at: lead.created_at } : null,
            visitor_id: vid,
            funnel_events: events.rows,
            transactions: transactions.rows,
            capi_purchase_logs: capiLogs.rows,
            summary: {
                total_events: events.rows.length,
                total_transactions: transactions.rows.length,
                upsell_events: events.rows.filter(e => e.event.includes('upsell')).map(e => ({ event: e.event, page: e.page, time: e.created_at }))
            }
        });
    } catch (error) {
        console.error('Customer journey error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Debug endpoint: Upsell tracking analysis
// Find upsell events that might be orphaned (not linked to leads)
app.get('/api/admin/debug/upsell-tracking', authenticateToken, async (req, res) => {
    try {
        // 1. Get all upsell-related events
        const upsellEvents = await pool.query(`
            SELECT event, COUNT(*) as count
            FROM funnel_events
            WHERE event LIKE '%upsell%'
            GROUP BY event
            ORDER BY count DESC
        `);
        
        // 2. Get upsell events that DON'T have a matching lead (orphaned)
        const orphanedEvents = await pool.query(`
            SELECT fe.visitor_id, COUNT(*) as event_count, 
                   array_agg(DISTINCT fe.event) as events,
                   MIN(fe.created_at) as first_event,
                   MAX(fe.created_at) as last_event
            FROM funnel_events fe
            LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
            WHERE fe.event LIKE '%upsell%' AND l.id IS NULL
            GROUP BY fe.visitor_id
            ORDER BY last_event DESC
            LIMIT 50
        `);
        
        // 3. Get upsell events that DO have matching leads
        const linkedEvents = await pool.query(`
            SELECT l.id as lead_id, l.email, l.name, fe.visitor_id,
                   COUNT(*) as event_count,
                   array_agg(DISTINCT fe.event) as events
            FROM funnel_events fe
            INNER JOIN leads l ON fe.visitor_id = l.visitor_id
            WHERE fe.event LIKE '%upsell%'
            GROUP BY l.id, l.email, l.name, fe.visitor_id
            ORDER BY MAX(fe.created_at) DESC
            LIMIT 50
        `);
        
        // 4. Get recent upsell events with page info
        const recentUpsellEvents = await pool.query(`
            SELECT fe.*, 
                   l.email as lead_email, 
                   l.name as lead_name
            FROM funnel_events fe
            LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
            WHERE fe.event LIKE '%upsell%'
            ORDER BY fe.created_at DESC
            LIMIT 100
        `);
        
        // 5. Check for visitor_ids that have transactions but no linked lead
        const transactionsWithoutLeadLink = await pool.query(`
            SELECT t.email, t.product, t.status, t.value, t.created_at,
                   l.visitor_id as lead_visitor_id,
                   (SELECT COUNT(*) FROM funnel_events fe WHERE fe.visitor_id = l.visitor_id AND fe.event LIKE '%upsell%') as upsell_events_count
            FROM transactions t
            LEFT JOIN leads l ON LOWER(t.email) = LOWER(l.email)
            WHERE t.status = 'approved'
            AND (t.product ILIKE '%recovery%' OR t.product ILIKE '%vault%' OR t.product ILIKE '%vision%' OR t.product ILIKE '%vip%')
            ORDER BY t.created_at DESC
            LIMIT 50
        `);
        
        res.json({
            summary: {
                totalUpsellEventTypes: upsellEvents.rows.length,
                orphanedVisitors: orphanedEvents.rows.length,
                linkedVisitors: linkedEvents.rows.length
            },
            eventBreakdown: upsellEvents.rows,
            orphanedEvents: orphanedEvents.rows,
            linkedEvents: linkedEvents.rows,
            recentEvents: recentUpsellEvents.rows,
            upsellTransactionsLinkStatus: transactionsWithoutLeadLink.rows
        });
        
    } catch (error) {
        console.error('Error in upsell tracking debug:', error);
        res.status(500).json({ error: error.message });
    }
});

// Debug endpoint: Find customer journey by email
app.get('/api/admin/debug/journey-by-email/:email', authenticateToken, async (req, res) => {
    try {
        const { email } = req.params;
        
        // Find all leads with this email
        const leads = await pool.query(`
            SELECT * FROM leads WHERE LOWER(email) = LOWER($1)
        `, [email]);
        
        // Find all transactions for this email
        const transactions = await pool.query(`
            SELECT * FROM transactions WHERE LOWER(email) = LOWER($1) ORDER BY created_at
        `, [email]);
        
        // Find all funnel events for each lead's visitor_id
        const allEvents = [];
        for (const lead of leads.rows) {
            if (lead.visitor_id) {
                const events = await pool.query(`
                    SELECT * FROM funnel_events WHERE visitor_id = $1 ORDER BY created_at
                `, [lead.visitor_id]);
                allEvents.push({
                    lead_id: lead.id,
                    visitor_id: lead.visitor_id,
                    events: events.rows
                });
            }
        }
        
        // Also find events that might match by looking for similar visitor_ids or time proximity
        // Look for upsell events that happened around the same time as the transactions
        if (transactions.rows.length > 0) {
            const firstTxTime = transactions.rows[0].created_at;
            const potentialEvents = await pool.query(`
                SELECT fe.*, l.email as potential_lead_email
                FROM funnel_events fe
                LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
                WHERE fe.event LIKE '%upsell%'
                AND fe.created_at BETWEEN $1::timestamp - INTERVAL '30 minutes' AND $1::timestamp + INTERVAL '30 minutes'
                ORDER BY fe.created_at
            `, [firstTxTime]);
            
            allEvents.push({
                context: 'potential_matches_by_time',
                events: potentialEvents.rows
            });
        }
        
        res.json({
            email: email,
            leads: leads.rows,
            transactions: transactions.rows,
            eventsByLead: allEvents
        });
        
    } catch (error) {
        console.error('Error in journey by email debug:', error);
        res.status(500).json({ error: error.message });
    }
});

// Start server
app.listen(PORT, async () => {
    console.log(`🚀 ZapSpy API running on port ${PORT}`);
    console.log(`📊 Admin panel: http://localhost:${PORT}/admin`);
    if (process.env.RAILWAY_PUBLIC_DOMAIN) {
        console.log(`🌐 Railway domain: https://${process.env.RAILWAY_PUBLIC_DOMAIN}`);
    }
    if (process.env.FRONTEND_URL) {
        console.log(`🔗 FRONTEND_URL: ${process.env.FRONTEND_URL}`);
    }
    console.log(`🔒 CORS allowed origins: ${ALLOWED_ORIGINS.join(', ')}`);
    
    // Initialize database
    await initDatabase();
    
    // Start auto-sync with Monetizze (every 30 minutes)
    startAutoSync();
});
