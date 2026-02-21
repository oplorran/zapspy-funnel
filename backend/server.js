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
const cookieParser = require('cookie-parser');
const path = require('path');

const pool = require('./src/database');
const { ALLOWED_ORIGINS } = require('./src/config');
const { adminLimiter } = require('./src/middleware');
const { initDatabase } = require('./src/init-database');
const { startAutoSync } = require('./src/services/monetizze');
const { errorHandler, notFoundHandler } = require('./src/error-handler');
const { validateEnv } = require('./src/validate-env');

// Route modules
const publicRoutes = require('./src/routes/public');
const postbackRoutes = require('./src/routes/postbacks');
const adminAuthRoutes = require('./src/routes/admin-auth');
const adminLeadsRoutes = require('./src/routes/admin-leads');
const adminStatsRoutes = require('./src/routes/admin-stats');
const adminAbTestsRoutes = require('./src/routes/admin-ab-tests');
const adminRecoveryRoutes = require('./src/routes/admin-recovery');
const adminRefundsRoutes = require('./src/routes/admin-refunds');
const adminDebugRoutes = require('./src/routes/admin-debug');

const app = express();
const PORT = process.env.PORT || 3000;

app.set('trust proxy', 1);

// ==================== MIDDLEWARE ====================

app.use(helmet({
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false
}));

app.use(compression({ threshold: 512 }));

// Dynamic CORS origins from A/B test URLs
const abTestAllowedOrigins = new Set();
async function loadABTestOrigins() {
    try {
        const result = await pool.query(`SELECT url_a, url_b FROM ab_tests WHERE url_a IS NOT NULL OR url_b IS NOT NULL`);
        for (const row of result.rows) {
            try { if (row.url_a) abTestAllowedOrigins.add(new URL(row.url_a).origin); } catch(e) {}
            try { if (row.url_b) abTestAllowedOrigins.add(new URL(row.url_b).origin); } catch(e) {}
        }
        if (abTestAllowedOrigins.size > 0) console.log(`🔒 AB Test CORS origins loaded: ${[...abTestAllowedOrigins].join(', ')}`);
    } catch(e) { /* DB not ready yet */ }
}

// Make abTestAllowedOrigins accessible to routes via app.locals
app.locals.abTestAllowedOrigins = abTestAllowedOrigins;

// Auto-detect server host for CORS
const selfOrigins = new Set();
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
        if (!origin) return callback(null, true);

        if (selfOrigins.has(origin)) return callback(null, true);

        const allowed = process.env.FRONTEND_URL
            ? process.env.FRONTEND_URL.split(',').map(s => s.trim())
            : [...ALLOWED_ORIGINS];

        if (process.env.RAILWAY_PUBLIC_DOMAIN) {
            allowed.push(`https://${process.env.RAILWAY_PUBLIC_DOMAIN}`);
        }
        if (process.env.RAILWAY_STATIC_URL) {
            allowed.push(process.env.RAILWAY_STATIC_URL.startsWith('http')
                ? process.env.RAILWAY_STATIC_URL
                : `https://${process.env.RAILWAY_STATIC_URL}`);
        }

        if (allowed.includes('*') || allowed.includes(origin)) return callback(null, true);
        
        try {
            const originHost = new URL(origin).hostname;
            if (originHost.endsWith('.zappdetect.com') || originHost.endsWith('.whatstalker.com')) {
                return callback(null, true);
            }
        } catch(e) {}
        
        if (abTestAllowedOrigins.has(origin)) return callback(null, true);
        
        console.error(`CORS blocked origin: ${origin} (allowed: ${allowed.join(', ')})`);
        callback(new Error('CORS not allowed'), false);
    },
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    credentials: true
}));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

// No-cache for admin panel files
app.use((req, res, next) => {
    if (req.path.includes('admin') || req.path.startsWith('/js/admin') || req.path.startsWith('/css/admin')) {
        res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
        res.set('Pragma', 'no-cache');
    }
    next();
});

// ==================== A/B TEST TRAFFIC SPLITTER ====================
// Must be BEFORE static file serving so /go/:slug is handled by Express
app.get('/go/:slug', async (req, res) => {
    try {
        const { slug } = req.params;
        
        const test = await pool.query(
            `SELECT id, url_a, url_b, traffic_split, status FROM ab_tests WHERE slug = $1 AND status = 'running' LIMIT 1`,
            [slug]
        );
        
        if (test.rows.length === 0) {
            return res.status(404).send('Test not found or not running');
        }
        
        const activeTest = test.rows[0];
        const testId = activeTest.id;
        
        const forceVariant = req.query.force?.toUpperCase();
        
        const cookieName = `ab_${testId}`;
        let variant = forceVariant && ['A', 'B'].includes(forceVariant) 
            ? forceVariant 
            : req.cookies?.[cookieName];
        
        if (!variant || !['A', 'B'].includes(variant)) {
            const random = Math.random() * 100;
            variant = random < Number(activeTest.traffic_split) ? 'A' : 'B';
            console.log(`🎲 AB Random: ${random.toFixed(2)} < ${activeTest.traffic_split} → ${variant}`);
        }
        
        res.cookie(cookieName, variant, { 
            maxAge: 30 * 24 * 60 * 60 * 1000, 
            httpOnly: false, 
            sameSite: 'lax',
            secure: true 
        });
        
        const visitorIp = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        const visitorUa = req.headers['user-agent'] || '';
        const visitorId = `split_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
        
        pool.query(`
            INSERT INTO ab_test_visitors (test_id, visitor_id, variant, ip_address, user_agent)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (test_id, visitor_id) DO NOTHING
        `, [testId, visitorId, variant, visitorIp, visitorUa]).catch(err => {
            console.error('AB split visitor log error:', err.message);
        });
        
        const targetUrl = variant === 'A' ? activeTest.url_a : activeTest.url_b;
        const url = new URL(targetUrl);
        
        url.searchParams.set('ab', String(testId));
        url.searchParams.set('abv', variant);
        
        const originalParams = req.query;
        for (const [key, value] of Object.entries(originalParams)) {
            if (key.startsWith('utm_') || key === 'fbclid' || key === 'gclid' || key === 'src') {
                url.searchParams.set(key, String(value));
            }
        }
        
        console.log(`🔀 AB Split: test=${testId} slug=${slug} variant=${variant} → ${url.toString()}`);
        
        res.redirect(302, url.toString());
        
    } catch (error) {
        console.error('AB split error:', error);
        res.status(500).send('Split error');
    }
});

// ==================== STATIC FILE SERVING ====================

app.use(express.static('public'));
app.use('/ingles', express.static(path.join(__dirname, 'public', 'ingles')));
app.use('/espanhol', express.static(path.join(__dirname, 'public', 'espanhol')));
app.use('/perfectpay', express.static(path.join(__dirname, 'public', 'perfectpay')));
app.use('/ingles3', express.static(path.join(__dirname, 'public', 'ingles3')));
app.use('/en', express.static(path.join(__dirname, 'public', 'ingles')));
app.use('/es', express.static(path.join(__dirname, 'public', 'espanhol')));

// Domain-based routing
app.use((req, res, next) => {
    const host = req.hostname || req.headers.host || '';
    if (host.includes('perfect') || host.includes('perfect.zappdetect')) {
        if (req.path.startsWith('/ingles')) {
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

// ==================== MOUNT ROUTES ====================

// Admin rate limiter for all admin routes
app.use('/api/admin', adminLimiter);

// Public routes (health, leads, tracking, CAPI, refunds)
app.use('/', publicRoutes);

// Postback webhooks (Monetizze + PerfectPay)
app.use('/', postbackRoutes);

// Admin routes
app.use('/', adminAuthRoutes);
app.use('/', adminLeadsRoutes);
app.use('/', adminStatsRoutes);
app.use('/', adminAbTestsRoutes);
app.use('/', adminRecoveryRoutes);
app.use('/', adminRefundsRoutes);
app.use('/', adminDebugRoutes);

// ==================== ERROR HANDLING ====================

app.use(notFoundHandler);
app.use(errorHandler);

// ==================== START SERVER ====================

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
    
    validateEnv();
    await initDatabase();
    await loadABTestOrigins();
    startAutoSync();
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('📴 SIGTERM received. Shutting down gracefully...');
    process.exit(0);
});

process.on('unhandledRejection', (reason) => {
    console.error('⚠️ Unhandled Promise Rejection:', reason);
});
