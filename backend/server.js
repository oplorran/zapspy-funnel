/**
 * ZapSpy.ai Backend API
 * Lead capture and admin panel API
 * With Facebook Conversions API integration
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
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
    // English funnel pixels
    en: [
        {
            id: '955477126807496',
            token: process.env.FB_PIXEL_TOKEN_1 || 'EAALZCphpZCmcIBQlQHRs2JIRPdsRXG8RYa25OuW5yct9zASVIZAXUyhNPPc0yNdPl7bKNGZBldKM9HXSPuGaj1sggT3Ogco9PfSTDVf6wUNWguVEWLYdtvwpm98Qy0sd5gwvotspZBDyjxserjHVAMGFZAMeYC7aaanSIamK9OUQtRLWwjEpP28Cq5CydGZCoqPDwZDZD',
            name: 'SPY INGLES 2026 - PABLO'
        },
        {
            id: '726299943423075',
            token: process.env.FB_PIXEL_TOKEN_2 || 'EAALZCphpZCmcIBQodgl2fJ81kKfOWRmhYmJPBVQSfOuBBbxfjOxg3HH6y03bqp8fAbZCoghz8d9HglfpbBeZBl7wTaBGvIWRqtNgoJCFz5lts434LKD5EhF26KZCFjICN9jwsEdDu4afDUYH8Ld5ZC9D8gRFq3Y884qotjlqIszrQAzZAju7qkt9OgMhX7X093PNQZDZD',
            name: '[PABLO NOVO] - [SPY INGLES] - [2025]'
        }
    ],
    // Spanish funnel pixels
    es: [
        {
            id: '534495082571779',
            token: process.env.FB_PIXEL_TOKEN_ES_1 || 'EAALZCphpZCmcIBQh5zHSNNj666RUi8XybMe3ZBRE31J9czSE04LBY4nZC9PBNG8SFNL4yCJf6zb9V88JkjNz55nTaIZC2wKSW22OhohIBY0IyYPYXTBFQTBVWUUIYDHhgZBf1CDVye724ekcSA6UbwSqJQPK8XYLEkvUfoJtXq7ktPv7qMOjloAx3jXdjUdJM3TgZDZD',
            name: 'PIXEL SPY ESPANHOL'
        },
        {
            id: '1271198251735428',
            token: process.env.FB_PIXEL_TOKEN_ES_2 || 'EAALZCphpZCmcIBQh5zHSNNj666RUi8XybMe3ZBRE31J9czSE04LBY4nZC9PBNG8SFNL4yCJf6zb9V88JkjNz55nTaIZC2wKSW22OhohIBY0IyYPYXTBFQTBVWUUIYDHhgZBf1CDVye724ekcSA6UbwSqJQPK8XYLEkvUfoJtXq7ktPv7qMOjloAx3jXdjUdJM3TgZDZD',
            name: 'SPY ESPANHOL 2026 - PABLO'
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

const FB_API_VERSION = 'v18.0';

// Hash function for user data (required by Facebook)
function hashData(data) {
    if (!data) return null;
    return crypto.createHash('sha256').update(data.toLowerCase().trim()).digest('hex');
}

// Normalize phone number for Facebook
function normalizePhone(phone) {
    if (!phone) return null;
    // Remove all non-numeric characters
    return phone.replace(/\D/g, '');
}

// Send event to Facebook Conversions API
// eventId: if provided, use it for deduplication with browser pixel
// userData.externalId: visitor ID for cross-device tracking
// options.language: 'en' or 'es' to select correct pixels
// options.pixelIds: array of custom pixel IDs (from frontend)
// options.accessToken: custom access token (from frontend)
async function sendToFacebookCAPI(eventName, userData, customData = {}, eventSourceUrl = null, eventId = null, options = {}) {
    const timestamp = Math.floor(Date.now() / 1000);
    // Use provided eventId or generate one
    const finalEventId = eventId || `${eventName}_${timestamp}_${Math.random().toString(36).substr(2, 9)}`;
    
    // Build user_data object
    const user_data = {};
    
    if (userData.email) {
        user_data.em = [hashData(userData.email)];
    }
    if (userData.phone) {
        user_data.ph = [hashData(normalizePhone(userData.phone))];
    }
    if (userData.firstName) {
        const names = userData.firstName.trim().split(' ');
        user_data.fn = [hashData(names[0])];
        if (names.length > 1) {
            user_data.ln = [hashData(names.slice(1).join(' '))];
        }
    }
    if (userData.lastName) {
        user_data.ln = [hashData(userData.lastName)];
    }
    if (userData.ip) {
        user_data.client_ip_address = userData.ip;
    }
    if (userData.userAgent) {
        user_data.client_user_agent = userData.userAgent;
    }
    if (userData.fbc) {
        user_data.fbc = userData.fbc;
    }
    if (userData.fbp) {
        user_data.fbp = userData.fbp;
    }
    // External ID for cross-device tracking
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
    
    if (eventSourceUrl) {
        eventPayload.event_source_url = eventSourceUrl;
    }
    
    if (Object.keys(customData).length > 0) {
        eventPayload.custom_data = customData;
    }
    
    // Get pixels for the correct language (or use custom pixels from frontend)
    const pixels = getPixelsForLanguage(options.language, options.pixelIds, options.accessToken);
    
    // Send to all pixels
    const results = [];
    
    for (const pixel of pixels) {
        try {
            const url = `https://graph.facebook.com/${FB_API_VERSION}/${pixel.id}/events?access_token=${pixel.token}`;
            
            const response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    data: [eventPayload]
                })
            });
            
            const result = await response.json();
            
            if (response.ok) {
                console.log(`✅ CAPI [${pixel.name}] ${eventName}: success (id: ${finalEventId}, events_received: ${result.events_received || 1})`);
                results.push({ pixel: pixel.id, success: true, result, eventId: finalEventId });
            } else {
                console.error(`❌ CAPI [${pixel.name}] ${eventName}: error`, result);
                results.push({ pixel: pixel.id, success: false, error: result });
            }
        } catch (error) {
            console.error(`❌ CAPI [${pixel.name}] ${eventName}: exception`, error.message);
            results.push({ pixel: pixel.id, success: false, error: error.message });
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

app.use(cors({
    origin: process.env.FRONTEND_URL || '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
    credentials: true
}));

app.use(express.json());
app.use(express.static('public'));

// Serve funnel static files
// These are placed in public/ingles and public/espanhol after build
const path = require('path');

// ==================== GEOLOCATION HELPER ====================
const https = require('https');

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
            
            const options = {
                method: 'GET',
                hostname: 'ip-geo-location.p.rapidapi.com',
                port: null,
                path: `/ip/${cleanIP}?format=json&language=en`,
                headers: {
                    'x-rapidapi-key': process.env.RAPIDAPI_KEY || 'd03f07c7d0msh0e23fb53734dctqp1c2c1fjsnc7937b7aa011',
                    'x-rapidapi-host': 'ip-geo-location.p.rapidapi.com'
                }
            };
            
            const req = https.request(options, function (res) {
                const chunks = [];
                
                res.on('data', function (chunk) {
                    chunks.push(chunk);
                });
                
                res.on('end', function () {
                    try {
                        const body = Buffer.concat(chunks);
                        const data = JSON.parse(body.toString());
                        
                        console.log('Geolocation: Found -', data.country?.name, data.country?.code, data.city?.name);
                        resolve({
                            country: data.country?.name || null,
                            country_code: data.country?.code || null,
                            city: data.city?.name || null
                        });
                    } catch (parseError) {
                        console.log('Geolocation parse error:', parseError.message);
                        resolve({ country: null, country_code: null, city: null });
                    }
                });
            });
            
            req.on('error', function (error) {
                console.log('Geolocation request error:', error.message);
                resolve({ country: null, country_code: null, city: null });
            });
            
            // Timeout after 5 seconds
            req.setTimeout(5000, function() {
                console.log('Geolocation: Request timeout');
                req.destroy();
                resolve({ country: null, country_code: null, city: null });
            });
            
            req.end();
        } catch (error) {
            console.log('Geolocation error:', error.message);
            resolve({ country: null, country_code: null, city: null });
        }
    });
}

app.use('/ingles', express.static(path.join(__dirname, 'public', 'ingles')));
app.use('/espanhol', express.static(path.join(__dirname, 'public', 'espanhol')));
app.use('/en', express.static(path.join(__dirname, 'public', 'ingles')));
app.use('/es', express.static(path.join(__dirname, 'public', 'espanhol')));

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

// JWT Authentication middleware
const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    
    if (!token) {
        return res.status(401).json({ error: 'Access token required' });
    }
    
    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) {
            return res.status(403).json({ error: 'Invalid or expired token' });
        }
        req.user = user;
        next();
    });
};

// ==================== PUBLIC API ROUTES ====================

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Root route
app.get('/', (req, res) => {
    res.json({ 
        name: 'ZapSpy.ai API',
        version: '1.0.0',
        status: 'running',
        admin: '/admin.html'
    });
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
            referrer,
            userAgent,
            fbc,  // Facebook click ID (from URL param or cookie)
            fbp,  // Facebook browser ID (from cookie)
            funnelLanguage,  // 'en' or 'es' - funnel language for pixel selection
            visitorId  // Funnel visitor ID for journey tracking
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
        
        // Get country from IP (non-blocking)
        const geoData = await getCountryFromIP(ipAddress);
        
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
                    last_visit_at = NOW(),
                    updated_at = NOW()
                WHERE id = $12
                RETURNING id, created_at`,
                [name || null, targetPhone || null, targetGender || null, ipAddress, referrer || null, ua || null, currentVisitCount + 1, geoData.country, geoData.country_code, geoData.city, visitorId || null, existingLead.rows[0].id]
            );
            console.log(`Returning lead [${language.toUpperCase()}]: ${name || 'No name'} - ${email} - ${geoData.country || 'Unknown'} (visit #${currentVisitCount + 1})`);
        } else {
            // Insert new lead
            result = await pool.query(
                `INSERT INTO leads (name, email, whatsapp, target_phone, target_gender, ip_address, referrer, user_agent, funnel_language, visit_count, country, country_code, city, visitor_id, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, $10, $11, $12, $13, NOW())
                 RETURNING id, created_at`,
                [name || null, email, whatsapp, targetPhone || null, targetGender || null, ipAddress, referrer || null, ua || null, language, geoData.country, geoData.country_code, geoData.city, visitorId || null]
            );
            isNewLead = true;
            console.log(`New lead captured [${language.toUpperCase()}]: ${name || 'No name'} - ${email} - ${whatsapp} - ${geoData.country || 'Unknown'}`);
        }
        
        // Send Lead event to Facebook Conversions API (using correct pixels for language)
        try {
            await sendToFacebookCAPI('Lead', {
                email: email,
                phone: whatsapp,
                firstName: name,
                ip: ipAddress,
                userAgent: ua,
                fbc: fbc,
                fbp: fbp
            }, {
                content_name: 'Lead Capture Form',
                content_category: 'Lead'
            }, referrer, null, { language: language });
        } catch (capiError) {
            console.error('CAPI Lead error (non-blocking):', capiError.message);
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
        
        // Build user data
        const userData = {
            email,
            phone,
            firstName,
            lastName,
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
        const { email, password } = req.body;
        
        // Check against environment variables (simple auth)
        if (email !== process.env.ADMIN_EMAIL) {
            return res.status(401).json({ error: 'Invalid credentials' });
        }
        
        // For first login, compare plain text; in production use hashed password
        const validPassword = password === process.env.ADMIN_PASSWORD;
        
        if (!validPassword) {
            return res.status(401).json({ error: 'Invalid credentials' });
        }
        
        // Generate JWT token
        const token = jwt.sign(
            { email, role: 'admin' },
            process.env.JWT_SECRET,
            { expiresIn: '24h' }
        );
        
        res.json({
            success: true,
            token,
            expiresIn: '24h'
        });
        
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ error: 'Login failed' });
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

// Get lead statistics (protected)
app.get('/api/admin/stats', authenticateToken, async (req, res) => {
    try {
        const [totalResult, todayResult, weekResult, statusResult] = await Promise.all([
            pool.query('SELECT COUNT(*) FROM leads'),
            pool.query(`SELECT COUNT(*) FROM leads WHERE created_at >= CURRENT_DATE`),
            pool.query(`SELECT COUNT(*) FROM leads WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'`),
            pool.query(`SELECT status, COUNT(*) FROM leads GROUP BY status`)
        ]);
        
        // Get leads by day for the last 7 days
        const dailyResult = await pool.query(`
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM leads
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(created_at)
            ORDER BY date DESC
        `);
        
        // Get leads by gender
        const genderResult = await pool.query(`
            SELECT target_gender, COUNT(*) FROM leads GROUP BY target_gender
        `);
        
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

// Serve admin panel
app.get('/admin', (req, res) => {
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
            metadata
        } = req.body;
        
        if (!visitorId || !event) {
            return res.status(400).json({ error: 'visitorId and event are required' });
        }
        
        const ipAddress = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        const userAgent = req.headers['user-agent'] || null;
        const language = funnelLanguage || 'en';
        
        // Add language to metadata
        const enrichedMetadata = {
            ...(metadata || {}),
            funnelLanguage: language
        };
        
        await pool.query(
            `INSERT INTO funnel_events (visitor_id, event, page, target_phone, target_gender, ip_address, user_agent, metadata, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())`,
            [visitorId, event, page || null, targetPhone || null, targetGender || null, ipAddress, userAgent, JSON.stringify(enrichedMetadata)]
        );
        
        res.json({ success: true, language });
        
    } catch (error) {
        console.error('Error tracking event:', error);
        res.status(500).json({ error: 'Failed to track event' });
    }
});

// Get funnel analytics (protected)
app.get('/api/admin/funnel', authenticateToken, async (req, res) => {
    try {
        const { language } = req.query;
        
        // Build language filter condition
        // The metadata column stores JSON with funnelLanguage field
        let langCondition = '';
        let langParams = [];
        if (language === 'en' || language === 'es') {
            langCondition = `AND (metadata->>'funnelLanguage' = $1 OR (metadata->>'funnelLanguage' IS NULL AND $1 = 'en'))`;
            langParams = [language];
        }
        
        // Get funnel stats by step
        const funnelStats = await pool.query(`
            SELECT 
                event,
                COUNT(DISTINCT visitor_id) as unique_visitors,
                COUNT(*) as total_events
            FROM funnel_events
            WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
            ${langCondition}
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
        `, langParams);
        
        // Get daily funnel data
        const dailyStats = await pool.query(`
            SELECT 
                DATE(created_at) as date,
                event,
                COUNT(DISTINCT visitor_id) as unique_visitors
            FROM funnel_events
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
            ${langCondition}
            GROUP BY DATE(created_at), event
            ORDER BY date DESC, event
        `, langParams);
        
        // Get visitor journeys (last 50)
        const journeys = await pool.query(`
            SELECT 
                visitor_id,
                target_phone,
                target_gender,
                array_agg(event ORDER BY created_at) as events,
                MIN(created_at) as first_seen,
                MAX(created_at) as last_seen,
                COUNT(*) as total_events
            FROM funnel_events
            WHERE 1=1 ${langCondition}
            GROUP BY visitor_id, target_phone, target_gender
            ORDER BY MAX(created_at) DESC
            LIMIT 50
        `, langParams);
        
        res.json({
            funnelStats: funnelStats.rows,
            dailyStats: dailyStats.rows,
            journeys: journeys.rows,
            language: language || 'all'
        });
        
    } catch (error) {
        console.error('Error fetching funnel data:', error);
        res.status(500).json({ error: 'Failed to fetch funnel data' });
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
                'page_view_landing': '👀 Visitou página inicial',
                'page_view_phone': '📱 Página do telefone',
                'gender_selected': '👤 Selecionou gênero',
                'phone_submitted': '✅ Submeteu telefone alvo',
                'page_view_conversas': '💬 Visualizou conversas',
                'page_view_chat': '💬 Visualizou chat',
                'page_view_cta': '🎯 Página de oferta',
                'email_captured': '📧 Email capturado',
                'checkout_clicked': '🛒 Clicou no checkout',
                'scroll_50_percent': '📜 Scroll 50%',
                'scroll_100_percent': '📜 Scroll 100%',
                'time_on_page_30s': '⏱️ 30s na página',
                'time_on_page_60s': '⏱️ 60s na página',
                'exit_intent_shown': '🚪 Exit intent'
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
        
        // Calculate summary
        const summary = {
            totalEvents: funnelEvents.length,
            totalTransactions: transactionsResult.rows.length,
            totalSpent: lead.total_spent || 0,
            productsPurchased: lead.products_purchased || [],
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

// Middleware to parse URL-encoded postback data
app.use(express.urlencoded({ extended: true }));

// Monetizze postback endpoint (public - no auth, uses token validation)
app.post('/api/postback/monetizze', async (req, res) => {
    try {
        console.log('📥 Monetizze Postback received:', JSON.stringify(req.body, null, 2));
        
        // Monetizze sends data as form-urlencoded
        const {
            chave_unica,        // Unique transaction key
            produto,            // Product info
            venda,              // Sale info
            comprador,          // Buyer info
            comissao,           // Commission info
            tipo_pagamento,     // Payment type
            status,             // Transaction status code
            data_compra,        // Purchase date
            data_atualizacao,   // Update date
            valor,              // Value
            email,              // Buyer email
            telefone,           // Buyer phone
            nome                // Buyer name
        } = req.body;
        
        // Validate postback (optional: check chave_unica against your secret)
        const postbackToken = process.env.MONETIZZE_POSTBACK_TOKEN;
        if (postbackToken && chave_unica !== postbackToken) {
            // Log but still process (Monetizze sends transaction ID as chave_unica)
            console.log('⚠️ Postback token check skipped (chave_unica is transaction ID)');
        }
        
        // Map Monetizze status codes to our status
        // Monetizze status codes:
        // 1 = Aguardando pagamento
        // 2 = Finalizada / Aprovada
        // 3 = Cancelada
        // 4 = Devolvida (Reembolso)
        // 5 = Abandono de Checkout
        // 6 = Bloqueada
        // 7 = Completa (subscription)
        // 8 = Chargeback
        // 9 = Ingressos
        // 10 = Assinatura - Ativa
        // 11 = Assinatura - Inadimplente
        // 12 = Assinatura - Cancelada
        // 13 = Assinatura - Aguardando pagamento
        // 14 = Status do pedido - Reenvio
        // 15 = Recuperação Parcelada - Ativa
        // 16 = Recuperação Parcelada - Cancelada
        
        const statusMap = {
            '1': 'pending_payment',
            '2': 'approved',
            '3': 'cancelled',
            '4': 'refunded',
            '5': 'abandoned_checkout',
            '6': 'blocked',
            '7': 'complete',
            '8': 'chargeback',
            '9': 'tickets',
            '10': 'subscription_active',
            '11': 'subscription_overdue',
            '12': 'subscription_cancelled',
            '13': 'subscription_pending',
            '14': 'resend',
            '15': 'recovery_active',
            '16': 'recovery_cancelled'
        };
        
        const mappedStatus = statusMap[status] || 'unknown';
        const buyerEmail = email || comprador?.email;
        const buyerPhone = telefone || comprador?.telefone;
        const buyerName = nome || comprador?.nome;
        const productName = typeof produto === 'object' ? produto.nome : produto;
        const productCode = typeof produto === 'object' ? (produto.codigo || produto.id) : null;
        const transactionValue = valor || venda?.valor;
        
        // Determine funnel language based on product code
        // Spanish product codes (Monetizze IDs):
        // - KCH455963: X Ai - Detector de Infidelidad (Front)
        // - KMS455971: X Ai - Detector de Infidelidad 50% OFF
        // - 455966 / up1: X Ai - Recuperación Total
        // - 455968 / up2: X Ai - Visión Total
        // - 455970 / up3: X Ai - VIP Sin Esperas
        // Spanish product names contain: "Infidelidad", "Recuperación", "Visión", "VIP Sin Esperas"
        const spanishProductCodes = ['455963', '455971', '455966', '455968', '455970', 'KCH455963', 'KMS455971'];
        const spanishProductKeywords = ['Infidelidad', 'Recuperación', 'Visión Total', 'VIP Sin Esperas'];
        
        let funnelLanguage = 'en'; // default to English
        if (productCode && spanishProductCodes.some(code => String(productCode).includes(code))) {
            funnelLanguage = 'es';
        } else if (productName && spanishProductKeywords.some(kw => productName.includes(kw))) {
            funnelLanguage = 'es';
        }
        
        // Identify product type (front/upsell1/upsell2/upsell3)
        let productType = 'front';
        const productNameLower = (productName || '').toLowerCase();
        
        // English products
        if (productNameLower.includes('full recovery') || productNameLower.includes('recovery') || productNameLower.includes('recuperação')) {
            productType = 'upsell1';
        } else if (productNameLower.includes('full vision') || productNameLower.includes('vision') || productNameLower.includes('visão')) {
            productType = 'upsell2';
        } else if (productNameLower.includes('vip') || productNameLower.includes('priority') || productNameLower.includes('prioridade')) {
            productType = 'upsell3';
        }
        // Spanish products  
        else if (productNameLower.includes('recuperación total')) {
            productType = 'upsell1';
        } else if (productNameLower.includes('visión total')) {
            productType = 'upsell2';
        } else if (productNameLower.includes('sin esperas')) {
            productType = 'upsell3';
        }
        
        console.log(`🌐 Funnel language detected: ${funnelLanguage} (product: ${productName}, code: ${productCode}, type: ${productType})`);
        
        // Store transaction in database with funnel_language
        await pool.query(`
            INSERT INTO transactions (
                transaction_id, email, phone, name, product, value, 
                monetizze_status, status, raw_data, funnel_language, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
            ON CONFLICT (transaction_id) 
            DO UPDATE SET 
                monetizze_status = $7,
                status = $8,
                raw_data = $9,
                funnel_language = $10,
                updated_at = NOW()
        `, [
            chave_unica,
            buyerEmail,
            buyerPhone,
            buyerName,
            productName,
            transactionValue,
            status,
            mappedStatus,
            JSON.stringify(req.body),
            funnelLanguage
        ]);
        
        // Try to match with existing lead and update status + products
        if (buyerEmail) {
            const purchaseValue = parseFloat(transactionValue) || 0;
            
            // Build product identifier (type + truncated name)
            const productIdentifier = `${productType}:${(productName || '').substring(0, 50)}`;
            
            const leadUpdate = await pool.query(`
                UPDATE leads 
                SET status = CASE 
                    WHEN $1 = 'approved' THEN 'converted'
                    WHEN $1 IN ('cancelled', 'refunded', 'chargeback') THEN 'lost'
                    WHEN $1 = 'pending_payment' THEN 'contacted'
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
            `, [mappedStatus, mappedStatus, buyerEmail, productIdentifier, purchaseValue]);
            
            if (leadUpdate.rows.length > 0) {
                const lead = leadUpdate.rows[0];
                console.log(`✅ Lead updated: ${buyerEmail} -> ${mappedStatus} | Products: ${lead.products_purchased?.join(', ') || 'none'} | Total: R$${lead.total_spent}`);
            } else {
                console.log(`⚠️ No matching lead found for: ${buyerEmail}`);
            }
        }
        
        // ==================== FACEBOOK CONVERSIONS API EVENTS ====================
        
        // User data for Facebook CAPI
        const fbUserData = {
            email: buyerEmail,
            phone: buyerPhone,
            firstName: buyerName
        };
        
        // Custom data for Facebook CAPI
        const fbCustomData = {
            content_name: productName,
            content_ids: [productCode || chave_unica],
            content_type: 'product',
            value: parseFloat(transactionValue) || 0,
            currency: 'BRL'
        };
        
        try {
            // Status 5 = Abandono de Checkout -> InitiateCheckout event
            if (status === '5') {
                console.log('📤 Sending InitiateCheckout to Facebook CAPI...');
                await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData);
            }
            
            // Status 1 = Aguardando pagamento -> Also InitiateCheckout (they started checkout)
            if (status === '1') {
                console.log('📤 Sending InitiateCheckout (pending) to Facebook CAPI...');
                await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData);
            }
            
            // Status 2 = Aprovada -> Purchase event
            if (status === '2') {
                console.log('📤 Sending Purchase to Facebook CAPI...');
                await sendToFacebookCAPI('Purchase', fbUserData, fbCustomData);
            }
            
            // Status 4 = Refund -> Refund event (custom)
            if (status === '4') {
                console.log('📤 Sending Refund to Facebook CAPI...');
                await sendToFacebookCAPI('Refund', fbUserData, fbCustomData);
            }
            
        } catch (capiError) {
            console.error('CAPI error (non-blocking):', capiError.message);
        }
        
        // Return success (Monetizze expects 200 OK)
        res.status(200).send('OK');
        
    } catch (error) {
        console.error('❌ Postback error:', error);
        // Still return 200 to prevent Monetizze from retrying
        res.status(200).send('OK');
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
            protocol
        } = req.body;

        // Validation
        if (!email || !fullName || !reason) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        const ipAddress = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        const userAgent = req.headers['user-agent'] || null;

        // Store refund request
        await pool.query(`
            INSERT INTO refund_requests (
                protocol, full_name, email, phone, country_code,
                purchase_date, product, reason, details,
                ip_address, user_agent, status, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending', NOW())
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
            userAgent
        ]);

        console.log(`📥 Refund request received: ${protocol} - ${email} - ${product}`);

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

// Get all refund requests (protected)
app.get('/api/admin/refunds', authenticateToken, async (req, res) => {
    try {
        const { status } = req.query;
        
        let query = `SELECT * FROM refund_requests`;
        let params = [];
        
        if (status) {
            query += ` WHERE status = $1`;
            params = [status];
        }
        
        query += ` ORDER BY created_at DESC LIMIT 100`;
        
        const result = await pool.query(query, params);

        res.json({ refunds: result.rows, total: result.rows.length });

    } catch (error) {
        console.error('Error fetching refunds:', error);
        res.status(500).json({ error: 'Failed to fetch refund requests' });
    }
});

// Update refund status (protected)
app.put('/api/admin/refunds/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { status, notes } = req.body;

        const result = await pool.query(`
            UPDATE refund_requests 
            SET status = $1, admin_notes = $2, updated_at = NOW()
            WHERE id = $3
            RETURNING *
        `, [status, notes, id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Refund request not found' });
        }

        res.json({ success: true, refund: result.rows[0] });

    } catch (error) {
        console.error('Error updating refund:', error);
        res.status(500).json({ error: 'Failed to update refund request' });
    }
});

// Get transactions (protected)
app.get('/api/admin/transactions', authenticateToken, async (req, res) => {
    try {
        const { language } = req.query;
        
        let query = `SELECT * FROM transactions`;
        let params = [];
        
        if (language === 'en' || language === 'es') {
            query += ` WHERE (funnel_language = $1 OR (funnel_language IS NULL AND $1 = 'en'))`;
            params = [language];
        }
        
        query += ` ORDER BY created_at DESC LIMIT 100`;
        
        const result = await pool.query(query, params);
        
        res.json({ transactions: result.rows, language: language || 'all' });
        
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
        const { language } = req.query;
        
        // Build language filter
        let langCondition = '';
        let langParams = [];
        if (language === 'en' || language === 'es') {
            langCondition = `AND (funnel_language = $1 OR (funnel_language IS NULL AND $1 = 'en'))`;
            langParams = [language];
        }
        
        const [totalResult, approvedResult, refundedResult, revenueResult] = await Promise.all([
            pool.query(`SELECT COUNT(*) FROM transactions WHERE 1=1 ${langCondition}`, langParams),
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' ${langCondition}`, langParams),
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status IN ('refunded', 'chargeback') ${langCondition}`, langParams),
            pool.query(`SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved' ${langCondition}`, langParams)
        ]);
        
        // Get today and this week
        const [todayResult, weekResult] = await Promise.all([
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' AND created_at >= CURRENT_DATE ${langCondition}`, langParams),
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' AND created_at >= CURRENT_DATE - INTERVAL '7 days' ${langCondition}`, langParams)
        ]);
        
        // Calculate conversion rate (leads -> sales) - also filtered by language
        const leadsCount = await pool.query(`SELECT COUNT(*) FROM leads WHERE 1=1 ${language ? `AND (funnel_language = $1 OR (funnel_language IS NULL AND $1 = 'en'))` : ''}`, langParams);
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
            WHERE product IS NOT NULL ${langCondition}
            GROUP BY product
            ORDER BY approved DESC
        `, langParams);
        
        // Calculate upsell take rates based on language
        // English products
        const enFrontKeywords = "product ILIKE '%Monitor%' OR product ILIKE '%341972%'";
        const enUp1Keywords = "product ILIKE '%Message Vault%' OR product ILIKE '%349241%'";
        const enUp2Keywords = "product ILIKE '%360%' OR product ILIKE '%Tracker%' OR product ILIKE '%349242%'";
        const enUp3Keywords = "product ILIKE '%Instant Access%' OR product ILIKE '%349243%'";
        
        // Spanish products
        const esFrontKeywords = "product ILIKE '%Infidelidad%' OR product ILIKE '%455963%' OR product ILIKE '%455971%'";
        const esUp1Keywords = "product ILIKE '%Recuperación%' OR product ILIKE '%455966%'";
        const esUp2Keywords = "product ILIKE '%Visión Total%' OR product ILIKE '%455968%'";
        const esUp3Keywords = "product ILIKE '%VIP Sin Esperas%' OR product ILIKE '%455970%'";
        
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
            WHERE status = 'approved' AND (${frontKeywords}) ${langCondition}
        `, langParams);
        
        const upsell1Sales = await pool.query(`
            SELECT COUNT(DISTINCT email) as count 
            FROM transactions 
            WHERE status = 'approved' AND (${up1Keywords}) ${langCondition}
        `, langParams);
        
        const upsell2Sales = await pool.query(`
            SELECT COUNT(DISTINCT email) as count 
            FROM transactions 
            WHERE status = 'approved' AND (${up2Keywords}) ${langCondition}
        `, langParams);
        
        const upsell3Sales = await pool.query(`
            SELECT COUNT(DISTINCT email) as count 
            FROM transactions 
            WHERE status = 'approved' AND (${up3Keywords}) ${langCondition}
        `, langParams);
        
        const frontCount = parseInt(frontSales.rows[0].count) || 0;
        const up1Count = parseInt(upsell1Sales.rows[0].count) || 0;
        const up2Count = parseInt(upsell2Sales.rows[0].count) || 0;
        const up3Count = parseInt(upsell3Sales.rows[0].count) || 0;
        
        res.json({
            total: parseInt(totalResult.rows[0].count),
            approved: parseInt(approvedResult.rows[0].count),
            refunded: parseInt(refundedResult.rows[0].count),
            revenue: parseFloat(revenueResult.rows[0].total) || 0,
            today: parseInt(todayResult.rows[0].count),
            thisWeek: parseInt(weekResult.rows[0].count),
            conversionRate: parseFloat(conversionRate),
            byProduct: productStats.rows,
            language: language || 'all',
            upsellStats: {
                front: frontCount,
                upsell1: up1Count,
                upsell2: up2Count,
                upsell3: up3Count,
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
        
        // Add customer journey tracking columns
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS visitor_id VARCHAR(100);`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS products_purchased TEXT[];`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS total_spent DECIMAL(10,2) DEFAULT 0;`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS first_purchase_at TIMESTAMP WITH TIME ZONE;`);
        await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS last_purchase_at TIMESTAMP WITH TIME ZONE;`);
        
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
        
        // Create indexes for funnel_events
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_funnel_visitor ON funnel_events(visitor_id);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_funnel_event ON funnel_events(event);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_funnel_created ON funnel_events(created_at DESC);`);
        
        // Create indexes for leads
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_leads_created_at ON leads(created_at DESC);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);`);
        
        // Add funnel_language to transactions if not exists
        await pool.query(`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS funnel_language VARCHAR(10);`);
        
        // Create indexes for transactions
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_transactions_email ON transactions(email);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions(created_at DESC);`);
        
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
        
        console.log('✅ Database ready');
    } catch (error) {
        console.error('❌ Database init error:', error.message);
    }
}

// Start server
app.listen(PORT, async () => {
    console.log(`🚀 ZapSpy API running on port ${PORT}`);
    console.log(`📊 Admin panel: http://localhost:${PORT}/admin`);
    
    // Initialize database
    await initDatabase();
});
