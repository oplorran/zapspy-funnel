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
app.use(express.urlencoded({ extended: true }));
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
            const url = `http://ip-api.com/json/${cleanIP}?fields=status,country,countryCode,city`;
            
            http.get(url, (res) => {
                let data = '';
                
                res.on('data', (chunk) => {
                    data += chunk;
                });
                
                res.on('end', () => {
                    try {
                        const json = JSON.parse(data);
                        
                        if (json.status === 'success') {
                            console.log('Geolocation: Found -', json.country, json.countryCode, json.city);
                            resolve({
                                country: json.country || null,
                                country_code: json.countryCode || null,
                                city: json.city || null
                            });
                        } else {
                            console.log('Geolocation: API returned fail status');
                            resolve({ country: null, country_code: null, city: null });
                        }
                    } catch (parseError) {
                        console.log('Geolocation parse error:', parseError.message);
                        resolve({ country: null, country_code: null, city: null });
                    }
                });
            }).on('error', (error) => {
                console.log('Geolocation request error:', error.message);
                resolve({ country: null, country_code: null, city: null });
            });
            
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

// Database diagnostic endpoint (protected - admin only)
app.get('/api/health/db', authenticateToken, async (req, res) => {
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
                    funnel_source = COALESCE($12, funnel_source),
                    utm_source = COALESCE($14, utm_source),
                    utm_medium = COALESCE($15, utm_medium),
                    utm_campaign = COALESCE($16, utm_campaign),
                    utm_content = COALESCE($17, utm_content),
                    utm_term = COALESCE($18, utm_term),
                    last_visit_at = NOW(),
                    updated_at = NOW()
                WHERE id = $13
                RETURNING id, created_at`,
                [name || null, targetPhone || null, targetGender || null, ipAddress, referrer || null, ua || null, currentVisitCount + 1, geoData.country, geoData.country_code, geoData.city, visitorId || null, source, existingLead.rows[0].id, utm_source || null, utm_medium || null, utm_campaign || null, utm_content || null, utm_term || null]
            );
            console.log(`Returning lead [${language.toUpperCase()}/${source}]: ${name || 'No name'} - ${email} - ${geoData.country || 'Unknown'} (visit #${currentVisitCount + 1})`);
        } else {
            // Insert new lead
            result = await pool.query(
                `INSERT INTO leads (name, email, whatsapp, target_phone, target_gender, ip_address, referrer, user_agent, funnel_language, funnel_source, visit_count, country, country_code, city, visitor_id, utm_source, utm_medium, utm_campaign, utm_content, utm_term, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, $11, $12, $13, $14, $15, $16, $17, $18, $19, NOW())
                 RETURNING id, created_at`,
                [name || null, email, whatsapp, targetPhone || null, targetGender || null, ipAddress, referrer || null, ua || null, language, source, geoData.country, geoData.country_code, geoData.city, visitorId || null, utm_source || null, utm_medium || null, utm_campaign || null, utm_content || null, utm_term || null]
            );
            isNewLead = true;
            console.log(`New lead captured [${language.toUpperCase()}/${source}]: ${name || 'No name'} - ${email} - ${whatsapp} - ${geoData.country || 'Unknown'}${utm_source ? ` [UTM: ${utm_source}]` : ''}`);
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
        const { name, role, is_active, password } = req.body;
        
        // Don't allow modifying the main admin (id = 1)
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

// Get flexible period comparison (week, month, quarter)
app.get('/api/admin/stats/period-comparison', authenticateToken, async (req, res) => {
    try {
        const { type = 'week' } = req.query;
        
        // Define intervals based on period type
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
        const currentLeads = await pool.query(`
            SELECT COUNT(*) FROM leads 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${currentInterval}')::date
        `);
        
        const currentSales = await pool.query(`
            SELECT COUNT(*), COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
            FROM transactions 
            WHERE status = 'approved' 
            AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${currentInterval}')::date
        `);
        
        // Previous period stats
        const previousLeads = await pool.query(`
            SELECT COUNT(*) FROM leads 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${previousInterval}')::date 
            AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date < ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${currentInterval}')::date
        `);
        
        const previousSales = await pool.query(`
            SELECT COUNT(*), COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
            FROM transactions 
            WHERE status = 'approved' 
            AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${previousInterval}')::date
            AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date < ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '${currentInterval}')::date
        `);
        
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

// ==================== WHATSAPP VERIFICATION ====================
// Z-API credentials (same as funnel)
const ZAPI_INSTANCE = '3E7938F228CBB0978267A6F61CAAA8C7';
const ZAPI_TOKEN = '983F7A4EF1F159FAD3C42B05';
const ZAPI_CLIENT_TOKEN = 'F0f2cc62f6c4f46088783537c957b7fd6S';

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
        let phone = lead.whatsapp || lead.phone || '';
        
        if (!phone) {
            return res.status(400).json({ error: 'No phone number available', verified: false });
        }
        
        // Clean phone number - remove all non-digits
        phone = phone.replace(/\D/g, '');
        
        // Check if number exists on WhatsApp via Z-API
        try {
            const response = await fetch(`https://api.z-api.io/instances/${ZAPI_INSTANCE}/token/${ZAPI_TOKEN}/phone-exists/${phone}`, {
                headers: { 'client-token': ZAPI_CLIENT_TOKEN }
            });
            
            const data = await response.json();
            const isRegistered = data.exists === true;
            
            // Try to get profile picture if registered
            let profilePicture = null;
            if (isRegistered) {
                try {
                    const picResponse = await fetch(`https://api.z-api.io/instances/${ZAPI_INSTANCE}/token/${ZAPI_TOKEN}/profile-picture?phone=${phone}`, {
                        headers: { 'client-token': ZAPI_CLIENT_TOKEN }
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
            'SELECT id, whatsapp, phone FROM leads WHERE id = ANY($1)',
            [limitedIds]
        );
        
        const results = [];
        
        for (const lead of leadsResult.rows) {
            let phone = lead.whatsapp || lead.phone || '';
            if (!phone) {
                results.push({ id: lead.id, verified: false, error: 'No phone' });
                continue;
            }
            
            phone = phone.replace(/\D/g, '');
            
            try {
                const response = await fetch(`https://api.z-api.io/instances/${ZAPI_INSTANCE}/token/${ZAPI_TOKEN}/phone-exists/${phone}`, {
                    headers: { 'client-token': ZAPI_CLIENT_TOKEN }
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
            }
        }
        
        res.json({ 
            success: true, 
            message: `Recalculated ${updatedCount} leads from ${transactionsResult.rows.length} buyer emails`,
            updated: updatedCount,
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
            funnelSource,    // 'main' or 'affiliate'
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

// Debug endpoint to see recent postbacks (memory + DB)
app.get('/api/admin/debug/postbacks', authenticateToken, async (req, res) => {
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
    
    // Handle pagination
    const totalPages = data.paginas || 1;
    if (totalPages > 1) {
        for (let page = 2; page <= totalPages; page++) {
            try {
                const pageParams = new URLSearchParams(params.toString());
                pageParams.set('pagina', String(page));
                const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                    method: 'GET',
                    headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                });
                if (pageResponse.ok) {
                    const pageData = await pageResponse.json();
                    salesArray = salesArray.concat(pageData.dados || pageData.vendas || []);
                }
            } catch (pageError) {
                console.error(`❌ Error fetching page ${page}:`, pageError.message);
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
            
            if (vendaStatus.includes('cancelada') || vendaStatus.includes('cancel')) {
                mappedStatus = 'cancelled';
            } else if (vendaStatus.includes('aguardando') || vendaStatus.includes('pending')) {
                mappedStatus = 'pending_payment';
            } else if (vendaStatus.includes('finalizada') || vendaStatus.includes('aprovada')) {
                // Only mark as approved if dataFinalizada is valid
                if (isFinalized) {
                    mappedStatus = 'approved';
                } else {
                    mappedStatus = 'pending_payment';
                }
            } else if (!isFinalized && statusCode === '2') {
                // Status code says approved but no valid dataFinalizada
                mappedStatus = 'pending_payment';
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
    } catch (error) {
        console.error('❌ Auto-sync error:', error.message);
    }
}

function startAutoSync() {
    // Run first sync 30 seconds after server start (let DB initialize first)
    setTimeout(() => {
        runAutoSync();
        // Then run every 30 minutes
        autoSyncInterval = setInterval(runAutoSync, 30 * 60 * 1000);
        console.log('🔄 Auto-sync scheduled: every 30 minutes');
    }, 30000);
}

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
        // 1=Aguardando, 2=Finalizada, 3=Cancelada, 4=Devolvida, 5=Bloqueada, 6=Completa
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
        
        // Handle pagination - fetch all pages
        const totalPages = data.paginas || 1;
        const totalRecords = data.registros || salesArray.length;
        
        if (totalPages > 1) {
            console.log(`📄 Pagination detected: ${totalPages} pages, ${totalRecords} total records`);
            for (let page = 2; page <= totalPages; page++) {
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
                    }
                } catch (pageError) {
                    console.error(`❌ Error fetching page ${page}:`, pageError.message);
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
                
                if (vendaStatus.includes('cancelada') || vendaStatus.includes('cancel')) {
                    mappedStatus = 'cancelled';
                } else if (vendaStatus.includes('aguardando') || vendaStatus.includes('pending')) {
                    mappedStatus = 'pending_payment';
                } else if (vendaStatus.includes('finalizada') || vendaStatus.includes('aprovada')) {
                    // Only mark as approved if dataFinalizada is valid
                    if (isFinalized) {
                        mappedStatus = 'approved';
                    } else {
                        mappedStatus = 'pending_payment';
                    }
                } else if (!isFinalized && statusCode === '2') {
                    // Status code says approved but no valid dataFinalizada
                    mappedStatus = 'pending_payment';
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
        
        // Use commission value if available, otherwise fall back to other values
        const valor = comissao || valorLiquido || valorRecebido || valorBruto;
        
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
        // Check dataFinalizada - if it's "0000-00-00" or empty, it's not finalized
        const dataFinalizada = venda.dataFinalizada || '';
        const isFinalized = dataFinalizada && 
                           dataFinalizada !== '0000-00-00 00:00:00' && 
                           dataFinalizada !== '0000-00-00' &&
                           !dataFinalizada.startsWith('0000-00-00');
        
        // Check venda.status text for the REAL status
        const vendaStatus = (venda.status || '').toLowerCase();
        
        if (vendaStatus.includes('cancelada') || vendaStatus.includes('cancel')) {
            finalStatus = 'cancelled';
        } else if (vendaStatus.includes('aguardando') || vendaStatus.includes('pending')) {
            finalStatus = 'pending_payment';
        } else if (vendaStatus.includes('finalizada') || vendaStatus.includes('aprovada')) {
            // Only mark as approved if dataFinalizada is valid
            if (isFinalized) {
                finalStatus = 'approved';
            } else {
                finalStatus = 'pending_payment';
            }
        } else if (!isFinalized && statusCode === '2') {
            // Status code says approved but no valid dataFinalizada
            finalStatus = 'pending_payment';
        }
        
        const mappedStatus = finalStatus; // Use finalStatus which includes all detection logic
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
        
        // Store transaction in database with funnel_language and funnel_source
        try {
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
                saleDate
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
            }
        }
        
        // ==================== FACEBOOK CONVERSIONS API EVENTS ====================
        
        // User data for Facebook CAPI
        const fbUserData = {
            email: finalEmail || buyerEmail,
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
            const statusStr = String(statusCode);
            
            // Options with language for correct pixel selection
            const capiOptions = { language: funnelLanguage };
            
            // Status 7 = Abandono de Checkout -> InitiateCheckout event
            if (statusStr === '7') {
                console.log(`📤 Sending InitiateCheckout to Facebook CAPI (${funnelLanguage})...`);
                await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData, null, null, capiOptions);
            }
            
            // Status 1 = Aguardando pagamento -> Also InitiateCheckout (they started checkout)
            if (statusStr === '1') {
                console.log(`📤 Sending InitiateCheckout (pending) to Facebook CAPI (${funnelLanguage})...`);
                await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData, null, null, capiOptions);
            }
            
            // Status 2 or 6 = Aprovada/Completa -> Purchase event
            // BUT ONLY if dataFinalizada is valid (not "0000-00-00")
            if ((statusStr === '2' || statusStr === '6') && isFinalized) {
                console.log(`📤 Sending Purchase to Facebook CAPI (${funnelLanguage}) - payment confirmed...`);
                await sendToFacebookCAPI('Purchase', fbUserData, fbCustomData, null, null, capiOptions);
            } else if (statusStr === '2' && !isFinalized) {
                // Status 2 but no valid dataFinalizada = still pending payment
                console.log('⏸️ Skipping Purchase event - payment not yet confirmed (invalid dataFinalizada)');
                console.log(`📤 Sending InitiateCheckout instead (${funnelLanguage})...`);
                await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData, null, null, capiOptions);
            }
            
            // Status 3 = Cancelled -> Send custom Cancel event
            if (statusStr === '3' || finalStatus === 'cancelled') {
                console.log('📤 Transaction cancelled - no Facebook event sent');
            }
            
            // Status 4 = Refund -> Refund event (custom)
            if (statusStr === '4' || finalStatus === 'refunded') {
                console.log(`📤 Sending Refund to Facebook CAPI (${funnelLanguage})...`);
                await sendToFacebookCAPI('Refund', fbUserData, fbCustomData, null, null, capiOptions);
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
        
        // Build date filter
        let dateFilter = '';
        let dateParams = [];
        if (startDate && endDate) {
            dateFilter = `AND created_at >= $1 AND created_at <= $2`;
            dateParams = [startDate, endDate + ' 23:59:59'];
        }
        
        // 1. Checkout Abandoned - People who clicked checkout but don't have approved transaction
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
            ${dateFilter.replace(/\$1/g, `$${dateParams.length > 0 ? 1 : 1}`).replace(/\$2/g, `$${dateParams.length > 0 ? 2 : 2}`)}
        `, dateParams);
        
        // 2. Payment Failed - Transactions with failed status
        const paymentFailed = await pool.query(`
            SELECT COUNT(*) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total_value
            FROM transactions
            WHERE status IN ('cancelled', 'refused', 'pending', 'waiting_payment')
            ${language ? `AND funnel_language = '${language}'` : ''}
            ${dateFilter}
        `, dateParams);
        
        // 3. Refund Requests - Current refund requests that are pending or handling
        const refundRequests = await pool.query(`
            SELECT COUNT(*) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total_value
            FROM refund_requests
            WHERE status IN ('pending', 'handling', 'processing')
            ${language ? `AND funnel_language = '${language}'` : ''}
            ${dateFilter}
        `, dateParams);
        
        // 4. Upsell Declined - People who bought front but declined upsells
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
            ${dateFilter}
        `, dateParams);
        
        // Calculate potential values
        const frontPrice = 47;
        const upsellPrice = 67;
        
        res.json({
            segments: {
                checkout_abandoned: {
                    count: parseInt(checkoutAbandoned.rows[0]?.count || 0),
                    potential_value: parseInt(checkoutAbandoned.rows[0]?.count || 0) * frontPrice,
                    label: 'Abandonos Checkout',
                    icon: '🛒',
                    color: '#f59e0b'
                },
                payment_failed: {
                    count: parseInt(paymentFailed.rows[0]?.count || 0),
                    potential_value: parseFloat(paymentFailed.rows[0]?.total_value || 0),
                    label: 'Pagamentos Recusados',
                    icon: '💳',
                    color: '#ef4444'
                },
                refund_requests: {
                    count: parseInt(refundRequests.rows[0]?.count || 0),
                    potential_value: parseFloat(refundRequests.rows[0]?.total_value || 0),
                    label: 'Pedidos Reembolso',
                    icon: '💸',
                    color: '#8b5cf6'
                },
                upsell_declined: {
                    count: parseInt(upsellDeclined.rows[0]?.count || 0),
                    potential_value: parseInt(upsellDeclined.rows[0]?.count || 0) * upsellPrice,
                    label: 'Recusas Upsell',
                    icon: '📦',
                    color: '#3b82f6'
                }
            },
            totals: {
                count: parseInt(checkoutAbandoned.rows[0]?.count || 0) + 
                       parseInt(paymentFailed.rows[0]?.count || 0) + 
                       parseInt(refundRequests.rows[0]?.count || 0) + 
                       parseInt(upsellDeclined.rows[0]?.count || 0),
                potential_value: (parseInt(checkoutAbandoned.rows[0]?.count || 0) * frontPrice) +
                                parseFloat(paymentFailed.rows[0]?.total_value || 0) +
                                parseFloat(refundRequests.rows[0]?.total_value || 0) +
                                (parseInt(upsellDeclined.rows[0]?.count || 0) * upsellPrice)
            }
        });
        
    } catch (error) {
        console.error('Error fetching recovery segments:', error);
        res.status(500).json({ error: 'Failed to fetch recovery segments' });
    }
});

// Get leads for a specific recovery segment
app.get('/api/admin/recovery/:segment', authenticateToken, async (req, res) => {
    try {
        const { segment } = req.params;
        const { language, startDate, endDate, minScore, contactStatus, sortBy, page = 1, limit = 20 } = req.query;
        const offset = (parseInt(page) - 1) * parseInt(limit);
        
        let leads = [];
        let totalCount = 0;
        
        // Build common date filter
        let dateFilter = '';
        if (startDate && endDate) {
            dateFilter = `AND created_at >= '${startDate}' AND created_at <= '${endDate} 23:59:59'`;
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
        
        if (segment === 'checkout_abandoned') {
            // Checkout abandoned leads - simplified query
            const result = await pool.query(`
                SELECT DISTINCT ON (COALESCE(l.email, fe.visitor_id))
                    COALESCE(l.id, 0) as id,
                    COALESCE(l.email, '') as email,
                    COALESCE(l.name, 'Visitante') as name,
                    COALESCE(l.whatsapp, '') as phone,
                    COALESCE(l.country, '') as country,
                    COALESCE(l.country_code, '') as country_code,
                    l.funnel_language as language,
                    fe.created_at as last_event_at,
                    'checkout_clicked' as event,
                    47.00 as potential_value,
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
                ${dateFilter}
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
                ${dateFilter}
            `);
            totalCount = parseInt(countResult.rows[0]?.count || 0);
            
        } else if (segment === 'payment_failed') {
            // Payment failed leads - simplified query
            const result = await pool.query(`
                SELECT 
                    t.id,
                    t.email,
                    t.name,
                    COALESCE(l.whatsapp, t.phone, '') as phone,
                    COALESCE(l.country, '') as country,
                    COALESCE(l.country_code, '') as country_code,
                    t.funnel_language as language,
                    t.created_at as last_event_at,
                    t.status as event,
                    CAST(t.value AS DECIMAL) as potential_value,
                    t.product,
                    1 as event_count,
                    false as has_purchase,
                    0 as contact_attempts,
                    NULL as last_contact
                FROM transactions t
                LEFT JOIN leads l ON LOWER(t.email) = LOWER(l.email)
                WHERE t.status IN ('cancelled', 'refused', 'pending', 'waiting_payment')
                ${language ? `AND t.funnel_language = '${language}'` : ''}
                ${dateFilter}
                ORDER BY t.created_at DESC
                LIMIT $1 OFFSET $2
            `, [parseInt(limit), offset]);
            
            leads = result.rows;
            
            const countResult = await pool.query(`
                SELECT COUNT(*) as count FROM transactions
                WHERE status IN ('cancelled', 'refused', 'pending', 'waiting_payment')
                ${language ? `AND funnel_language = '${language}'` : ''}
                ${dateFilter}
            `);
            totalCount = parseInt(countResult.rows[0]?.count || 0);
            
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
                ${dateFilter}
                ORDER BY r.created_at DESC
                LIMIT $1 OFFSET $2
            `, [parseInt(limit), offset]);
            
            leads = result.rows;
            
            const countResult = await pool.query(`
                SELECT COUNT(*) as count FROM refund_requests
                WHERE status IN ('pending', 'handling', 'processing')
                ${language ? `AND funnel_language = '${language}'` : ''}
                ${dateFilter}
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
                ${dateFilter}
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
                ${dateFilter}
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

// Get recovery templates
app.get('/api/admin/recovery/templates', authenticateToken, async (req, res) => {
    try {
        const templates = {
            checkout_abandoned: [
                {
                    id: 'urgency',
                    name: 'Urgência',
                    icon: '⏰',
                    message_en: "Hey {name}! 👋 I noticed you were checking out X AI Monitor but didn't complete your purchase. Just wanted to let you know we have LIMITED spots available. Don't miss out on discovering what's happening behind the scenes! 🔥",
                    message_es: "¡Hola {name}! 👋 Vi que estabas por comprar X AI Monitor pero no completaste. Solo quería avisarte que tenemos CUPOS LIMITADOS. ¡No te pierdas la oportunidad de descubrir qué está pasando! 🔥",
                    message_pt: "Oi {name}! 👋 Vi que você estava prestes a comprar o X AI Monitor mas não finalizou. Só queria avisar que temos VAGAS LIMITADAS. Não perca a chance de descobrir o que está acontecendo! 🔥"
                },
                {
                    id: 'discount',
                    name: 'Desconto Especial',
                    icon: '💰',
                    message_en: "Hi {name}! 🎁 I have a special offer just for you: Get 50% OFF on X AI Monitor for the next 24 hours! Use this exclusive link: [LINK]. Don't let this opportunity slip away!",
                    message_es: "¡Hola {name}! 🎁 Tengo una oferta especial solo para ti: ¡50% DE DESCUENTO en X AI Monitor por las próximas 24 horas! Usa este link exclusivo: [LINK]. ¡No dejes escapar esta oportunidad!",
                    message_pt: "Oi {name}! 🎁 Tenho uma oferta especial só pra você: 50% DE DESCONTO no X AI Monitor pelas próximas 24 horas! Use este link exclusivo: [LINK]. Não deixe essa oportunidade passar!"
                },
                {
                    id: 'support',
                    name: 'Suporte',
                    icon: '🤝',
                    message_en: "Hey {name}! 👋 I noticed you were interested in X AI Monitor. Is there anything I can help you with? Any questions about how it works? I'm here to help! 😊",
                    message_es: "¡Hola {name}! 👋 Vi que te interesó X AI Monitor. ¿Hay algo en lo que pueda ayudarte? ¿Alguna pregunta sobre cómo funciona? ¡Estoy aquí para ayudar! 😊",
                    message_pt: "Oi {name}! 👋 Vi que você se interessou pelo X AI Monitor. Tem algo que eu possa te ajudar? Alguma dúvida sobre como funciona? Estou aqui pra ajudar! 😊"
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

// Get recovery stats summary
app.get('/api/admin/recovery/stats', authenticateToken, async (req, res) => {
    try {
        // Ensure table exists
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
        
        // Get best hour for contact (based on conversions)
        const bestHour = await pool.query(`
            SELECT EXTRACT(HOUR FROM created_at) as hour, COUNT(*) as count
            FROM recovery_contacts
            WHERE status = 'converted'
            GROUP BY hour
            ORDER BY count DESC
            LIMIT 1
        `);
        
        const bestContactHour = bestHour.rows[0]?.hour ? `${bestHour.rows[0].hour}:00` : '10:00';
        
        res.json({
            recovery_rate: recoveryRate,
            total_contacts: total,
            total_converted: converted,
            best_contact_hour: bestContactHour
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
        
        const result = await pool.query(`
            SELECT * FROM refund_requests 
            ${whereClause}
            ORDER BY created_at DESC 
            LIMIT 100
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
        const { language, startDate, endDate, source, page = 1, limit = 10 } = req.query;
        
        const pageNum = parseInt(page) || 1;
        const limitNum = parseInt(limit) || 10;
        const offset = (pageNum - 1) * limitNum;
        
        let baseQuery = `FROM transactions WHERE 1=1`;
        let params = [];
        let paramIndex = 1;
        
        // Exclude test transactions
        baseQuery += ` AND transaction_id NOT LIKE 'TEST%' AND transaction_id NOT LIKE '%TEST%'`;
        baseQuery += ` AND email NOT LIKE '%test%@%' AND email NOT LIKE '%@test.%'`;
        
        if (language === 'en' || language === 'es') {
            baseQuery += ` AND (funnel_language = $${paramIndex} OR (funnel_language IS NULL AND $${paramIndex} = 'en'))`;
            params.push(language);
            paramIndex++;
        }
        
        // Filter by funnel source (main/affiliate)
        if (source === 'main' || source === 'affiliate') {
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
        
        // Build source filter (main/affiliate)
        let sourceCondition = '';
        if (source === 'main' || source === 'affiliate') {
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
            // Count unique customers (not total transactions) for more accurate metrics
            pool.query(`SELECT COUNT(DISTINCT email) FROM transactions WHERE 1=1 ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(DISTINCT email) FROM transactions WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}`, langParams),
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
        
        // Get today and this week (using Brazil timezone) - count unique customers
        const [todayResult, weekResult] = await Promise.all([
            pool.query(`SELECT COUNT(DISTINCT email) FROM transactions WHERE status = 'approved' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(DISTINCT email) FROM transactions WHERE status = 'approved' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '7 days')::date ${langCondition}${sourceCondition}${dateCondition}`, langParams)
        ]);
        
        // Calculate checkout abandonment (clicked checkout but no approved transaction)
        // Build language condition for funnel_events
        let funnelLangCondition = '';
        if (language === 'en' || language === 'es') {
            funnelLangCondition = ` AND (metadata->>'funnelLanguage' = '${language}' OR (metadata->>'funnelLanguage' IS NULL AND '${language}' = 'en'))`;
        }
        
        // Build source condition for funnel_events
        let funnelSourceCondition = '';
        if (source === 'main' || source === 'affiliate') {
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
        
        // Count checkout abandonment more accurately:
        // Visitors who clicked checkout but have NO payment attempt at all (not even rejected)
        // People with rejected cards TRIED to pay, so they are NOT abandoned
        const checkoutAbandonedResult = await pool.query(`
            SELECT COUNT(DISTINCT fe.visitor_id) as count
            FROM funnel_events fe
            LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
            WHERE fe.event = 'checkout_clicked'
            ${funnelLangCondition.replace(/metadata/g, 'fe.metadata')}
            ${funnelSourceCondition.replace(/metadata/g, 'fe.metadata')}
            ${funnelDateCondition.replace(/created_at/g, 'fe.created_at')}
            AND (
                l.email IS NULL 
                OR NOT EXISTS (
                    SELECT 1 FROM transactions t 
                    WHERE LOWER(t.email) = LOWER(l.email)
                    AND t.status IN ('approved', 'cancelled', 'refused', 'rejected', 'pending_payment', 'blocked', 'waiting_payment', 'refunded', 'chargeback')
                )
            )
        `);
        
        // Count unique emails with approved transactions (for other metrics)
        const approvedEmailsResult = await pool.query(`
            SELECT COUNT(DISTINCT LOWER(email)) as count 
            FROM transactions 
            WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}
        `, langParams);
        
        const checkoutClicked = parseInt(checkoutClickedResult.rows[0].count) || 0;
        const checkoutAbandoned = parseInt(checkoutAbandonedResult.rows[0].count) || 0;
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
        if (source === 'main' || source === 'affiliate') {
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
    
    // Initialize database
    await initDatabase();
    
    // Start auto-sync with Monetizze (every 30 minutes)
    startAutoSync();
});
