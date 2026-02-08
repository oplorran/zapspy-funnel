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
// Uses date range with < end+1day to include the full end date
function buildDateFilter(startDate, endDate, columnName = 'created_at') {
    if (!startDate || !endDate) return { sql: '', params: [] };
    return {
        sql: ` AND ${columnName} >= $PARAM_START::date AND ${columnName} < ($PARAM_END::date + INTERVAL '1 day')`,
        params: [startDate, endDate]
    };
}

// ==================== PUBLIC API ROUTES ====================

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Database diagnostic endpoint (public for debugging)
app.get('/api/health/db', async (req, res) => {
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
            SELECT id, username, email, name, role, is_active, last_login, created_at
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
        
        // Check if user already exists
        const existing = await pool.query(
            'SELECT id FROM admin_users WHERE email = $1 OR username = $2',
            [email, username]
        );
        
        if (existing.rows.length > 0) {
            return res.status(409).json({ error: 'User with this email or username already exists' });
        }
        
        // Hash password
        const hashedPassword = await bcrypt.hash(password, 10);
        
        // Insert new user
        const result = await pool.query(`
            INSERT INTO admin_users (username, email, password_hash, name, role, is_active, created_by)
            VALUES ($1, $2, $3, $4, $5, true, $6)
            RETURNING id, username, email, name, role, is_active, created_at
        `, [username, email, hashedPassword, name || username, userRole, req.user.userId]);
        
        console.log(`✅ New user created: ${username} (${role}) by admin ${req.user.email}`);
        
        res.json({ success: true, user: result.rows[0] });
    } catch (error) {
        console.error('Error creating user:', error);
        res.status(500).json({ error: 'Failed to create user' });
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
        
        if (startDate && endDate) {
            conditions.push(`created_at >= $${params.length + 1}::date AND created_at < ($${params.length + 2}::date + INTERVAL '1 day')`);
            params.push(startDate, endDate);
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
        
        // Build date filter
        let dateFilter = '';
        const params = [];
        if (startDate && endDate) {
            dateFilter = ` AND created_at >= $1::date AND created_at < ($2::date + INTERVAL '1 day')`;
            params.push(startDate, endDate);
        }
        
        const [totalResult, todayResult, weekResult, statusResult] = await Promise.all([
            pool.query(`SELECT COUNT(*) FROM leads WHERE 1=1${dateFilter}`, params),
            pool.query(`SELECT COUNT(*) FROM leads WHERE created_at >= CURRENT_DATE${dateFilter}`, params),
            pool.query(`SELECT COUNT(*) FROM leads WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'${dateFilter}`, params),
            pool.query(`SELECT status, COUNT(*) FROM leads WHERE 1=1${dateFilter} GROUP BY status`, params)
        ]);
        
        // Get leads by day for the last 7 days
        const dailyResult = await pool.query(`
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM leads
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'${dateFilter}
            GROUP BY DATE(created_at)
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
        // Current week stats
        const currentWeekLeads = await pool.query(`
            SELECT COUNT(*) FROM leads 
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
        `);
        
        const currentWeekSales = await pool.query(`
            SELECT COUNT(*), COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
            FROM transactions 
            WHERE status = 'approved' AND created_at >= CURRENT_DATE - INTERVAL '7 days'
        `);
        
        // Previous week stats
        const previousWeekLeads = await pool.query(`
            SELECT COUNT(*) FROM leads 
            WHERE created_at >= CURRENT_DATE - INTERVAL '14 days' 
            AND created_at < CURRENT_DATE - INTERVAL '7 days'
        `);
        
        const previousWeekSales = await pool.query(`
            SELECT COUNT(*), COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
            FROM transactions 
            WHERE status = 'approved' 
            AND created_at >= CURRENT_DATE - INTERVAL '14 days'
            AND created_at < CURRENT_DATE - INTERVAL '7 days'
        `);
        
        // Hourly heatmap data (for conversion optimization)
        const hourlyData = await pool.query(`
            SELECT 
                EXTRACT(HOUR FROM created_at) as hour,
                EXTRACT(DOW FROM created_at) as day_of_week,
                COUNT(*) as count
            FROM leads
            WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY EXTRACT(HOUR FROM created_at), EXTRACT(DOW FROM created_at)
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
        `);
        
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
        `);
        
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
        `);
        
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

// Store last 20 postbacks for debugging
const recentPostbacks = [];

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
        
        console.log('🔄 Starting Monetizze sync...');
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
                
                // Extract real sale date from Monetizze
                const saleDateStr = vendaData.dataInicio || vendaData.dataFinalizada || vendaData.dataVenda || vendaData.data || null;
                let saleDate = null;
                if (saleDateStr) {
                    try {
                        saleDate = new Date(saleDateStr);
                        if (isNaN(saleDate.getTime())) saleDate = null;
                    } catch (e) {
                        saleDate = null;
                    }
                }
                
                // Detect funnel language and source (main vs affiliate)
                const spanishCodes = ['349260', '349261', '349266', '349267', '338375', '341452', '341453', '341454'];
                const affiliateCodes = ['330254', '341443', '341444', '341448', '338375', '341452', '341453', '341454'];
                
                let funnelLanguage = spanishCodes.includes(String(productCode)) ? 'es' : 'en';
                const funnelSource = affiliateCodes.includes(String(productCode)) ? 'affiliate' : 'main';
                
                // Map status
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
                const mappedStatus = statusMap[String(statusCode)] || 'approved';
                
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
        
        // Sale date from venda.dataVenda or data - use real sale time, not NOW()
        const dataVenda = venda.dataVenda || body['venda.dataVenda'] || body['venda[dataVenda]'] || 
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
        
        const mappedStatus = finalStatus; // Use finalStatus which includes chargeback detection
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
        // dataVenda format from Monetizze: "2026-02-07 16:28:00" or ISO format
        let saleDate = null;
        if (dataVenda) {
            try {
                saleDate = new Date(dataVenda);
                if (isNaN(saleDate.getTime())) saleDate = null;
            } catch (e) {
                saleDate = null;
            }
        }
        console.log(`📅 Sale date: ${saleDate ? saleDate.toISOString() : 'Using NOW()'}`);
        
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
                    updated_at = NOW()
            `, [
                transactionId,
                finalEmail || buyerEmail,
                buyerPhone,
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
            
            // Status 7 = Abandono de Checkout -> InitiateCheckout event
            if (statusStr === '7') {
                console.log('📤 Sending InitiateCheckout to Facebook CAPI...');
                await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData);
            }
            
            // Status 1 = Aguardando pagamento -> Also InitiateCheckout (they started checkout)
            if (statusStr === '1') {
                console.log('📤 Sending InitiateCheckout (pending) to Facebook CAPI...');
                await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData);
            }
            
            // Status 2 or 6 = Aprovada/Completa -> Purchase event
            if (statusStr === '2' || statusStr === '6') {
                console.log('📤 Sending Purchase to Facebook CAPI...');
                await sendToFacebookCAPI('Purchase', fbUserData, fbCustomData);
            }
            
            // Status 4 = Refund -> Refund event (custom)
            if (statusStr === '4' || mappedStatus === 'refunded') {
                console.log('📤 Sending Refund to Facebook CAPI...');
                await sendToFacebookCAPI('Refund', fbUserData, fbCustomData);
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
            protocol
        } = req.body;

        // Validation
        if (!email || !fullName || !reason) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        const ipAddress = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        const userAgent = req.headers['user-agent'] || null;

        // ==================== CROSS-REFERENCE DATA ====================
        // Try to find this person in our leads and transactions to enrich the refund data
        let detectedLanguage = null;
        let detectedValue = null;
        let matchedTransactionId = null;
        
        try {
            // 1. Check transactions first (most reliable for language + value)
            const txResult = await pool.query(`
                SELECT transaction_id, value, funnel_language, product, status 
                FROM transactions 
                WHERE LOWER(email) = LOWER($1) AND status = 'approved'
                ORDER BY created_at DESC 
                LIMIT 1
            `, [email]);
            
            if (txResult.rows.length > 0) {
                const tx = txResult.rows[0];
                detectedLanguage = tx.funnel_language || null;
                detectedValue = tx.value || null;
                matchedTransactionId = tx.transaction_id || null;
                console.log(`🔗 Refund cross-ref: Found transaction for ${email} -> lang: ${detectedLanguage}, value: R$${detectedValue}, txId: ${matchedTransactionId}`);
            }
            
            // 2. If no language from transaction, check leads (funnel_events metadata)
            if (!detectedLanguage) {
                const leadResult = await pool.query(`
                    SELECT l.id, l.metadata
                    FROM leads l
                    WHERE LOWER(l.email) = LOWER($1)
                    ORDER BY l.created_at DESC 
                    LIMIT 1
                `, [email]);
                
                if (leadResult.rows.length > 0) {
                    const lead = leadResult.rows[0];
                    const metadata = lead.metadata || {};
                    detectedLanguage = metadata.funnelLanguage || null;
                    console.log(`🔗 Refund cross-ref: Found lead for ${email} -> lang: ${detectedLanguage}`);
                }
            }
            
            // 3. If still no language, check funnel_events for this email
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
                    console.log(`🔗 Refund cross-ref: Found funnel event for ${email} -> lang: ${detectedLanguage}`);
                }
            }
            
            console.log(`🔗 Refund cross-ref final: email=${email}, lang=${detectedLanguage || 'unknown'}, value=${detectedValue || 'unknown'}, txId=${matchedTransactionId || 'none'}`);
            
        } catch (crossRefError) {
            console.error('⚠️ Cross-reference error (non-blocking):', crossRefError.message);
        }

        // Store refund request with enriched data
        await pool.query(`
            INSERT INTO refund_requests (
                protocol, full_name, email, phone, country_code,
                purchase_date, product, reason, details,
                ip_address, user_agent, status, source, refund_type,
                funnel_language, value, transaction_id, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending', 'form', 'refund',
                $12, $13, $14, NOW())
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
            matchedTransactionId
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
            conditions.push(`created_at >= $${paramIndex++}::date AND created_at < ($${paramIndex++}::date + INTERVAL '1 day')`);
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
            statsConditions.push(`created_at >= $${statsParamIndex++}::date AND created_at < ($${statsParamIndex++}::date + INTERVAL '1 day')`);
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
        
        // Calculate totals with language breakdown
        const stats = {
            form: { pending: 0, processing: 0, approved: 0, rejected: 0, total: 0, en: 0, es: 0 },
            monetizze_refund: { pending: 0, processing: 0, approved: 0, rejected: 0, total: 0, en: 0, es: 0 },
            monetizze_chargeback: { pending: 0, processing: 0, approved: 0, rejected: 0, total: 0, en: 0, es: 0 }
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
                SELECT id, email, name, phone, country, status, source,
                    products_purchased, total_spent, first_purchase_at, last_purchase_at,
                    metadata, created_at
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
                    detectedLanguage: transactions[0]?.funnel_language || leadData?.metadata?.funnelLanguage || null
                }
            }
        });
        
    } catch (error) {
        console.error('Error fetching refund details:', error);
        res.status(500).json({ error: 'Failed to fetch refund details' });
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
        const { language, startDate, endDate, source } = req.query;
        
        let query = `SELECT * FROM transactions WHERE 1=1`;
        let params = [];
        let paramIndex = 1;
        
        // Exclude test transactions
        query += ` AND transaction_id NOT LIKE 'TEST%' AND transaction_id NOT LIKE '%TEST%'`;
        query += ` AND email NOT LIKE '%test%@%' AND email NOT LIKE '%@test.%'`;
        
        if (language === 'en' || language === 'es') {
            query += ` AND (funnel_language = $${paramIndex} OR (funnel_language IS NULL AND $${paramIndex} = 'en'))`;
            params.push(language);
            paramIndex++;
        }
        
        // Filter by funnel source (main/affiliate)
        if (source === 'main' || source === 'affiliate') {
            query += ` AND (funnel_source = $${paramIndex} OR (funnel_source IS NULL AND $${paramIndex} = 'main'))`;
            params.push(source);
            paramIndex++;
        }
        
        if (startDate && endDate) {
            query += ` AND created_at >= $${paramIndex}::date AND created_at < ($${paramIndex + 1}::date + INTERVAL '1 day')`;
            params.push(startDate, endDate);
            paramIndex += 2;
        }
        
        query += ` ORDER BY created_at DESC LIMIT 100`;
        
        const result = await pool.query(query, params);
        
        res.json({ transactions: result.rows, language: language || 'all', source: source || 'all' });
        
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

// TEMPORARY DEBUG: Check transaction dates in DB (no auth for easy access - REMOVE LATER)
app.get('/api/admin/debug-dates', async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        
        const results = await pool.query(`
            SELECT 
                MIN(created_at) as oldest_date,
                MAX(created_at) as newest_date,
                COUNT(*) as total_transactions,
                COUNT(*) FILTER (WHERE status = 'approved') as approved_count,
                COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE - INTERVAL '30 days') as last_30_days,
                COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE - INTERVAL '30 days' AND status = 'approved') as approved_last_30,
                COUNT(*) FILTER (WHERE created_at::date = CURRENT_DATE) as today_count,
                COUNT(*) FILTER (WHERE created_at::date = CURRENT_DATE AND status = 'approved') as approved_today,
                NOW() as server_now,
                CURRENT_DATE as server_date
            FROM transactions
        `);
        
        // Test parameterized query (same as sales endpoint)
        let paramTest = { note: 'No date params provided. Add ?startDate=2026-02-07&endDate=2026-02-08 to test' };
        if (startDate && endDate) {
            try {
                const testQuery = await pool.query(
                    `SELECT COUNT(*) as count, 
                            COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
                     FROM transactions 
                     WHERE status = 'approved' 
                     AND created_at >= $1::date 
                     AND created_at < ($2::date + INTERVAL '1 day')`,
                    [startDate, endDate]
                );
                
                // Also test without ::date cast
                const testQuery2 = await pool.query(
                    `SELECT COUNT(*) as count
                     FROM transactions 
                     WHERE status = 'approved' 
                     AND created_at >= $1::timestamp 
                     AND created_at < ($2::timestamp + INTERVAL '1 day')`,
                    [startDate, endDate]
                );
                
                // Also test with explicit date strings
                const testQuery3 = await pool.query(
                    `SELECT COUNT(*) as count
                     FROM transactions 
                     WHERE status = 'approved' 
                     AND created_at >= '${startDate}'::date 
                     AND created_at < ('${endDate}'::date + INTERVAL '1 day')`
                );
                
                paramTest = {
                    params_received: { startDate, endDate },
                    with_date_cast: testQuery.rows[0],
                    with_timestamp_cast: testQuery2.rows[0],
                    with_inline_dates: testQuery3.rows[0]
                };
            } catch (queryError) {
                paramTest = { 
                    error: queryError.message, 
                    params: { startDate, endDate } 
                };
            }
        }
        
        const sampleDates = await pool.query(`
            SELECT transaction_id, status, created_at, product, 
                   created_at::date as date_only
            FROM transactions 
            WHERE status = 'approved' 
            ORDER BY created_at DESC 
            LIMIT 5
        `);
        
        res.json({
            summary: results.rows[0],
            parameterized_test: paramTest,
            sample_transactions: sampleDates.rows
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Get sales stats (protected)
app.get('/api/admin/sales', authenticateToken, async (req, res) => {
    try {
        const { language, startDate, endDate, source } = req.query;
        
        // Debug: log filter params and transaction dates
        console.log(`📊 Sales API called - startDate: ${startDate || 'none'}, endDate: ${endDate || 'none'}, language: ${language || 'all'}, source: ${source || 'all'}`);
        const debugDates = await pool.query(`SELECT created_at::date as sale_date, COUNT(*) as count FROM transactions WHERE status = 'approved' GROUP BY created_at::date ORDER BY sale_date DESC LIMIT 10`);
        console.log('📅 Transaction dates in DB:', debugDates.rows);
        
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
        
        // Build date filter
        let dateCondition = '';
        if (startDate && endDate) {
            const startIdx = langParams.length + 1;
            const endIdx = langParams.length + 2;
            dateCondition = ` AND created_at >= $${startIdx}::date AND created_at < ($${endIdx}::date + INTERVAL '1 day')`;
            langParams.push(startDate, endDate);
        }
        
        const [totalResult, approvedResult, refundedResult, revenueResult] = await Promise.all([
            pool.query(`SELECT COUNT(*) FROM transactions WHERE 1=1 ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status IN ('refunded', 'chargeback') ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}`, langParams)
        ]);
        
        // Get today and this week
        const [todayResult, weekResult] = await Promise.all([
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' AND created_at >= CURRENT_DATE ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' AND created_at >= CURRENT_DATE - INTERVAL '7 days' ${langCondition}${sourceCondition}${dateCondition}`, langParams)
        ]);
        
        // Calculate conversion rate (leads -> sales) - also filtered by language
        const leadsCount = await pool.query(`SELECT COUNT(*) FROM leads WHERE 1=1 ${language ? `AND (funnel_language = $1 OR (funnel_language IS NULL AND $1 = 'en'))` : ''}`, language ? [language] : []);
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
            WHERE status = 'approved' AND (${frontKeywords}) ${langCondition}${sourceCondition}
        `, langParams);
        
        const upsell1Sales = await pool.query(`
            SELECT COUNT(DISTINCT email) as count 
            FROM transactions 
            WHERE status = 'approved' AND (${up1Keywords}) ${langCondition}${sourceCondition}
        `, langParams);
        
        const upsell2Sales = await pool.query(`
            SELECT COUNT(DISTINCT email) as count 
            FROM transactions 
            WHERE status = 'approved' AND (${up2Keywords}) ${langCondition}${sourceCondition}
        `, langParams);
        
        const upsell3Sales = await pool.query(`
            SELECT COUNT(DISTINCT email) as count 
            FROM transactions 
            WHERE status = 'approved' AND (${up3Keywords}) ${langCondition}${sourceCondition}
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
            source: source || 'all',
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
                    
                    // If no language from tx, try leads metadata
                    if (!lang) {
                        const leadResult = await pool.query(`
                            SELECT metadata->>'funnelLanguage' as funnel_language
                            FROM leads 
                            WHERE LOWER(email) = LOWER($1) AND metadata->>'funnelLanguage' IS NOT NULL
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

// Start server
app.listen(PORT, async () => {
    console.log(`🚀 ZapSpy API running on port ${PORT}`);
    console.log(`📊 Admin panel: http://localhost:${PORT}/admin`);
    
    // Initialize database
    await initDatabase();
});
