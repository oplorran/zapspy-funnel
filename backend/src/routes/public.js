const express = require('express');
const router = express.Router();
const pool = require('../database');
const { authenticateToken, requireAdmin, leadLimiter, apiLimiter, invalidateCache } = require('../middleware');
const { sendToFacebookCAPI, hashData, sendMissingCAPIPurchases, backfillTransactionFbcFbp } = require('../services/facebook-capi');
const { getCountryFromIP } = require('../services/geolocation');
const { ZAPI_BASE_URL, ZAPI_CLIENT_TOKEN } = require('../config');

// ==================== PUBLIC API ROUTES ====================

// Health check
router.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

router.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

router.get('/api/capi/status', async (req, res) => {
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

router.get('/api/health/db', authenticateToken, (req, res, next) => {
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
router.get('/', (req, res) => {
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
router.post('/api/leads', leadLimiter, async (req, res) => {
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
            utm_term,
            // A/B test tracking
            ab_test_id,
            ab_variant
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
        const existingLead = await pool.queryRetry(
            `SELECT id, email, whatsapp, visit_count FROM leads WHERE LOWER(email) = LOWER($1) OR whatsapp = $2 LIMIT 1`,
            [email, whatsapp]
        );
        
        let result;
        let isNewLead = false;
        
        if (existingLead.rows.length > 0) {
            // Update existing lead with new visit info
            const currentVisitCount = existingLead.rows[0].visit_count || 1;
            result = await pool.queryRetry(
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
                    ab_test_id = COALESCE($22, ab_test_id),
                    ab_variant = COALESCE($23, ab_variant),
                    last_visit_at = NOW(),
                    updated_at = NOW()
                WHERE id = $13
                RETURNING id, created_at`,
                [name || null, targetPhone || null, targetGender || null, ipAddress, referrer || null, ua || null, currentVisitCount + 1, geoData.country, geoData.country_code, geoData.city, visitorId || null, source, existingLead.rows[0].id, utm_source || null, utm_medium || null, utm_campaign || null, utm_content || null, utm_term || null, fbc || null, fbp || null, geoData.state || null, ab_test_id || null, ab_variant || null]
            );
            console.log(`Returning lead [${language.toUpperCase()}/${source}]: ${name || 'No name'} - ${email} - ${geoData.country || 'Unknown'} (visit #${currentVisitCount + 1})`);
        } else {
            // Insert new lead
            result = await pool.queryRetry(
                `INSERT INTO leads (name, email, whatsapp, target_phone, target_gender, ip_address, referrer, user_agent, funnel_language, funnel_source, visit_count, country, country_code, city, state, visitor_id, utm_source, utm_medium, utm_campaign, utm_content, utm_term, fbc, fbp, ab_test_id, ab_variant, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, NOW())
                 RETURNING id, created_at`,
                [name || null, email, whatsapp, targetPhone || null, targetGender || null, ipAddress, referrer || null, ua || null, language, source, geoData.country, geoData.country_code, geoData.city, geoData.state || null, visitorId || null, utm_source || null, utm_medium || null, utm_campaign || null, utm_content || null, utm_term || null, fbc || null, fbp || null, ab_test_id || null, ab_variant || null]
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
router.post('/api/capi/test', async (req, res) => {
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
router.post('/api/capi/event', async (req, res) => {
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

// ==================== FUNNEL TRACKING API ====================

// Track funnel event (public - no auth required)
router.post('/api/track', async (req, res) => {
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
            ab_test_id,      // A/B test ID (from URL splitter)
            ab_variant,      // A/B variant (A or B)
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
        
        await pool.queryRetry(
            `INSERT INTO funnel_events (visitor_id, event, page, target_phone, target_gender, ip_address, user_agent, fbc, fbp, ab_test_id, ab_variant, metadata, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())`,
            [visitorId, event, page || null, targetPhone || null, targetGender || null, ipAddress, userAgent, fbc || null, fbp || null, ab_test_id || null, ab_variant || null, JSON.stringify(enrichedMetadata)]
        );
        
        res.json({ success: true, language });
        
    } catch (error) {
        console.error('Error tracking event:', error);
        res.status(500).json({ error: 'Failed to track event' });
    }
});

router.post('/api/admin/capi-catchup', authenticateToken, requireAdmin, async (req, res) => {
    try {
        console.log('🔄 Manual CAPI catch-up triggered by admin...');
        await sendMissingCAPIPurchases();
        res.json({ success: true, message: 'CAPI catch-up executado. Verifique os logs de Purchase.' });
    } catch (error) {
        console.error('CAPI catch-up error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

router.post('/api/enrich-purchase', async (req, res) => {
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
router.post('/api/admin/capi-clear-resend', authenticateToken, requireAdmin, async (req, res) => {
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

// ==================== REFUND REQUESTS API ====================

// Submit refund request (public)
router.post('/api/refund', async (req, res) => {
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
            language,
            visitorId
        } = req.body;

        // Validation
        if (!email || !fullName || !reason) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        const ipAddress = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        const userAgent = req.headers['user-agent'] || null;

        // ==================== CROSS-REFERENCE DATA ====================
        // Try to find this person in our leads and transactions to enrich the refund data
        // Language priority: 1. explicit from form, 2. cross-reference
        let detectedLanguage = (language === 'en' || language === 'es' || language === 'pt') ? language : null;
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
                    if (!detectedLanguage) detectedLanguage = tx.funnel_language || null;
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

// ==================== SOCIAL SCAN (DEFASTRA) ====================

router.post('/api/social-scan', apiLimiter, async (req, res) => {
    try {
        const { phone } = req.body;
        if (!phone) {
            return res.status(400).json({ error: 'Phone number required' });
        }

        const cleanPhone = phone.replace(/\D/g, '');
        if (cleanPhone.length < 10) {
            return res.status(400).json({ error: 'Invalid phone number' });
        }

        const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY || '';
        
        if (!RAPIDAPI_KEY) {
            return res.status(200).json({ 
                success: false, 
                error: 'API key not configured',
                fallback: true 
            });
        }

        const response = await fetch('https://phone-social-data-enrichment.p.rapidapi.com/deep_phone_check', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-RapidAPI-Key': RAPIDAPI_KEY,
                'X-RapidAPI-Host': 'phone-social-data-enrichment.p.rapidapi.com'
            },
            body: new URLSearchParams({
                phone: cleanPhone,
                timeout: 'normal'
            })
        });

        const data = await response.json();
        
        if (!data.status) {
            console.log('Defastra API error:', data.error_message || 'Unknown error');
            return res.status(200).json({ success: false, fallback: true });
        }

        const check = data.deep_phone_check || {};
        const profiles = check.online_profiles || {};
        
        const foundPlatforms = [];
        const allPlatforms = {};
        
        for (const [platform, info] of Object.entries(profiles)) {
            if (info && !info.error) {
                allPlatforms[platform] = {
                    found: info.is_registered === true,
                    info: info.additional_information || null
                };
                if (info.is_registered === true) {
                    foundPlatforms.push(platform);
                }
            }
        }

        res.json({
            success: true,
            platforms: allPlatforms,
            found: foundPlatforms,
            foundCount: foundPlatforms.length,
            carrier: check.carrier || null,
            location: check.location || null,
            os: check.os || null,
            riskScore: check.risk_score,
            riskLevel: check.risk_level
        });

    } catch (error) {
        console.error('Social scan error:', error.message);
        res.status(200).json({ success: false, fallback: true });
    }
});

module.exports = router;
