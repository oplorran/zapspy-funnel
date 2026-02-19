const express = require('express');
const router = express.Router();
const pool = require('../database');
const { authenticateToken, getCached, setCache } = require('../middleware');
const { buildDateFilter } = require('../helpers');
const { UPSELL_SQL } = require('../config');

// Real-time active users tracking (protected)
router.get('/api/admin/active-users', authenticateToken, async (req, res) => {
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
router.get('/api/admin/debug/funnel-events', authenticateToken, async (req, res) => {
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

// Trends endpoint with sparkline data (protected)
router.get('/api/admin/stats/trends', authenticateToken, async (req, res) => {
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
                `SELECT COUNT(DISTINCT email) as count, COALESCE(SUM(CASE WHEN CAST(value AS numeric) > 0 THEN CAST(value AS numeric) ELSE 0 END), 0) as revenue 
                 FROM transactions WHERE status = 'approved' AND created_at >= NOW() - make_interval(days => $1)`,
                [cappedDays]
            ),
            pool.query(
                `SELECT COUNT(DISTINCT email) as count, COALESCE(SUM(CASE WHEN CAST(value AS numeric) > 0 THEN CAST(value AS numeric) ELSE 0 END), 0) as revenue 
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
                `SELECT DATE(created_at AT TIME ZONE 'America/Sao_Paulo') as day, COUNT(DISTINCT email) as count,
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
router.get('/api/admin/capi-purchase-logs', authenticateToken, async (req, res) => {
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
router.get('/api/admin/pixel-stats', authenticateToken, async (req, res) => {
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

// ==================== FINANCIAL ENDPOINTS ====================

// Get financial summary (revenue from transactions + costs)
router.get('/api/admin/financial/summary', authenticateToken, async (req, res) => {
    try {
        const days = parseInt(req.query.days) || 30;
        const language = req.query.language || '';
        const source = req.query.source || '';
        
        let txConditions = [];
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
        
        const langSourceFilter = txConditions.length > 0 ? ' AND ' + txConditions.join(' AND ') : '';
        
        // PerfectPay USD->BRL conversion rate
        const usdToBrl = 1 / parseFloat(process.env.CONVERSION_BRL_TO_USD || '0.18');
        
        // Revenue expression: Monetizze values are BRL, PerfectPay values need USD->BRL conversion
        const revenueBRL = `
            CASE 
                WHEN t.value ~ '^[0-9.]+$' AND t.funnel_source = 'perfectpay' 
                    THEN CAST(t.value AS DECIMAL) * ${usdToBrl.toFixed(2)}
                WHEN t.value ~ '^[0-9.]+$' 
                    THEN CAST(t.value AS DECIMAL)
                ELSE 0 
            END
        `;
        
        // Today's approved revenue + sales
        const todayRevenueQuery = `
            SELECT COALESCE(SUM(${revenueBRL}), 0) as revenue, COUNT(*) as sales
            FROM transactions t
            WHERE t.status = 'approved'
            AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date = CURRENT_DATE
            ${langSourceFilter}
        `;
        
        // Today's refunds
        const todayRefundsQuery = `
            SELECT COALESCE(SUM(${revenueBRL}), 0) as refunds, COUNT(*) as refund_count
            FROM transactions t
            WHERE t.status IN ('refunded', 'chargeback')
            AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date = CURRENT_DATE
            ${langSourceFilter}
        `;
        
        // Today's costs (BRL)
        const todayCostsQuery = `
            SELECT COALESCE(SUM(amount), 0) as total FROM financial_costs WHERE cost_date = CURRENT_DATE
        `;
        
        // Month's approved revenue + sales
        const monthRevenueQuery = `
            SELECT COALESCE(SUM(${revenueBRL}), 0) as revenue, COUNT(*) as sales
            FROM transactions t
            WHERE t.status = 'approved'
            AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= date_trunc('month', CURRENT_DATE)
            ${langSourceFilter}
        `;
        
        // Month's refunds
        const monthRefundsQuery = `
            SELECT COALESCE(SUM(${revenueBRL}), 0) as refunds, COUNT(*) as refund_count
            FROM transactions t
            WHERE t.status IN ('refunded', 'chargeback')
            AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= date_trunc('month', CURRENT_DATE)
            ${langSourceFilter}
        `;
        
        // Month's costs (BRL)
        const monthCostsQuery = `
            SELECT COALESCE(SUM(amount), 0) as total FROM financial_costs WHERE cost_date >= date_trunc('month', CURRENT_DATE)::date
        `;
        
        // Year's approved revenue + sales
        const yearRevenueQuery = `
            SELECT COALESCE(SUM(${revenueBRL}), 0) as revenue, COUNT(*) as sales
            FROM transactions t
            WHERE t.status = 'approved'
            AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= date_trunc('year', CURRENT_DATE)
            ${langSourceFilter}
        `;
        
        // Year's refunds
        const yearRefundsQuery = `
            SELECT COALESCE(SUM(${revenueBRL}), 0) as refunds, COUNT(*) as refund_count
            FROM transactions t
            WHERE t.status IN ('refunded', 'chargeback')
            AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= date_trunc('year', CURRENT_DATE)
            ${langSourceFilter}
        `;
        
        // Year's costs (BRL)
        const yearCostsQuery = `
            SELECT COALESCE(SUM(amount), 0) as total FROM financial_costs WHERE cost_date >= date_trunc('year', CURRENT_DATE)::date
        `;
        
        // Daily breakdown
        const safeDays = Math.min(Math.max(parseInt(days) || 30, 1), 365);
        
        const dailyQuery = `
            WITH daily_revenue AS (
                SELECT 
                    (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date as day,
                    COALESCE(SUM(${revenueBRL}), 0) as revenue,
                    COUNT(*) as sales
                FROM transactions t
                WHERE t.status = 'approved'
                AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= CURRENT_DATE - INTERVAL '${safeDays} days'
                ${langSourceFilter}
                GROUP BY (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date
            ),
            daily_refunds AS (
                SELECT 
                    (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date as day,
                    COALESCE(SUM(${revenueBRL}), 0) as refunds,
                    COUNT(*) as refund_count
                FROM transactions t
                WHERE t.status IN ('refunded', 'chargeback')
                AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= CURRENT_DATE - INTERVAL '${safeDays} days'
                ${langSourceFilter}
                GROUP BY (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date
            ),
            daily_costs AS (
                SELECT cost_date as day, COALESCE(SUM(amount), 0) as costs
                FROM financial_costs
                WHERE cost_date >= CURRENT_DATE - INTERVAL '${safeDays} days'
                GROUP BY cost_date
            ),
            date_series AS (
                SELECT generate_series(
                    (CURRENT_DATE - INTERVAL '${safeDays} days')::date,
                    CURRENT_DATE, '1 day'::interval
                )::date as day
            )
            SELECT 
                ds.day,
                COALESCE(dr.revenue, 0) as revenue,
                COALESCE(dr.sales, 0) as sales,
                COALESCE(dc.costs, 0) as costs,
                COALESCE(drf.refunds, 0) as refunds,
                COALESCE(drf.refund_count, 0) as refund_count,
                COALESCE(dr.revenue, 0) - COALESCE(dc.costs, 0) - COALESCE(drf.refunds, 0) as profit
            FROM date_series ds
            LEFT JOIN daily_revenue dr ON ds.day = dr.day
            LEFT JOIN daily_refunds drf ON ds.day = drf.day
            LEFT JOIN daily_costs dc ON ds.day = dc.day
            ORDER BY ds.day DESC
        `;
        
        // Monthly breakdown (last 12 months)
        const monthlyQuery = `
            WITH monthly_revenue AS (
                SELECT 
                    date_trunc('month', (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date)::date as month,
                    COALESCE(SUM(${revenueBRL}), 0) as revenue,
                    COUNT(*) as sales
                FROM transactions t
                WHERE t.status = 'approved'
                AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= CURRENT_DATE - INTERVAL '12 months'
                ${langSourceFilter}
                GROUP BY date_trunc('month', (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date)::date
            ),
            monthly_refunds AS (
                SELECT 
                    date_trunc('month', (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date)::date as month,
                    COALESCE(SUM(${revenueBRL}), 0) as refunds,
                    COUNT(*) as refund_count
                FROM transactions t
                WHERE t.status IN ('refunded', 'chargeback')
                AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= CURRENT_DATE - INTERVAL '12 months'
                ${langSourceFilter}
                GROUP BY date_trunc('month', (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date)::date
            ),
            monthly_costs AS (
                SELECT 
                    date_trunc('month', cost_date)::date as month,
                    COALESCE(SUM(amount), 0) as costs
                FROM financial_costs
                WHERE cost_date >= CURRENT_DATE - INTERVAL '12 months'
                GROUP BY date_trunc('month', cost_date)::date
            ),
            all_months AS (
                SELECT DISTINCT month FROM (
                    SELECT month FROM monthly_revenue
                    UNION SELECT month FROM monthly_refunds
                    UNION SELECT month FROM monthly_costs
                ) combined
            )
            SELECT 
                am.month,
                COALESCE(mr.revenue, 0) as revenue,
                COALESCE(mr.sales, 0) as sales,
                COALESCE(mc.costs, 0) as costs,
                COALESCE(mrf.refunds, 0) as refunds,
                COALESCE(mrf.refund_count, 0) as refund_count,
                COALESCE(mr.revenue, 0) - COALESCE(mc.costs, 0) - COALESCE(mrf.refunds, 0) as profit
            FROM all_months am
            LEFT JOIN monthly_revenue mr ON am.month = mr.month
            LEFT JOIN monthly_refunds mrf ON am.month = mrf.month
            LEFT JOIN monthly_costs mc ON am.month = mc.month
            ORDER BY am.month DESC
            LIMIT 12
        `;
        
        const [
            todayRevResult, todayRefResult, todayCostResult,
            monthRevResult, monthRefResult, monthCostResult,
            yearRevResult, yearRefResult, yearCostResult,
            dailyResult, monthlyResult
        ] = await Promise.all([
            pool.query(todayRevenueQuery, txParams),
            pool.query(todayRefundsQuery, txParams),
            pool.query(todayCostsQuery),
            pool.query(monthRevenueQuery, txParams),
            pool.query(monthRefundsQuery, txParams),
            pool.query(monthCostsQuery),
            pool.query(yearRevenueQuery, txParams),
            pool.query(yearRefundsQuery, txParams),
            pool.query(yearCostsQuery),
            pool.query(dailyQuery, txParams),
            pool.query(monthlyQuery, txParams)
        ]);
        
        const todayRev = parseFloat(todayRevResult.rows[0]?.revenue || 0);
        const todaySales = parseInt(todayRevResult.rows[0]?.sales || 0);
        const todayRef = parseFloat(todayRefResult.rows[0]?.refunds || 0);
        const todayRefCount = parseInt(todayRefResult.rows[0]?.refund_count || 0);
        const todayCost = parseFloat(todayCostResult.rows[0]?.total || 0);
        const todayProfit = todayRev - todayCost - todayRef;
        
        const monthRev = parseFloat(monthRevResult.rows[0]?.revenue || 0);
        const monthSales = parseInt(monthRevResult.rows[0]?.sales || 0);
        const monthRef = parseFloat(monthRefResult.rows[0]?.refunds || 0);
        const monthRefCount = parseInt(monthRefResult.rows[0]?.refund_count || 0);
        const monthCost = parseFloat(monthCostResult.rows[0]?.total || 0);
        const monthTotalCosts = monthCost + monthRef;
        const monthProfit = monthRev - monthTotalCosts;
        
        const monthROI = monthTotalCosts > 0 ? ((monthRev - monthTotalCosts) / monthTotalCosts * 100) : 0;
        const monthMargin = monthRev > 0 ? (monthProfit / monthRev * 100) : 0;
        const monthCPA = monthSales > 0 ? monthCost / monthSales : 0;
        
        const yearRev = parseFloat(yearRevResult.rows[0]?.revenue || 0);
        const yearSales = parseInt(yearRevResult.rows[0]?.sales || 0);
        const yearRef = parseFloat(yearRefResult.rows[0]?.refunds || 0);
        const yearRefCount = parseInt(yearRefResult.rows[0]?.refund_count || 0);
        const yearCost = parseFloat(yearCostResult.rows[0]?.total || 0);
        const yearProfit = yearRev - yearCost - yearRef;
        
        res.json({
            today: {
                revenue: todayRev, sales: todaySales, costs: todayCost,
                refunds: todayRef, refundCount: todayRefCount, profit: todayProfit
            },
            month: {
                revenue: monthRev, sales: monthSales, costs: monthCost,
                refunds: monthRef, refundCount: monthRefCount, profit: monthProfit,
                roi: Math.round(monthROI * 100) / 100,
                margin: Math.round(monthMargin * 100) / 100,
                cpa: Math.round(monthCPA * 100) / 100
            },
            year: {
                revenue: yearRev, sales: yearSales, costs: yearCost,
                refunds: yearRef, refundCount: yearRefCount, profit: yearProfit
            },
            daily: dailyResult.rows,
            monthly: monthlyResult.rows
        });
        
    } catch (error) {
        console.error('❌ Error fetching financial summary:', error.message);
        res.status(500).json({ error: 'Failed to fetch financial summary', details: error.message });
    }
});

// Get financial costs list
router.get('/api/admin/financial/costs', authenticateToken, async (req, res) => {
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
                COALESCE(SUM(amount), 0) as total_brl,
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

// Add a new cost (always BRL)
router.post('/api/admin/financial/costs', authenticateToken, async (req, res) => {
    try {
        const { cost_date, category, description, amount, notes } = req.body;
        
        if (!cost_date || !description || !amount) {
            return res.status(400).json({ error: 'Data, descrição e valor são obrigatórios' });
        }
        
        const amountBrl = parseFloat(amount);
        
        const result = await pool.query(`
            INSERT INTO financial_costs (cost_date, category, description, amount, currency, notes, created_by)
            VALUES ($1, $2, $3, $4, 'BRL', $5, $6)
            RETURNING *
        `, [cost_date, category || 'other', description, amountBrl, notes || null, req.user?.id || null]);
        
        console.log(`💰 Custo adicionado: ${description} - R$ ${amountBrl.toFixed(2)} em ${cost_date}`);
        
        res.json({ success: true, cost: result.rows[0] });
        
    } catch (error) {
        console.error('Error adding cost:', error);
        res.status(500).json({ error: 'Failed to add cost' });
    }
});

// Update a cost (always BRL)
router.put('/api/admin/financial/costs/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { cost_date, category, description, amount, notes } = req.body;
        
        const result = await pool.query(`
            UPDATE financial_costs 
            SET cost_date = $1, category = $2, description = $3, amount = $4, currency = 'BRL',
                notes = $5, updated_at = NOW()
            WHERE id = $6
            RETURNING *
        `, [cost_date, category, description, parseFloat(amount), notes || null, id]);
        
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
router.delete('/api/admin/financial/costs/:id', authenticateToken, async (req, res) => {
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
router.get('/api/admin/stats', authenticateToken, async (req, res) => {
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
router.get('/api/admin/stats/comparison', authenticateToken, async (req, res) => {
    try {
        // Current week stats (using Brazil timezone)
        const currentWeekLeads = await pool.query(`
            SELECT COUNT(*) FROM leads 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '7 days')::date
        `);
        
        const currentWeekSales = await pool.query(`
            SELECT COUNT(DISTINCT email) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
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
            SELECT COUNT(DISTINCT email) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
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
router.get('/api/admin/stats/period-comparison', authenticateToken, async (req, res) => {
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
                SELECT COUNT(DISTINCT email) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
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
                SELECT COUNT(DISTINCT email) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
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
                SELECT COUNT(DISTINCT email) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
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
                SELECT COUNT(DISTINCT email) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
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
router.get('/api/admin/stats/heatmap', authenticateToken, async (req, res) => {
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
                    COUNT(DISTINCT email) as count
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
router.get('/api/admin/stats/countries-sales', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                COALESCE(NULLIF(TRIM(l.country_code), ''), 'XX') as country_code,
                COALESCE(NULLIF(TRIM(l.country), ''), 'Desconhecido') as country_name,
                COUNT(DISTINCT t.email) as sales,
                COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) as revenue
            FROM transactions t
            LEFT JOIN leads l ON LOWER(TRIM(t.email)) = LOWER(TRIM(l.email))
            WHERE t.status = 'approved'
            AND (t.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '30 days')::date
            GROUP BY COALESCE(NULLIF(TRIM(l.country_code), ''), 'XX'), COALESCE(NULLIF(TRIM(l.country), ''), 'Desconhecido')
            ORDER BY sales DESC, revenue DESC
            LIMIT 10
        `);
        
        res.json({ countries: result.rows });
        
    } catch (error) {
        console.error('Error fetching countries sales:', error);
        res.status(500).json({ error: 'Failed to fetch countries sales' });
    }
});

// Get traffic sources from UTM and referrer data
router.get('/api/admin/stats/traffic-sources', authenticateToken, async (req, res) => {
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
router.get('/api/admin/stats/weekly-performance', authenticateToken, async (req, res) => {
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
        
        // Get sales by day of week (last 4 weeks) - unique buyers only
        const salesResult = await pool.query(`
            SELECT 
                EXTRACT(DOW FROM created_at AT TIME ZONE 'America/Sao_Paulo') as day_of_week,
                COUNT(DISTINCT email) as count
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
router.get('/api/admin/funnel-stats', authenticateToken, async (req, res) => {
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
        
        // Total approved sales (unique front-end buyers, not counting upsells as separate sales)
        const salesRes = await pool.query(`SELECT COUNT(DISTINCT email) as count FROM transactions WHERE status = 'approved'${dateFilterTx}`, params);
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

// ==================== FUNNEL TRACKING ENDPOINTS ====================

// Get funnel data with language/source/date filtering
router.get('/api/admin/funnel', authenticateToken, async (req, res) => {
    try {
        const { language, source, startDate, endDate } = req.query;
        
        let langCondition = '';
        if (language === 'en') {
            langCondition = `AND COALESCE(metadata->>'funnelLanguage', 'en') = 'en'`;
        } else if (language === 'es') {
            langCondition = `AND metadata->>'funnelLanguage' = 'es'`;
        }
        
        let sourceCondition = '';
        if (source === 'main') {
            sourceCondition = `AND COALESCE(metadata->>'funnelSource', 'main') = 'main'`;
        } else if (source === 'affiliate') {
            sourceCondition = `AND metadata->>'funnelSource' = 'affiliate'`;
        }
        
        let dateCondition = '';
        if (startDate && endDate) {
            dateCondition = `AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= '${startDate}'::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= '${endDate}'::date`;
        } else if (startDate) {
            dateCondition = `AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= '${startDate}'::date`;
        } else {
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
            const enFrontKeywords = "product ILIKE '%Monitor%' OR product ILIKE '%ZappDetect%' OR product ILIKE '%341972%' OR product ILIKE '%330254%'";
            const enUp1Keywords = "product ILIKE '%Message Vault%' OR product ILIKE '%349241%' OR product ILIKE '%341443%'";
            const enUp2Keywords = "product ILIKE '%360%' OR product ILIKE '%Tracker%' OR product ILIKE '%349242%' OR product ILIKE '%341444%'";
            const enUp3Keywords = "product ILIKE '%Instant Access%' OR product ILIKE '%349243%' OR product ILIKE '%341448%'";
            
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
router.get('/api/debug/funnel/search', authenticateToken, async (req, res) => {
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
router.get('/api/admin/funnel/visitor/:visitorId', authenticateToken, async (req, res) => {
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
router.get('/api/admin/customer/:leadId/journey', authenticateToken, async (req, res) => {
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
            totalSpent: calculatedTotalSpent,
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

module.exports = router;
