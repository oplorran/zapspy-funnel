const express = require('express');
const router = express.Router();
const pool = require('../database');
const { authenticateToken } = require('../middleware');
const { UPSELL_SQL } = require('../config');

// ==================== A/B TESTING API (URL SPLITTER) ====================

// Admin: List all A/B tests with REAL stats from leads + transactions + upsell breakdown
router.get('/api/admin/ab-tests', authenticateToken, async (req, res) => {
    try {
        const tests = await pool.query(`
            SELECT 
                at.*,
                (SELECT COUNT(*) FROM ab_test_visitors WHERE test_id = at.id AND variant = 'A') as visitors_a,
                (SELECT COUNT(*) FROM ab_test_visitors WHERE test_id = at.id AND variant = 'B') as visitors_b,
                (SELECT COUNT(*) FROM leads WHERE ab_test_id = at.id AND ab_variant = 'A') as leads_a,
                (SELECT COUNT(*) FROM leads WHERE ab_test_id = at.id AND ab_variant = 'B') as leads_b,
                -- Total purchases/revenue per variant
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved') as purchases_a,
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved') as purchases_b,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved') as revenue_a,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved') as revenue_b,
                -- Front-end only purchases/revenue
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved' AND ${UPSELL_SQL.front}) as front_purchases_a,
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved' AND ${UPSELL_SQL.front}) as front_purchases_b,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved' AND ${UPSELL_SQL.front}) as front_revenue_a,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved' AND ${UPSELL_SQL.front}) as front_revenue_b,
                -- Upsell 1
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved' AND ${UPSELL_SQL.up1}) as up1_purchases_a,
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved' AND ${UPSELL_SQL.up1}) as up1_purchases_b,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved' AND ${UPSELL_SQL.up1}) as up1_revenue_a,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved' AND ${UPSELL_SQL.up1}) as up1_revenue_b,
                -- Upsell 2
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved' AND ${UPSELL_SQL.up2}) as up2_purchases_a,
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved' AND ${UPSELL_SQL.up2}) as up2_purchases_b,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved' AND ${UPSELL_SQL.up2}) as up2_revenue_a,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved' AND ${UPSELL_SQL.up2}) as up2_revenue_b,
                -- Upsell 3
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved' AND ${UPSELL_SQL.up3}) as up3_purchases_a,
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved' AND ${UPSELL_SQL.up3}) as up3_purchases_b,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved' AND ${UPSELL_SQL.up3}) as up3_revenue_a,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved' AND ${UPSELL_SQL.up3}) as up3_revenue_b,
                -- Upsell 4
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved' AND ${UPSELL_SQL.up4}) as up4_purchases_a,
                (SELECT COUNT(DISTINCT t.email) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved' AND ${UPSELL_SQL.up4}) as up4_purchases_b,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'A' AND t.status = 'approved' AND ${UPSELL_SQL.up4}) as up4_revenue_a,
                (SELECT COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) FROM transactions t 
                    INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email) 
                    WHERE l.ab_test_id = at.id AND l.ab_variant = 'B' AND t.status = 'approved' AND ${UPSELL_SQL.up4}) as up4_revenue_b
            FROM ab_tests at
            ORDER BY at.created_at DESC
        `);
        
        const testsWithStats = tests.rows.map(test => {
            const vA = parseInt(test.visitors_a) || 0;
            const vB = parseInt(test.visitors_b) || 0;
            const lA = parseInt(test.leads_a) || 0;
            const lB = parseInt(test.leads_b) || 0;
            const pA = parseInt(test.purchases_a) || 0;
            const pB = parseInt(test.purchases_b) || 0;
            const rA = parseFloat(test.revenue_a) || 0;
            const rB = parseFloat(test.revenue_b) || 0;
            const fpA = parseInt(test.front_purchases_a) || 0;
            const fpB = parseInt(test.front_purchases_b) || 0;
            const frA = parseFloat(test.front_revenue_a) || 0;
            const frB = parseFloat(test.front_revenue_b) || 0;
            const u1pA = parseInt(test.up1_purchases_a) || 0;
            const u1pB = parseInt(test.up1_purchases_b) || 0;
            const u1rA = parseFloat(test.up1_revenue_a) || 0;
            const u1rB = parseFloat(test.up1_revenue_b) || 0;
            const u2pA = parseInt(test.up2_purchases_a) || 0;
            const u2pB = parseInt(test.up2_purchases_b) || 0;
            const u2rA = parseFloat(test.up2_revenue_a) || 0;
            const u2rB = parseFloat(test.up2_revenue_b) || 0;
            const u3pA = parseInt(test.up3_purchases_a) || 0;
            const u3pB = parseInt(test.up3_purchases_b) || 0;
            const u3rA = parseFloat(test.up3_revenue_a) || 0;
            const u3rB = parseFloat(test.up3_revenue_b) || 0;
            const u4pA = parseInt(test.up4_purchases_a) || 0;
            const u4pB = parseInt(test.up4_purchases_b) || 0;
            const u4rA = parseFloat(test.up4_revenue_a) || 0;
            const u4rB = parseFloat(test.up4_revenue_b) || 0;
            
            return {
                ...test,
                visitors_a: vA, visitors_b: vB,
                leads_a: lA, leads_b: lB,
                purchases_a: pA, purchases_b: pB,
                revenue_a: rA, revenue_b: rB,
                lead_rate_a: vA > 0 ? ((lA / vA) * 100).toFixed(2) : '0.00',
                lead_rate_b: vB > 0 ? ((lB / vB) * 100).toFixed(2) : '0.00',
                purchase_rate_a: vA > 0 ? ((pA / vA) * 100).toFixed(2) : '0.00',
                purchase_rate_b: vB > 0 ? ((pB / vB) * 100).toFixed(2) : '0.00',
                rpv_a: vA > 0 ? (rA / vA).toFixed(2) : '0.00',
                rpv_b: vB > 0 ? (rB / vB).toFixed(2) : '0.00',
                front_purchases_a: fpA, front_purchases_b: fpB,
                front_revenue_a: frA, front_revenue_b: frB,
                up1_purchases_a: u1pA, up1_purchases_b: u1pB,
                up1_revenue_a: u1rA, up1_revenue_b: u1rB,
                up1_take_a: fpA > 0 ? ((u1pA / fpA) * 100).toFixed(1) : '0.0',
                up1_take_b: fpB > 0 ? ((u1pB / fpB) * 100).toFixed(1) : '0.0',
                up2_purchases_a: u2pA, up2_purchases_b: u2pB,
                up2_revenue_a: u2rA, up2_revenue_b: u2rB,
                up2_take_a: fpA > 0 ? ((u2pA / fpA) * 100).toFixed(1) : '0.0',
                up2_take_b: fpB > 0 ? ((u2pB / fpB) * 100).toFixed(1) : '0.0',
                up3_purchases_a: u3pA, up3_purchases_b: u3pB,
                up3_revenue_a: u3rA, up3_revenue_b: u3rB,
                up3_take_a: fpA > 0 ? ((u3pA / fpA) * 100).toFixed(1) : '0.0',
                up3_take_b: fpB > 0 ? ((u3pB / fpB) * 100).toFixed(1) : '0.0',
                up4_purchases_a: u4pA, up4_purchases_b: u4pB,
                up4_revenue_a: u4rA, up4_revenue_b: u4rB,
                up4_take_a: fpA > 0 ? ((u4pA / fpA) * 100).toFixed(1) : '0.0',
                up4_take_b: fpB > 0 ? ((u4pB / fpB) * 100).toFixed(1) : '0.0',
                avg_ticket_a: pA > 0 ? (rA / pA).toFixed(2) : '0.00',
                avg_ticket_b: pB > 0 ? (rB / pB).toFixed(2) : '0.00',
                upsell_revenue_a: (u1rA + u2rA + u3rA + u4rA),
                upsell_revenue_b: (u1rB + u2rB + u3rB + u4rB)
            };
        });
        
        res.json(testsWithStats);
    } catch (error) {
        console.error('List A/B tests error:', error);
        res.status(500).json({ error: 'Failed to fetch A/B tests' });
    }
});

// Admin: Get single A/B test with detailed stats
router.get('/api/admin/ab-tests/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const test = await pool.query(`SELECT * FROM ab_tests WHERE id = $1`, [id]);
        if (test.rows.length === 0) return res.status(404).json({ error: 'Test not found' });
        
        // Visitors by day
        const dailyVisitors = await pool.query(`
            SELECT DATE(created_at) as date, variant, COUNT(*) as count
            FROM ab_test_visitors WHERE test_id = $1
            GROUP BY DATE(created_at), variant ORDER BY date
        `, [id]);
        
        // Leads by day
        const dailyLeads = await pool.query(`
            SELECT DATE(created_at) as date, ab_variant as variant, COUNT(*) as count
            FROM leads WHERE ab_test_id = $1
            GROUP BY DATE(created_at), ab_variant ORDER BY date
        `, [id]);
        
        // Sales by day (via lead email match)
        const dailySales = await pool.query(`
            SELECT DATE(t.created_at) as date, l.ab_variant as variant, 
                   COUNT(DISTINCT t.email) as count, COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) as revenue
            FROM transactions t
            INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email)
            WHERE l.ab_test_id = $1 AND t.status = 'approved'
            GROUP BY DATE(t.created_at), l.ab_variant ORDER BY date
        `, [id]);
        
        // Totals
        const visitors = await pool.query(`
            SELECT variant, COUNT(*) as count FROM ab_test_visitors WHERE test_id = $1 GROUP BY variant
        `, [id]);
        const leads = await pool.query(`
            SELECT ab_variant as variant, COUNT(*) as count FROM leads WHERE ab_test_id = $1 GROUP BY ab_variant
        `, [id]);
        const sales = await pool.query(`
            SELECT l.ab_variant as variant, COUNT(DISTINCT t.email) as count, 
                   COALESCE(SUM(CAST(t.value AS DECIMAL)), 0) as revenue
            FROM transactions t
            INNER JOIN leads l ON LOWER(t.email) = LOWER(l.email)
            WHERE l.ab_test_id = $1 AND t.status = 'approved'
            GROUP BY l.ab_variant
        `, [id]);
        
        res.json({
            test: test.rows[0],
            daily: { visitors: dailyVisitors.rows, leads: dailyLeads.rows, sales: dailySales.rows },
            totals: { visitors: visitors.rows, leads: leads.rows, sales: sales.rows }
        });
    } catch (error) {
        console.error('Get A/B test error:', error);
        res.status(500).json({ error: 'Failed to fetch test details' });
    }
});

// Admin: Create A/B test (URL splitter)
router.post('/api/admin/ab-tests', authenticateToken, async (req, res) => {
    try {
        const { name, description, url_a, url_b, variant_a_name, variant_b_name, traffic_split } = req.body;
        
        if (!name || !url_a || !url_b) {
            return res.status(400).json({ error: 'Name, URL A and URL B are required' });
        }
        
        // Generate slug from name
        const slug = name.toLowerCase()
            .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
            .replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')
            .substring(0, 80);
        
        // Check slug uniqueness
        const existingSlug = await pool.query(`SELECT id FROM ab_tests WHERE slug = $1`, [slug]);
        const finalSlug = existingSlug.rows.length > 0 ? `${slug}-${Date.now().toString(36)}` : slug;
        
        const result = await pool.query(`
            INSERT INTO ab_tests (name, description, funnel, variant_a_name, variant_b_name, 
                traffic_split, status, created_by, url_a, url_b, slug, test_type)
            VALUES ($1, $2, 'url_split', $3, $4, $5, 'draft', $6, $7, $8, $9, 'url_split')
            RETURNING *
        `, [
            name, description || '', variant_a_name || 'Controle', variant_b_name || 'Teste',
            traffic_split || 50, req.user.id, url_a, url_b, finalSlug
        ]);
        
        // Update dynamic CORS origins
        try { req.app.locals.abTestAllowedOrigins.add(new URL(url_a).origin); } catch(e) {}
        try { req.app.locals.abTestAllowedOrigins.add(new URL(url_b).origin); } catch(e) {}
        
        res.json(result.rows[0]);
    } catch (error) {
        console.error('Create A/B test error:', error);
        res.status(500).json({ error: 'Failed to create A/B test' });
    }
});

// Admin: Update A/B test
router.put('/api/admin/ab-tests/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { name, description, url_a, url_b, variant_a_name, variant_b_name, traffic_split } = req.body;
        
        const result = await pool.query(`
            UPDATE ab_tests SET
                name = COALESCE($1, name), description = COALESCE($2, description),
                url_a = COALESCE($3, url_a), url_b = COALESCE($4, url_b),
                variant_a_name = COALESCE($5, variant_a_name), variant_b_name = COALESCE($6, variant_b_name),
                traffic_split = COALESCE($7, traffic_split)
            WHERE id = $8 RETURNING *
        `, [name, description, url_a, url_b, variant_a_name, variant_b_name, traffic_split, id]);
        
        if (result.rows.length === 0) return res.status(404).json({ error: 'Test not found' });
        
        // Update dynamic CORS origins
        if (url_a) try { req.app.locals.abTestAllowedOrigins.add(new URL(url_a).origin); } catch(e) {}
        if (url_b) try { req.app.locals.abTestAllowedOrigins.add(new URL(url_b).origin); } catch(e) {}
        
        res.json(result.rows[0]);
    } catch (error) {
        console.error('Update A/B test error:', error);
        res.status(500).json({ error: 'Failed to update A/B test' });
    }
});

// Admin: Start/Stop/Reset A/B test
router.post('/api/admin/ab-tests/:id/status', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { action } = req.body;
        let result;
        
        if (action === 'start') {
            result = await pool.query(
                `UPDATE ab_tests SET status = 'running', started_at = NOW() WHERE id = $1 RETURNING *`, [id]
            );
        } else if (action === 'stop') {
            result = await pool.query(
                `UPDATE ab_tests SET status = 'stopped', ended_at = NOW() WHERE id = $1 RETURNING *`, [id]
            );
        } else if (action === 'reset') {
            await pool.query(`DELETE FROM ab_test_visitors WHERE test_id = $1`, [id]);
            await pool.query(`UPDATE leads SET ab_test_id = NULL, ab_variant = NULL WHERE ab_test_id = $1`, [id]);
            result = await pool.query(
                `UPDATE ab_tests SET status = 'draft', started_at = NULL, ended_at = NULL, winner = NULL WHERE id = $1 RETURNING *`, [id]
            );
        } else {
            return res.status(400).json({ error: 'Invalid action' });
        }
        
        if (!result || result.rows.length === 0) return res.status(404).json({ error: 'Test not found' });
        res.json(result.rows[0]);
    } catch (error) {
        console.error('Update A/B test status error:', error);
        res.status(500).json({ error: 'Failed to update test status' });
    }
});

// Admin: Set winner
router.post('/api/admin/ab-tests/:id/winner', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { winner } = req.body;
        if (!['A', 'B'].includes(winner)) return res.status(400).json({ error: 'Winner must be A or B' });
        
        const result = await pool.query(
            `UPDATE ab_tests SET status = 'completed', winner = $1, ended_at = NOW() WHERE id = $2 RETURNING *`,
            [winner, id]
        );
        if (result.rows.length === 0) return res.status(404).json({ error: 'Test not found' });
        res.json(result.rows[0]);
    } catch (error) {
        console.error('Set A/B winner error:', error);
        res.status(500).json({ error: 'Failed to set winner' });
    }
});

// Admin: Delete A/B test
router.delete('/api/admin/ab-tests/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        await pool.query(`DELETE FROM ab_tests WHERE id = $1`, [id]);
        
        res.json({ success: true });
        
    } catch (error) {
        console.error('Delete A/B test error:', error);
        res.status(500).json({ error: 'Failed to delete test' });
    }
});

module.exports = router;
