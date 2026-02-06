/**
 * ZapSpy.ai Backend API
 * Lead capture and admin panel API
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

const app = express();
const PORT = process.env.PORT || 3000;

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
app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Capture lead (from frontend form)
app.post('/api/leads', leadLimiter, async (req, res) => {
    try {
        const {
            email,
            whatsapp,
            targetPhone,
            targetGender,
            referrer,
            userAgent
        } = req.body;
        
        // Validation
        if (!email || !whatsapp) {
            return res.status(400).json({ error: 'Email and WhatsApp are required' });
        }
        
        // Get IP address
        const ipAddress = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
        
        // Insert lead into database
        const result = await pool.query(
            `INSERT INTO leads (email, whatsapp, target_phone, target_gender, ip_address, referrer, user_agent, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
             RETURNING id, created_at`,
            [email, whatsapp, targetPhone || null, targetGender || null, ipAddress, referrer || null, userAgent || null]
        );
        
        console.log(`New lead captured: ${email} - ${whatsapp}`);
        
        res.status(201).json({
            success: true,
            message: 'Lead captured successfully',
            id: result.rows[0].id
        });
        
    } catch (error) {
        console.error('Error capturing lead:', error);
        res.status(500).json({ error: 'Failed to capture lead' });
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
        
        let query = `SELECT * FROM leads`;
        let countQuery = `SELECT COUNT(*) FROM leads`;
        let params = [];
        let conditions = [];
        
        if (search) {
            conditions.push(`(email ILIKE $${params.length + 1} OR whatsapp ILIKE $${params.length + 1} OR target_phone ILIKE $${params.length + 1})`);
            params.push(`%${search}%`);
        }
        
        if (status) {
            conditions.push(`status = $${params.length + 1}`);
            params.push(status);
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

// Export leads as CSV (protected)
app.get('/api/admin/leads/export', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM leads ORDER BY created_at DESC');
        
        // Create CSV
        const headers = ['ID', 'Email', 'WhatsApp', 'Target Phone', 'Gender', 'Status', 'IP', 'Created At'];
        const rows = result.rows.map(lead => [
            lead.id,
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

// Start server
app.listen(PORT, () => {
    console.log(`🚀 ZapSpy API running on port ${PORT}`);
    console.log(`📊 Admin panel: http://localhost:${PORT}/admin`);
});
