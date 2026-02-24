/**
 * Email Dispatch Admin API Routes
 * 
 * Provides endpoints for the admin panel to manage
 * batch email dispatch from PostgreSQL to ActiveCampaign.
 */

const express = require('express');
const router = express.Router();
const { authenticateToken } = require('../middleware');
const dispatchService = require('../services/email-dispatch');

// ==================== LEAD COUNTS ====================

// GET /api/admin/dispatch/counts - Get lead counts by category
router.get('/api/admin/dispatch/counts', authenticateToken, async (req, res) => {
    try {
        const counts = await dispatchService.getLeadCounts();
        res.json({ success: true, counts });
    } catch (error) {
        console.error('Error getting lead counts:', error);
        res.status(500).json({ error: error.message });
    }
});

// ==================== BATCH DISPATCH ====================

// POST /api/admin/dispatch/start - Start a batch dispatch
router.post('/api/admin/dispatch/start', authenticateToken, async (req, res) => {
    try {
        const { category, language, batchSize = 500 } = req.body;

        if (!category || !language) {
            return res.status(400).json({ error: 'category and language are required' });
        }

        const validCategories = ['checkout_abandon', 'sale_cancelled', 'funnel_abandon'];
        if (!validCategories.includes(category)) {
            return res.status(400).json({ error: `Invalid category. Must be one of: ${validCategories.join(', ')}` });
        }

        const validLanguages = ['en', 'es'];
        if (!validLanguages.includes(language)) {
            return res.status(400).json({ error: 'Invalid language. Must be en or es' });
        }

        const size = Math.min(Math.max(parseInt(batchSize) || 500, 10), 2000);
        const result = await dispatchService.startBatchDispatch(category, language, size);
        res.json(result);
    } catch (error) {
        console.error('Error starting dispatch:', error);
        res.status(500).json({ error: error.message });
    }
});

// GET /api/admin/dispatch/status - Get current dispatch status
router.get('/api/admin/dispatch/status', authenticateToken, async (req, res) => {
    try {
        const status = dispatchService.getDispatchStatus();
        res.json({ success: true, status });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// GET /api/admin/dispatch/history - Get dispatch history
router.get('/api/admin/dispatch/history', authenticateToken, async (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit) || 20, 100);
        const history = await dispatchService.getDispatchHistory(limit);
        res.json({ success: true, history });
    } catch (error) {
        console.error('Error getting dispatch history:', error);
        res.status(500).json({ error: error.message });
    }
});

// GET /api/admin/dispatch/stats - Get dispatch statistics
router.get('/api/admin/dispatch/stats', authenticateToken, async (req, res) => {
    try {
        const stats = await dispatchService.getDispatchStats();
        res.json({ success: true, stats });
    } catch (error) {
        console.error('Error getting dispatch stats:', error);
        res.status(500).json({ error: error.message });
    }
});

// ==================== SCHEDULED EMAILS ====================

// POST /api/admin/dispatch/process-scheduled - Process due scheduled emails
router.post('/api/admin/dispatch/process-scheduled', authenticateToken, async (req, res) => {
    try {
        const result = await dispatchService.processScheduledEmails();
        res.json({ success: true, ...result });
    } catch (error) {
        console.error('Error processing scheduled emails:', error);
        res.status(500).json({ error: error.message });
    }
});

// ==================== TEST EMAIL ====================

// POST /api/admin/dispatch/test - Send test emails to a specific email address
router.post('/api/admin/dispatch/test', authenticateToken, async (req, res) => {
    try {
        const { email, category, language, emailNumbers } = req.body;

        if (!email || !category || !language) {
            return res.status(400).json({ error: 'email, category and language are required' });
        }

        const validCategories = ['checkout_abandon', 'sale_cancelled', 'funnel_abandon'];
        if (!validCategories.includes(category)) {
            return res.status(400).json({ error: `Invalid category. Must be one of: ${validCategories.join(', ')}` });
        }

        const validLanguages = ['en', 'es'];
        if (!validLanguages.includes(language)) {
            return res.status(400).json({ error: 'Invalid language. Must be en or es' });
        }

        const nums = emailNumbers || [1, 2, 3, 4];
        const result = await dispatchService.sendTestEmails(email, category, language, nums);
        res.json({ success: true, ...result });
    } catch (error) {
        console.error('Error sending test emails:', error);
        res.status(500).json({ error: error.message });
    }
});

// ==================== CLEANUP ====================

// POST /api/admin/dispatch/cleanup - Run cleanup of completed contacts
router.post('/api/admin/dispatch/cleanup', authenticateToken, async (req, res) => {
    try {
        const result = await dispatchService.cleanupCompletedContacts();
        res.json({ success: true, ...result });
    } catch (error) {
        console.error('Error running cleanup:', error);
        res.status(500).json({ error: error.message });
    }
});

module.exports = router;
