/**
 * Email Tracking Admin API Routes
 * 
 * Provides endpoints for the admin panel to view email tracking metrics:
 * - GET /api/admin/tracking/metrics — Detailed metrics by category/language/email
 * - GET /api/admin/tracking/summary — Summary totals (opens, clicks, rates)
 * - GET /api/admin/tracking/events — Recent tracking events
 * - GET /api/admin/tracking/daily — Daily metrics for charts
 */

const express = require('express');
const router = express.Router();
const { authenticateToken } = require('../middleware');
const trackingService = require('../services/email-tracking');
const { AC_API_URL, AC_API_KEY } = require('../config');

// GET /api/admin/tracking/metrics — Detailed metrics by category/language/emailNum
router.get('/api/admin/tracking/metrics', authenticateToken, async (req, res) => {
  try {
    const { category, language, dateFrom, dateTo } = req.query;
    const metrics = await trackingService.getMetrics({ category, language, dateFrom, dateTo });
    res.json({ success: true, data: metrics });
  } catch (error) {
    console.error('Error getting tracking metrics:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/admin/tracking/summary — Summary metrics (aggregated totals)
router.get('/api/admin/tracking/summary', authenticateToken, async (req, res) => {
  try {
    const rows = await trackingService.getSummaryMetrics();
    // Aggregate all rows into a single summary object
    const totals = rows.reduce((acc, row) => {
      acc.total_sent += row.total_sent || 0;
      acc.unique_opens += row.unique_opens || 0;
      acc.unique_clicks += row.unique_clicks || 0;
      acc.unique_contacts += row.unique_contacts || 0;
      acc.contacts_opened += row.contacts_opened || 0;
      acc.contacts_clicked += row.contacts_clicked || 0;
      return acc;
    }, { total_sent: 0, unique_opens: 0, unique_clicks: 0, unique_contacts: 0, contacts_opened: 0, contacts_clicked: 0 });
    
    totals.open_rate = totals.total_sent > 0 ? (totals.unique_opens / totals.total_sent * 100).toFixed(1) : '0.0';
    totals.click_rate = totals.total_sent > 0 ? (totals.unique_clicks / totals.total_sent * 100).toFixed(1) : '0.0';
    
    res.json({ success: true, data: totals, breakdown: rows });
  } catch (error) {
    console.error('Error getting tracking summary:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/admin/tracking/events — Recent events
router.get('/api/admin/tracking/events', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const events = await trackingService.getRecentEvents(limit);
    res.json({ success: true, data: events });
  } catch (error) {
    console.error('Error getting tracking events:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/admin/tracking/daily — Daily metrics for charts
router.get('/api/admin/tracking/daily', authenticateToken, async (req, res) => {
  try {
    const days = Math.min(parseInt(req.query.days) || 30, 90);
    const daily = await trackingService.getDailyMetrics(days);
    res.json({ success: true, data: daily });
  } catch (error) {
    console.error('Error getting daily metrics:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ==================== INJECT TRACKING INTO AC CAMPAIGNS ====================

const CAMPAIGNS_TO_TRACK = [
  { key: 'checkout_abandon_en_1', messageId: 256, category: 'checkout_abandon', language: 'en', emailNum: 1 },
  { key: 'checkout_abandon_en_2', messageId: 257, category: 'checkout_abandon', language: 'en', emailNum: 2 },
  { key: 'checkout_abandon_en_3', messageId: 258, category: 'checkout_abandon', language: 'en', emailNum: 3 },
  { key: 'checkout_abandon_en_4', messageId: 259, category: 'checkout_abandon', language: 'en', emailNum: 4 },
  { key: 'checkout_abandon_es_1', messageId: 260, category: 'checkout_abandon', language: 'es', emailNum: 1 },
  { key: 'checkout_abandon_es_2', messageId: 261, category: 'checkout_abandon', language: 'es', emailNum: 2 },
  { key: 'checkout_abandon_es_3', messageId: 262, category: 'checkout_abandon', language: 'es', emailNum: 3 },
  { key: 'checkout_abandon_es_4', messageId: 263, category: 'checkout_abandon', language: 'es', emailNum: 4 },
  { key: 'sale_cancelled_en_1', messageId: 264, category: 'sale_cancelled', language: 'en', emailNum: 1 },
  { key: 'sale_cancelled_en_2', messageId: 265, category: 'sale_cancelled', language: 'en', emailNum: 2 },
  { key: 'sale_cancelled_en_3', messageId: 266, category: 'sale_cancelled', language: 'en', emailNum: 3 },
  { key: 'sale_cancelled_en_4', messageId: 267, category: 'sale_cancelled', language: 'en', emailNum: 4 },
  { key: 'sale_cancelled_es_1', messageId: 268, category: 'sale_cancelled', language: 'es', emailNum: 1 },
  { key: 'sale_cancelled_es_2', messageId: 269, category: 'sale_cancelled', language: 'es', emailNum: 2 },
  { key: 'sale_cancelled_es_3', messageId: 270, category: 'sale_cancelled', language: 'es', emailNum: 3 },
  { key: 'sale_cancelled_es_4', messageId: 271, category: 'sale_cancelled', language: 'es', emailNum: 4 },
  { key: 'funnel_abandon_en_1', messageId: 272, category: 'funnel_abandon', language: 'en', emailNum: 1 },
  { key: 'funnel_abandon_en_2', messageId: 273, category: 'funnel_abandon', language: 'en', emailNum: 2 },
  { key: 'funnel_abandon_en_3', messageId: 274, category: 'funnel_abandon', language: 'en', emailNum: 3 },
  { key: 'funnel_abandon_en_4', messageId: 275, category: 'funnel_abandon', language: 'en', emailNum: 4 },
  { key: 'funnel_abandon_es_1', messageId: 276, category: 'funnel_abandon', language: 'es', emailNum: 1 },
  { key: 'funnel_abandon_es_2', messageId: 277, category: 'funnel_abandon', language: 'es', emailNum: 2 },
  { key: 'funnel_abandon_es_3', messageId: 278, category: 'funnel_abandon', language: 'es', emailNum: 3 },
  { key: 'funnel_abandon_es_4', messageId: 279, category: 'funnel_abandon', language: 'es', emailNum: 4 },
];

const TRACKING_BASE = 'https://zapspy-funnel-production.up.railway.app';

async function acApiV1Get(action, params = {}) {
  const queryParams = new URLSearchParams({ api_key: AC_API_KEY, api_action: action, api_output: 'json', ...params });
  const url = `${AC_API_URL}/admin/api.php?${queryParams.toString()}`;
  const response = await fetch(url, { method: 'GET' });
  return await response.json();
}

async function acApiV1Post(action, formData) {
  const url = `${AC_API_URL}/admin/api.php?api_action=${action}&api_output=json`;
  const body = new URLSearchParams({ api_key: AC_API_KEY, ...formData });
  const response = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: body.toString() });
  return await response.json();
}

function injectTrackingIntoHtml(html, category, language, emailNum) {
  if (html.includes('/t/open?') || html.includes('zapspy-funnel-production.up.railway.app/t/')) {
    return { html, modified: false, reason: 'Already has tracking' };
  }

  let modified = html;

  // 1. Inject pixel before </body>
  const pixel = `<img src="${TRACKING_BASE}/t/open?e=%EMAIL%&c=${category}&l=${language}&n=${emailNum}" width="1" height="1" style="display:none;border:0;height:1px;width:1px;" alt="" />`;
  if (modified.includes('</body>')) {
    modified = modified.replace('</body>', `${pixel}\n</body>`);
  } else {
    modified += `\n${pixel}`;
  }

  // 2. Wrap CTA links with click tracking
  const linkRegex = /<a\s+([^>]*?)href="(https?:\/\/[^"]+)"([^>]*?)>/gi;
  modified = modified.replace(linkRegex, (match, before, url, after) => {
    if (url.includes('%UNSUBSCRIBELINK%') || url.startsWith('mailto:') || url.includes('/privacy') || url.includes('/t/click') || url.includes('zapspy-funnel-production')) {
      return match;
    }
    const trackedUrl = `${TRACKING_BASE}/t/click?e=%EMAIL%&c=${category}&l=${language}&n=${emailNum}&url=${encodeURIComponent(url)}`;
    return `<a ${before}href="${trackedUrl}"${after}>`;
  });

  return { html: modified, modified: true, reason: 'Tracking injected' };
}

// GET /api/admin/tracking/debug-message/:id — Debug: view raw message_view response
router.get('/api/admin/tracking/debug-message/:id', authenticateToken, async (req, res) => {
  try {
    const msgData = await acApiV1Get('message_view', { id: req.params.id });
    // Remove HTML to keep response small
    const { html, htmlcontent, text, ...meta } = msgData;
    res.json({ success: true, meta, htmlLength: (html||'').length });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/admin/tracking/inject — Inject tracking into all AC campaign templates
router.post('/api/admin/tracking/inject', authenticateToken, async (req, res) => {
  const dryRun = req.query.dry_run === 'true';
  const results = [];

  try {
    for (const campaign of CAMPAIGNS_TO_TRACK) {
      const { key, messageId, category, language, emailNum } = campaign;
      try {
        // 1. Get current HTML
        const msgData = await acApiV1Get('message_view', { id: messageId });
        const currentHtml = msgData.html || msgData.htmlcontent || msgData.text || '';

        if (!currentHtml || currentHtml.length < 50) {
          results.push({ key, status: 'skipped', reason: 'No HTML content', htmlLength: currentHtml?.length || 0 });
          continue;
        }

        // 2. Inject tracking
        const { html: updatedHtml, modified, reason } = injectTrackingIntoHtml(currentHtml, category, language, emailNum);

        if (!modified) {
          results.push({ key, status: 'skipped', reason });
          continue;
        }

        if (dryRun) {
          results.push({ key, status: 'dry-run', reason: `Would update (${currentHtml.length} -> ${updatedHtml.length} chars)` });
          continue;
        }

        // 3. Save updated HTML - must include required fields from original message
        const editParams = {
          id: messageId,
          html: updatedHtml,
          htmlconstructor: 'editor',
          format: 'html',
        };
        // Include list IDs from original message data
        if (msgData.listslist) {
          // listslist can be comma-separated list IDs
          const listIds = String(msgData.listslist).split(',');
          listIds.forEach((lid, idx) => { editParams[`p[${lid.trim()}]`] = lid.trim(); });
        } else {
          // Fallback: determine list from campaign key
          const listMap = {
            'checkout_abandon_en': '7', 'checkout_abandon_es': '8',
            'sale_cancelled_en': '9', 'sale_cancelled_es': '10',
            'funnel_abandon_en': '5', 'funnel_abandon_es': '6',
          };
          const listKey = `${category}_${language}`;
          const listId = listMap[listKey];
          if (listId) editParams[`p[${listId}]`] = listId;
        }
        const editResult = await acApiV1Post('message_edit', editParams);
        
        if (editResult.result_code === 0) {
          results.push({ key, status: 'error', reason: editResult.result_message || 'message_edit failed' });
        } else {
          results.push({ key, status: 'updated', reason: `Tracking injected (${currentHtml.length} -> ${updatedHtml.length} chars)` });
        }

      } catch (error) {
        results.push({ key, status: 'error', reason: error.message });
      }

      // Rate limiting
      await new Promise(resolve => setTimeout(resolve, 300));
    }

    const summary = {
      total: results.length,
      updated: results.filter(r => r.status === 'updated').length,
      skipped: results.filter(r => r.status === 'skipped').length,
      errors: results.filter(r => r.status === 'error').length,
      dryRun: results.filter(r => r.status === 'dry-run').length,
    };

    res.json({ success: true, dryRun, summary, results });
  } catch (error) {
    console.error('Error injecting tracking:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;
