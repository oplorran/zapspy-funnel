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

        // 3. Save updated HTML - must include all required fields from original message
        const editParams = {
          id: messageId,
          html: updatedHtml,
          htmlconstructor: 'editor',
          format: msgData.format || 'mime',
          charset: msgData.charset || 'utf-8',
          encoding: msgData.encoding || 'quoted-printable',
          subject: msgData.subject || '',
          fromemail: msgData.fromemail || 'noreply@zapspy.ai',
          fromname: msgData.fromname || 'ZapSpy.ai',
          reply2: msgData.reply2 || 'support@zapspy.ai',
          priority: msgData.priority || '3',
          textcopy: msgData.textcopy || '',
        };
        // Include list IDs from original message data
        if (msgData.listslist) {
          const listIds = String(msgData.listslist).split(',');
          listIds.forEach((lid) => { editParams[`p[${lid.trim()}]`] = lid.trim(); });
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

// ==================== AC CAMPAIGN REPORTS ====================

// GET /api/admin/tracking/ac-reports — Get native AC campaign report data (opens, clicks, bounces)
router.get('/api/admin/tracking/ac-reports', authenticateToken, async (req, res) => {
  try {
    const results = [];
    
    // Map campaignId from CAMPAIGN_MAP in email-dispatch.js
    const CAMPAIGN_MAP = {
      checkout_abandon_en_1: { campaignId: 194, messageId: 256 },
      checkout_abandon_en_2: { campaignId: 195, messageId: 257 },
      checkout_abandon_en_3: { campaignId: 196, messageId: 258 },
      checkout_abandon_en_4: { campaignId: 197, messageId: 259 },
      checkout_abandon_es_1: { campaignId: 198, messageId: 260 },
      checkout_abandon_es_2: { campaignId: 199, messageId: 261 },
      checkout_abandon_es_3: { campaignId: 200, messageId: 262 },
      checkout_abandon_es_4: { campaignId: 201, messageId: 263 },
      sale_cancelled_en_1: { campaignId: 202, messageId: 264 },
      sale_cancelled_en_2: { campaignId: 203, messageId: 265 },
      sale_cancelled_en_3: { campaignId: 204, messageId: 266 },
      sale_cancelled_en_4: { campaignId: 205, messageId: 267 },
      sale_cancelled_es_1: { campaignId: 206, messageId: 268 },
      sale_cancelled_es_2: { campaignId: 207, messageId: 269 },
      sale_cancelled_es_3: { campaignId: 208, messageId: 270 },
      sale_cancelled_es_4: { campaignId: 209, messageId: 271 },
      funnel_abandon_en_1: { campaignId: 210, messageId: 272 },
      funnel_abandon_en_2: { campaignId: 211, messageId: 273 },
      funnel_abandon_en_3: { campaignId: 212, messageId: 274 },
      funnel_abandon_en_4: { campaignId: 213, messageId: 275 },
      funnel_abandon_es_1: { campaignId: 214, messageId: 276 },
      funnel_abandon_es_2: { campaignId: 215, messageId: 277 },
      funnel_abandon_es_3: { campaignId: 216, messageId: 278 },
      funnel_abandon_es_4: { campaignId: 217, messageId: 279 },
    };

    for (const [key, { campaignId }] of Object.entries(CAMPAIGN_MAP)) {
      try {
        const report = await acApiV1Get('campaign_report_totals', { campaignid: campaignId });
        // key format: checkout_abandon_en_1, sale_cancelled_es_2, funnel_abandon_en_3
        const parts = key.split('_');
        const emailNum = parseInt(parts.pop()); // last part is number
        const language = parts.pop(); // second to last is language
        const category = parts.join('_'); // rest is category
        results.push({
          key,
          category,
          language,
          emailNum,
          campaignId,
          totals: {
            sends: parseInt(report.send_amt || report.total_amt || report.sends || 0),
            uniqueOpens: parseInt(report.uniqueopens || report.verified_unique_opens || 0),
            opens: parseInt(report.opens || report.uniqueopens || 0),
            clicks: parseInt(report.subscriberclicks || report.uniquelinkclicks || 0),
            uniqueClicks: parseInt(report.uniquelinkclicks || 0),
            bounces: parseInt(report.totalbounces || report.bounces || 0),
            softBounces: parseInt(report.softbounces || 0),
            hardBounces: parseInt(report.hardbounces || 0),
            unsubscribes: parseInt(report.unsubscribes || 0),
          },
          raw: report
        });
      } catch (err) {
        results.push({ key, campaignId, error: err.message });
      }
      await new Promise(r => setTimeout(r, 200));
    }

    // Aggregate totals
    const totals = results.reduce((acc, r) => {
      if (r.totals) {
        acc.sends += r.totals.sends;
        acc.uniqueOpens += r.totals.uniqueOpens;
        acc.opens += r.totals.opens;
        acc.clicks += r.totals.clicks;
        acc.uniqueClicks += r.totals.uniqueClicks;
        acc.bounces += r.totals.bounces;
        acc.unsubscribes += r.totals.unsubscribes;
      }
      return acc;
    }, { sends: 0, uniqueOpens: 0, opens: 0, clicks: 0, uniqueClicks: 0, bounces: 0, unsubscribes: 0 });

    totals.openRate = totals.sends > 0 ? (totals.uniqueOpens / totals.sends * 100).toFixed(1) : '0.0';
    totals.clickRate = totals.sends > 0 ? (totals.uniqueClicks / totals.sends * 100).toFixed(1) : '0.0';

    res.json({ success: true, totals, campaigns: results });
  } catch (error) {
    console.error('Error getting AC reports:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/admin/tracking/test-send — Send a test email to verify delivery
router.post('/api/admin/tracking/test-send', authenticateToken, async (req, res) => {
  const { email, campaignId, messageId } = req.body;
  if (!email || !campaignId || !messageId) {
    return res.status(400).json({ success: false, error: 'email, campaignId, and messageId are required' });
  }
  try {
    const result = await acApiV1Get('campaign_send', {
      email,
      campaignid: campaignId,
      messageid: messageId,
      type: 'mime',
      action: 'test'
    });
    res.json({ success: true, result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/admin/tracking/verify-html/:messageId — Check if tracking is present in a campaign's HTML
router.get('/api/admin/tracking/verify-html/:messageId', authenticateToken, async (req, res) => {
  try {
    const msgData = await acApiV1Get('message_view', { id: req.params.messageId });
    const html = msgData.html || msgData.htmlcontent || '';
    const hasPixel = html.includes('/t/open?');
    const hasClickTracking = html.includes('/t/click?');
    const hasACTracking = html.includes('lt.php') || html.includes('trackcmp');
    
    // Find all links
    const linkRegex = /href="([^"]+)"/gi;
    const links = [];
    let match;
    while ((match = linkRegex.exec(html)) !== null) {
      links.push(match[1]);
    }
    
    res.json({
      success: true,
      messageId: req.params.messageId,
      htmlLength: html.length,
      hasCustomPixel: hasPixel,
      hasCustomClickTracking: hasClickTracking,
      hasACNativeTracking: hasACTracking,
      links,
      htmlPreview: html.substring(html.length - 500) // Last 500 chars to check pixel
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ==================== UPDATE CHECKOUT LINKS ====================

// Link mapping: progressive discounts for all categories
const CHECKOUT_LINKS = {
  en: {
    1: 'https://go.centerpag.com/PPU38CQ848P', // $47
    2: 'https://go.centerpag.com/PPU38CQ848Q', // $37
    3: 'https://go.centerpag.com/PPU38CQ848R', // $27
    4: 'https://go.centerpag.com/PPU38CQ85IH', // $17
  },
  es: {
    1: 'https://go.centerpag.com/PPU38CQ848S', // $37
    2: 'https://go.centerpag.com/PPU38CQ848T', // $27
    3: 'https://go.centerpag.com/PPU38CQ848U', // $18
    4: 'https://go.centerpag.com/PPU38CQ85II', // $9
  }
};

// POST /api/admin/tracking/update-links — Update all campaign templates with correct checkout links
router.post('/api/admin/tracking/update-links', authenticateToken, async (req, res) => {
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
          results.push({ key, messageId, status: 'skipped', reason: 'No HTML content' });
          continue;
        }

        const correctLink = CHECKOUT_LINKS[language][emailNum];
        let updatedHtml = currentHtml;
        let changes = [];

        // Replace broken placeholders {{checkout_link_30off}} and {{checkout_link_50off}}
        if (updatedHtml.includes('{{checkout_link_30off}}')) {
          updatedHtml = updatedHtml.replace(/\{\{checkout_link_30off\}\}/g, correctLink);
          changes.push('Replaced {{checkout_link_30off}}');
        }
        if (updatedHtml.includes('{{checkout_link_50off}}')) {
          updatedHtml = updatedHtml.replace(/\{\{checkout_link_50off\}\}/g, correctLink);
          changes.push('Replaced {{checkout_link_50off}}');
        }

        // Replace wrong checkout links with correct ones for this email number
        // For E2: replace full-price links with discount links
        if (emailNum === 2) {
          const fullPriceLink = CHECKOUT_LINKS[language][1]; // full price link
          if (updatedHtml.includes(fullPriceLink) && fullPriceLink !== correctLink) {
            updatedHtml = updatedHtml.split(fullPriceLink).join(correctLink);
            changes.push(`Replaced full-price link with discount link`);
          }
          // Also check inside tracked URLs (encoded)
          const encodedFull = encodeURIComponent(fullPriceLink);
          const encodedCorrect = encodeURIComponent(correctLink);
          if (updatedHtml.includes(encodedFull) && encodedFull !== encodedCorrect) {
            updatedHtml = updatedHtml.split(encodedFull).join(encodedCorrect);
            changes.push(`Replaced encoded full-price link with discount link`);
          }
        }

        // Also inject tracking pixel if not present
        if (!updatedHtml.includes('/t/open?')) {
          const pixel = `<img src="${TRACKING_BASE}/t/open?e=%EMAIL%&c=${category}&l=${language}&n=${emailNum}" width="1" height="1" style="display:none;border:0;" alt="" />`;
          if (updatedHtml.includes('</body>')) {
            updatedHtml = updatedHtml.replace('</body>', `${pixel}\n</body>`);
          } else {
            updatedHtml += `\n${pixel}`;
          }
          changes.push('Injected tracking pixel');
        }

        // Wrap CTA links with click tracking if not already tracked
        if (!updatedHtml.includes('/t/click?')) {
          const linkRegex = /<a\s+([^>]*?)href="(https?:\/\/go\.centerpag\.com[^"]+)"([^>]*?)>/gi;
          updatedHtml = updatedHtml.replace(linkRegex, (match, before, url, after) => {
            const trackedUrl = `${TRACKING_BASE}/t/click?e=%EMAIL%&c=${category}&l=${language}&n=${emailNum}&url=${encodeURIComponent(url)}`;
            return `<a ${before}href="${trackedUrl}"${after}>`;
          });
          changes.push('Wrapped CTA links with click tracking');
        }

        if (changes.length === 0) {
          results.push({ key, messageId, status: 'skipped', reason: 'No changes needed', correctLink });
          continue;
        }

        if (dryRun) {
          results.push({ key, messageId, status: 'dry-run', changes, correctLink });
          continue;
        }

        // 2. Save updated HTML
        const editParams = {
          id: messageId,
          html: updatedHtml,
          htmlconstructor: 'editor',
          format: msgData.format || 'mime',
          charset: msgData.charset || 'utf-8',
          encoding: msgData.encoding || 'quoted-printable',
          subject: msgData.subject || '',
          fromemail: msgData.fromemail || 'noreply@zapspy.ai',
          fromname: msgData.fromname || 'ZapSpy.ai',
          reply2: msgData.reply2 || 'support@zapspy.ai',
          priority: msgData.priority || '3',
          textcopy: msgData.textcopy || '',
        };
        if (msgData.listslist) {
          const listIds = String(msgData.listslist).split(',');
          listIds.forEach((lid) => { editParams[`p[${lid.trim()}]`] = lid.trim(); });
        }
        const editResult = await acApiV1Post('message_edit', editParams);

        if (editResult.result_code === 0) {
          results.push({ key, messageId, status: 'error', reason: editResult.result_message, changes });
        } else {
          results.push({ key, messageId, status: 'updated', changes, correctLink });
        }

      } catch (error) {
        results.push({ key, messageId, status: 'error', reason: error.message });
      }

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
    console.error('Error updating links:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;
