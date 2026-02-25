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
          fromemail: msgData.fromemail || 'noreply@xaimonitor.com',
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
  if (!email || !campaignId) {
    return res.status(400).json({ success: false, error: 'email and campaignId are required' });
  }
  try {
    // Use messageid=0 and action=send (same as real dispatch)
    // messageid=0 tells AC to use the campaign's default message
    const result = await acApiV1Get('campaign_send', {
      email,
      campaignid: campaignId,
      messageid: 0,
      type: 'mime',
      action: 'send'
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
          fromemail: msgData.fromemail || 'noreply@xaimonitor.com',
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

// ==================== UPDATE SENDER EMAIL ====================
router.post('/api/admin/tracking/update-sender', authenticateToken, async (req, res) => {
  try {
    const { messageId, fromEmail, fromName } = req.body;
    
    if (!messageId || !fromEmail) {
      return res.status(400).json({ success: false, error: 'messageId and fromEmail are required' });
    }

    // First get the current message data to preserve lists
    const msgData = await acApiV1Get('message_view', { id: messageId });
    if (msgData.result_code === 0) {
      return res.status(404).json({ success: false, error: 'Message not found' });
    }

    const editParams = {
      id: messageId,
      fromemail: fromEmail,
      fromname: fromName || msgData.fromname || 'ZapSpy.ai',
      reply2: fromEmail,
      subject: msgData.subject,
      html: msgData.html || msgData.htmlcontent || '',
      htmlconstructor: 'editor',
      format: 'html',
    };

    // Preserve list associations
    if (msgData.listslist) {
      const listIds = String(msgData.listslist).split(',');
      listIds.forEach((lid) => { editParams[`p[${lid.trim()}]`] = lid.trim(); });
    }

    const editResult = await acApiV1Post('message_edit', editParams);

    if (editResult.result_code === 0) {
      return res.json({ success: false, error: editResult.result_message });
    }

    res.json({ 
      success: true, 
      messageId, 
      oldFrom: msgData.fromemail,
      newFrom: fromEmail,
      subject: msgData.subject 
    });
  } catch (error) {
    console.error('Error updating sender:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ==================== BULK UPDATE ALL SENDERS ====================
router.post('/api/admin/tracking/update-all-senders', authenticateToken, async (req, res) => {
  try {
    const { fromEmail, fromName, dryRun } = req.body;
    
    if (!fromEmail) {
      return res.status(400).json({ success: false, error: 'fromEmail is required' });
    }

    const results = [];

    for (const campaign of CAMPAIGNS_TO_TRACK) {
      const { key, messageId } = campaign;

      try {
        const msgData = await acApiV1Get('message_view', { id: messageId });
        if (msgData.result_code === 0) {
          results.push({ key, messageId, status: 'error', reason: 'Message not found' });
          continue;
        }

        const currentFrom = msgData.fromemail;
        if (currentFrom === fromEmail) {
          results.push({ key, messageId, status: 'skipped', reason: 'Already using correct sender', currentFrom });
          continue;
        }

        if (dryRun) {
          results.push({ key, messageId, status: 'dry-run', currentFrom, newFrom: fromEmail });
          continue;
        }

        const editParams = {
          id: messageId,
          fromemail: fromEmail,
          fromname: fromName || 'ZapSpy.ai',
          reply2: fromEmail,
          subject: msgData.subject,
          html: msgData.html || msgData.htmlcontent || '',
          htmlconstructor: 'editor',
          format: 'html',
        };

        if (msgData.listslist) {
          const listIds = String(msgData.listslist).split(',');
          listIds.forEach((lid) => { editParams[`p[${lid.trim()}]`] = lid.trim(); });
        }

        const editResult = await acApiV1Post('message_edit', editParams);

        if (editResult.result_code === 0) {
          results.push({ key, messageId, status: 'error', reason: editResult.result_message, currentFrom });
        } else {
          results.push({ key, messageId, status: 'updated', oldFrom: currentFrom, newFrom: fromEmail });
        }

      } catch (error) {
        results.push({ key, messageId, status: 'error', reason: error.message });
      }

      await new Promise(resolve => setTimeout(resolve, 500));
    }

    const summary = {
      total: results.length,
      updated: results.filter(r => r.status === 'updated').length,
      skipped: results.filter(r => r.status === 'skipped').length,
      errors: results.filter(r => r.status === 'error').length,
      dryRun: results.filter(r => r.status === 'dry-run').length,
    };

    res.json({ success: true, dryRun: !!dryRun, summary, results });
  } catch (error) {
    console.error('Error bulk updating senders:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ==================== RECOVERY SALES METRICS ====================

// GET /api/admin/tracking/recovery-sales — Get recovery email sales from PerfectPay + Monetizze
router.get('/api/admin/tracking/recovery-sales', authenticateToken, async (req, res) => {
  try {
    const pool = require('../database');
    
    // ===== METHOD 1: Direct UTM match in transactions.raw_data =====
    // PerfectPay: raw_data->'metadata'->>'utm_source' = 'activecampaign'
    // Monetizze: raw_data->'venda'->>'utm_source' = 'ActiveCampaign'
    const utmQuery = await pool.queryRetry(`
      SELECT 
        t.transaction_id,
        t.email,
        t.name,
        t.value,
        t.status,
        t.funnel_language,
        t.funnel_source,
        t.created_at,
        -- PerfectPay UTMs
        COALESCE(
          t.raw_data->'metadata'->>'utm_source',
          t.raw_data->'venda'->>'utm_source'
        ) as utm_source,
        COALESCE(
          t.raw_data->'metadata'->>'utm_medium',
          t.raw_data->'venda'->>'utm_medium'
        ) as utm_medium,
        COALESCE(
          t.raw_data->'metadata'->>'utm_campaign',
          t.raw_data->'venda'->>'utm_campaign'
        ) as utm_campaign,
        COALESCE(
          t.raw_data->'metadata'->>'utm_content',
          t.raw_data->'venda'->>'utm_content'
        ) as utm_content,
        COALESCE(
          t.raw_data->'metadata'->>'utm_term',
          t.raw_data->'venda'->>'utm_term'
        ) as utm_term
      FROM transactions t
      WHERE (
        -- PerfectPay recovery emails
        (LOWER(t.raw_data->'metadata'->>'utm_source') = 'activecampaign'
         AND LOWER(t.raw_data->'metadata'->>'utm_medium') = 'email')
        OR
        (LOWER(t.raw_data->'metadata'->>'utm_term') = 'recovery')
        OR
        (LOWER(t.raw_data->'metadata'->>'utm_campaign') LIKE 'recovery_%')
        OR
        -- Monetizze recovery emails  
        (LOWER(t.raw_data->'venda'->>'utm_source') = 'activecampaign'
         AND LOWER(t.raw_data->'venda'->>'utm_medium') = 'email')
        OR
        (LOWER(t.raw_data->'venda'->>'utm_campaign') LIKE '%rmkt%')
        OR
        (LOWER(t.raw_data->'venda'->>'utm_campaign') LIKE 'recovery_%')
      )
      ORDER BY t.created_at DESC
    `);
    
    const allRecoveryTx = utmQuery.rows;
    
    // ===== METHOD 2: Cross-reference dispatch log with transactions =====
    // Find emails that received recovery emails AND later made a purchase
    const crossRefQuery = await pool.queryRetry(`
      SELECT DISTINCT
        t.transaction_id,
        t.email,
        t.name,
        t.value,
        t.status,
        t.funnel_language,
        t.funnel_source,
        t.created_at,
        d.category as dispatch_category,
        d.language as dispatch_language,
        d.email_num as dispatch_email_num,
        d.dispatched_at as email_sent_at
      FROM transactions t
      INNER JOIN email_dispatch_log d ON LOWER(t.email) = LOWER(d.email)
      WHERE d.status = 'sent'
        AND t.status = 'approved'
        AND t.created_at > d.dispatched_at
        AND t.transaction_id NOT IN (
          SELECT transaction_id FROM (
            SELECT transaction_id FROM transactions WHERE
              LOWER(raw_data->'metadata'->>'utm_source') = 'activecampaign'
              OR LOWER(raw_data->'metadata'->>'utm_term') = 'recovery'
              OR LOWER(raw_data->'metadata'->>'utm_campaign') LIKE 'recovery_%'
              OR LOWER(raw_data->'venda'->>'utm_source') = 'activecampaign'
              OR LOWER(raw_data->'venda'->>'utm_campaign') LIKE '%rmkt%'
              OR LOWER(raw_data->'venda'->>'utm_campaign') LIKE 'recovery_%'
          ) utm_matched
        )
      ORDER BY t.created_at DESC
    `);
    
    const crossRefTx = crossRefQuery.rows;
    
    // ===== AGGREGATE METRICS =====
    // UTM-tracked recovery sales
    const utmApproved = allRecoveryTx.filter(t => t.status === 'approved');
    const utmPending = allRecoveryTx.filter(t => ['pending_payment', 'abandoned_checkout'].includes(t.status));
    const utmRejected = allRecoveryTx.filter(t => ['cancelled', 'refunded', 'chargeback'].includes(t.status));
    
    const utmRevenue = utmApproved.reduce((sum, t) => sum + (parseFloat(t.value) || 0), 0);
    
    // Cross-referenced (attributed) sales
    const crossRefRevenue = crossRefTx.reduce((sum, t) => sum + (parseFloat(t.value) || 0), 0);
    
    // Combined unique approved sales
    const allApprovedIds = new Set([
      ...utmApproved.map(t => t.transaction_id),
      ...crossRefTx.map(t => t.transaction_id)
    ]);
    
    // Breakdown by campaign
    const campaignBreakdown = {};
    for (const tx of utmApproved) {
      const camp = tx.utm_campaign || 'unknown';
      if (!campaignBreakdown[camp]) {
        campaignBreakdown[camp] = { count: 0, revenue: 0, emails: [] };
      }
      campaignBreakdown[camp].count++;
      campaignBreakdown[camp].revenue += parseFloat(tx.value) || 0;
      campaignBreakdown[camp].emails.push(tx.email);
    }
    
    // Breakdown by category (from utm_campaign: recovery_checkout_abandon_en -> checkout_abandon)
    const categoryBreakdown = {};
    for (const tx of [...utmApproved, ...crossRefTx]) {
      let category = 'unknown';
      const camp = (tx.utm_campaign || tx.dispatch_category || '').toLowerCase();
      if (camp.includes('checkout_abandon') || camp.includes('checkout abandon')) category = 'checkout_abandon';
      else if (camp.includes('sale_cancelled') || camp.includes('sale cancelled')) category = 'sale_cancelled';
      else if (camp.includes('funnel_abandon') || camp.includes('funnel abandon')) category = 'funnel_abandon';
      else if (camp.includes('rmkt')) category = 'remarketing';
      else category = camp || 'unknown';
      
      if (!categoryBreakdown[category]) {
        categoryBreakdown[category] = { count: 0, revenue: 0 };
      }
      // Avoid double counting
      if (!categoryBreakdown[category][tx.transaction_id]) {
        categoryBreakdown[category][tx.transaction_id] = true;
        categoryBreakdown[category].count++;
        categoryBreakdown[category].revenue += parseFloat(tx.value) || 0;
      }
    }
    // Clean up tracking keys from breakdown
    for (const cat of Object.values(categoryBreakdown)) {
      for (const key of Object.keys(cat)) {
        if (key !== 'count' && key !== 'revenue') delete cat[key];
      }
    }
    
    // Get total dispatch stats for conversion rate
    let totalDispatched = 0;
    try {
      const dispatchCount = await pool.queryRetry(`
        SELECT COUNT(DISTINCT email) as total FROM email_dispatch_log WHERE status = 'sent'
      `);
      totalDispatched = parseInt(dispatchCount.rows[0]?.total || 0);
    } catch (e) {
      console.log('Error getting dispatch count:', e.message);
    }
    
    // Today's sales
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const todayApproved = utmApproved.filter(t => new Date(t.created_at) >= today);
    const todayRevenue = todayApproved.reduce((sum, t) => sum + (parseFloat(t.value) || 0), 0);
    
    // Daily breakdown (last 7 days)
    const dailyBreakdown = [];
    for (let i = 0; i < 7; i++) {
      const dayStart = new Date();
      dayStart.setDate(dayStart.getDate() - i);
      dayStart.setHours(0, 0, 0, 0);
      const dayEnd = new Date(dayStart);
      dayEnd.setDate(dayEnd.getDate() + 1);
      
      const daySales = utmApproved.filter(t => {
        const d = new Date(t.created_at);
        return d >= dayStart && d < dayEnd;
      });
      
      dailyBreakdown.push({
        date: dayStart.toISOString().split('T')[0],
        count: daySales.length,
        revenue: daySales.reduce((sum, t) => sum + (parseFloat(t.value) || 0), 0)
      });
    }
    
    res.json({
      success: true,
      summary: {
        totalApproved: utmApproved.length,
        totalRevenue: Math.round(utmRevenue * 100) / 100,
        totalPending: utmPending.length,
        totalRejected: utmRejected.length,
        totalAllEvents: allRecoveryTx.length,
        crossRefSales: crossRefTx.length,
        crossRefRevenue: Math.round(crossRefRevenue * 100) / 100,
        combinedUniqueSales: allApprovedIds.size,
        todaySales: todayApproved.length,
        todayRevenue: Math.round(todayRevenue * 100) / 100,
        totalDispatched,
        conversionRate: totalDispatched > 0 
          ? ((allApprovedIds.size / totalDispatched) * 100).toFixed(2) 
          : '0.00'
      },
      categoryBreakdown,
      campaignBreakdown,
      dailyBreakdown,
      recentSales: utmApproved.slice(0, 20).map(t => ({
        transaction_id: t.transaction_id,
        email: t.email,
        name: t.name,
        value: parseFloat(t.value) || 0,
        campaign: t.utm_campaign,
        language: t.funnel_language,
        created_at: t.created_at
      }))
    });
  } catch (error) {
    console.error('Error getting recovery sales:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;
