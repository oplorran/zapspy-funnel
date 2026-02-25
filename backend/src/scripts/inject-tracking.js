/**
 * Inject Tracking Pixel and Click Tracking into AC Campaign Templates
 * 
 * This script:
 * 1. Fetches the HTML of each campaign message via AC API v1 (message_view)
 * 2. Injects a tracking pixel before </body>
 * 3. Wraps CTA links with click tracking redirect
 * 4. Saves the updated HTML back via AC API v1 (message_edit)
 * 
 * Usage: node src/scripts/inject-tracking.js [--dry-run]
 */

require('dotenv').config();
const { AC_API_URL, AC_API_KEY } = require('../config');

const BASE_URL = 'https://zapspy-funnel-production.up.railway.app';

// Campaign mapping: key -> { campaignId, messageId, category, language, emailNum }
const CAMPAIGNS = [
  { key: 'checkout_abandon_en_1', campaignId: 191, messageId: 256, category: 'checkout_abandon', language: 'en', emailNum: 1 },
  { key: 'checkout_abandon_en_2', campaignId: 192, messageId: 257, category: 'checkout_abandon', language: 'en', emailNum: 2 },
  { key: 'checkout_abandon_en_3', campaignId: 193, messageId: 258, category: 'checkout_abandon', language: 'en', emailNum: 3 },
  { key: 'checkout_abandon_en_4', campaignId: 194, messageId: 259, category: 'checkout_abandon', language: 'en', emailNum: 4 },
  { key: 'checkout_abandon_es_1', campaignId: 195, messageId: 260, category: 'checkout_abandon', language: 'es', emailNum: 1 },
  { key: 'checkout_abandon_es_2', campaignId: 196, messageId: 261, category: 'checkout_abandon', language: 'es', emailNum: 2 },
  { key: 'checkout_abandon_es_3', campaignId: 197, messageId: 262, category: 'checkout_abandon', language: 'es', emailNum: 3 },
  { key: 'checkout_abandon_es_4', campaignId: 198, messageId: 263, category: 'checkout_abandon', language: 'es', emailNum: 4 },
  { key: 'sale_cancelled_en_1', campaignId: 199, messageId: 264, category: 'sale_cancelled', language: 'en', emailNum: 1 },
  { key: 'sale_cancelled_en_2', campaignId: 200, messageId: 265, category: 'sale_cancelled', language: 'en', emailNum: 2 },
  { key: 'sale_cancelled_en_3', campaignId: 201, messageId: 266, category: 'sale_cancelled', language: 'en', emailNum: 3 },
  { key: 'sale_cancelled_en_4', campaignId: 202, messageId: 267, category: 'sale_cancelled', language: 'en', emailNum: 4 },
  { key: 'sale_cancelled_es_1', campaignId: 203, messageId: 268, category: 'sale_cancelled', language: 'es', emailNum: 1 },
  { key: 'sale_cancelled_es_2', campaignId: 204, messageId: 269, category: 'sale_cancelled', language: 'es', emailNum: 2 },
  { key: 'sale_cancelled_es_3', campaignId: 205, messageId: 270, category: 'sale_cancelled', language: 'es', emailNum: 3 },
  { key: 'sale_cancelled_es_4', campaignId: 206, messageId: 271, category: 'sale_cancelled', language: 'es', emailNum: 4 },
  { key: 'funnel_abandon_en_1', campaignId: 207, messageId: 272, category: 'funnel_abandon', language: 'en', emailNum: 1 },
  { key: 'funnel_abandon_en_2', campaignId: 208, messageId: 273, category: 'funnel_abandon', language: 'en', emailNum: 2 },
  { key: 'funnel_abandon_en_3', campaignId: 209, messageId: 274, category: 'funnel_abandon', language: 'en', emailNum: 3 },
  { key: 'funnel_abandon_en_4', campaignId: 210, messageId: 275, category: 'funnel_abandon', language: 'en', emailNum: 4 },
  { key: 'funnel_abandon_es_1', campaignId: 211, messageId: 276, category: 'funnel_abandon', language: 'es', emailNum: 1 },
  { key: 'funnel_abandon_es_2', campaignId: 212, messageId: 277, category: 'funnel_abandon', language: 'es', emailNum: 2 },
  { key: 'funnel_abandon_es_3', campaignId: 213, messageId: 278, category: 'funnel_abandon', language: 'es', emailNum: 3 },
  { key: 'funnel_abandon_es_4', campaignId: 214, messageId: 279, category: 'funnel_abandon', language: 'es', emailNum: 4 },
];

const DRY_RUN = process.argv.includes('--dry-run');

async function acApiV1(action, params = {}, method = 'GET') {
  const queryParams = new URLSearchParams({
    api_key: AC_API_KEY,
    api_action: action,
    api_output: 'json',
    ...params
  });

  const url = `${AC_API_URL}/admin/api.php?${queryParams.toString()}`;
  const response = await fetch(url, { method });
  return await response.json();
}

async function acApiV1Post(action, formData) {
  const url = `${AC_API_URL}/admin/api.php?api_action=${action}&api_output=json`;
  
  const body = new URLSearchParams({
    api_key: AC_API_KEY,
    ...formData
  });

  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString()
  });
  return await response.json();
}

function generatePixel(category, language, emailNum) {
  return `<img src="${BASE_URL}/t/open?e=%EMAIL%&c=${category}&l=${language}&n=${emailNum}" width="1" height="1" style="display:none;border:0;height:1px;width:1px;" alt="" />`;
}

function wrapCTALinks(html, category, language, emailNum) {
  // Find all <a href="..."> links that are NOT unsubscribe, mailto, or privacy links
  const linkRegex = /<a\s+([^>]*?)href="(https?:\/\/[^"]+)"([^>]*?)>/gi;
  
  return html.replace(linkRegex, (match, before, url, after) => {
    // Skip unsubscribe links, mailto links, privacy links, and already-tracked links
    if (url.includes('%UNSUBSCRIBELINK%') || 
        url.startsWith('mailto:') || 
        url.includes('/privacy') ||
        url.includes('/t/click') ||
        url.includes('zapspy-funnel-production')) {
      return match;
    }
    
    const trackedUrl = `${BASE_URL}/t/click?e=%EMAIL%&c=${category}&l=${language}&n=${emailNum}&url=${encodeURIComponent(url)}`;
    return `<a ${before}href="${trackedUrl}"${after}>`;
  });
}

function hasTrackingPixel(html) {
  return html.includes('/t/open?') || html.includes('zapspy-funnel-production.up.railway.app/t/');
}

function injectTracking(html, category, language, emailNum) {
  // Check if tracking is already injected
  if (hasTrackingPixel(html)) {
    return { html, modified: false, reason: 'Already has tracking' };
  }

  let modified = html;

  // 1. Inject pixel before </body>
  const pixel = generatePixel(category, language, emailNum);
  if (modified.includes('</body>')) {
    modified = modified.replace('</body>', `${pixel}\n</body>`);
  } else {
    // Fallback: append at the end
    modified += `\n${pixel}`;
  }

  // 2. Wrap CTA links with click tracking
  modified = wrapCTALinks(modified, category, language, emailNum);

  return { html: modified, modified: true, reason: 'Tracking injected' };
}

async function getMessageHtml(messageId) {
  // AC API v1: message_view returns the message details including HTML
  const result = await acApiV1('message_view', { id: messageId });
  
  if (result.result_code === 0) {
    throw new Error(`message_view failed for ${messageId}: ${result.result_message}`);
  }

  return result.html || result.htmlcontent || result.text || '';
}

async function updateMessageHtml(messageId, html) {
  // AC API v1: message_edit updates the message
  const result = await acApiV1Post('message_edit', {
    id: messageId,
    html: html,
    htmlconstructor: 'editor'
  });

  if (result.result_code === 0) {
    throw new Error(`message_edit failed for ${messageId}: ${result.result_message}`);
  }

  return result;
}

async function processCampaign(campaign) {
  const { key, messageId, category, language, emailNum } = campaign;
  
  console.log(`\n📧 Processing: ${key} (messageId: ${messageId})`);
  
  try {
    // 1. Get current HTML
    const currentHtml = await getMessageHtml(messageId);
    
    if (!currentHtml || currentHtml.length < 50) {
      console.log(`  ⚠️  No HTML content found (length: ${currentHtml?.length || 0})`);
      return { key, status: 'skipped', reason: 'No HTML content' };
    }

    console.log(`  📄 Current HTML: ${currentHtml.length} chars`);

    // 2. Inject tracking
    const { html: updatedHtml, modified, reason } = injectTracking(currentHtml, category, language, emailNum);

    if (!modified) {
      console.log(`  ℹ️  ${reason}`);
      return { key, status: 'skipped', reason };
    }

    console.log(`  ✏️  Modified HTML: ${updatedHtml.length} chars (+${updatedHtml.length - currentHtml.length})`);

    // 3. Save updated HTML
    if (DRY_RUN) {
      console.log(`  🔍 DRY RUN - would save ${updatedHtml.length} chars`);
      return { key, status: 'dry-run', reason: 'Would update' };
    }

    const result = await updateMessageHtml(messageId, updatedHtml);
    console.log(`  ✅ Updated successfully`);
    return { key, status: 'updated', reason: 'Tracking injected' };

  } catch (error) {
    console.error(`  ❌ Error: ${error.message}`);
    return { key, status: 'error', reason: error.message };
  }
}

async function main() {
  console.log('🔧 ActiveCampaign Email Tracking Injector');
  console.log(`📡 AC API URL: ${AC_API_URL}`);
  console.log(`🔑 AC API Key: ${AC_API_KEY ? AC_API_KEY.substring(0, 8) + '...' : 'NOT SET'}`);
  console.log(`🌐 Tracking Base URL: ${BASE_URL}`);
  console.log(`📝 Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}`);
  console.log(`📊 Total campaigns: ${CAMPAIGNS.length}`);
  console.log('---');

  if (!AC_API_URL || !AC_API_KEY) {
    console.error('❌ AC_API_URL or AC_API_KEY not configured!');
    process.exit(1);
  }

  const results = [];

  for (const campaign of CAMPAIGNS) {
    const result = await processCampaign(campaign);
    results.push(result);
    
    // Rate limiting: wait 500ms between API calls
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  // Summary
  console.log('\n\n==================== SUMMARY ====================');
  const updated = results.filter(r => r.status === 'updated').length;
  const skipped = results.filter(r => r.status === 'skipped').length;
  const errors = results.filter(r => r.status === 'error').length;
  const dryRun = results.filter(r => r.status === 'dry-run').length;

  console.log(`✅ Updated: ${updated}`);
  console.log(`⏭️  Skipped: ${skipped}`);
  console.log(`❌ Errors: ${errors}`);
  if (DRY_RUN) console.log(`🔍 Dry-run: ${dryRun}`);

  if (errors > 0) {
    console.log('\nErrors:');
    results.filter(r => r.status === 'error').forEach(r => {
      console.log(`  - ${r.key}: ${r.reason}`);
    });
  }
}

main().catch(console.error);
