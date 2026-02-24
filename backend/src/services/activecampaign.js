/**
 * ActiveCampaign Integration Service
 * 
 * Handles all communication with ActiveCampaign API v3
 * for email recovery automations (abandoned funnel, abandoned checkout,
 * cancelled sales).
 * 
 * Tags and lists are created automatically on first run via setup script.
 * 
 * Environment variables required:
 *   AC_API_URL - e.g. https://yourname.api-us1.com
 *   AC_API_KEY - API key from Settings > Developer
 */

const { AC_API_URL, AC_API_KEY } = require('../config');

// Tag mapping: event -> { en: tagName, es: tagName }
const TAG_MAP = {
    'lead_captured': {
        en: 'zapspy-lead-en',
        es: 'zapspy-lead-es'
    },
    'checkout_abandoned': {
        en: 'zapspy-checkout-abandon-en',
        es: 'zapspy-checkout-abandon-es'
    },
    'sale_cancelled': {
        en: 'zapspy-sale-cancelled-en',
        es: 'zapspy-sale-cancelled-es'
    },
    'sale_approved': {
        en: 'zapspy-buyer-en',
        es: 'zapspy-buyer-es'
    },
    'sale_refunded': {
        en: 'zapspy-refunded-en',
        es: 'zapspy-refunded-es'
    },
    'sale_chargeback': {
        en: 'zapspy-chargeback-en',
        es: 'zapspy-chargeback-es'
    }
};

// List mapping: language -> listName
const LIST_MAP = {
    'lead_captured': {
        en: 'ZapSpy - Leads EN',
        es: 'ZapSpy - Leads ES'
    },
    'checkout_abandoned': {
        en: 'ZapSpy - Checkout Abandon EN',
        es: 'ZapSpy - Checkout Abandon ES'
    },
    'sale_cancelled': {
        en: 'ZapSpy - Sale Cancelled EN',
        es: 'ZapSpy - Sale Cancelled ES'
    }
};

// Cache for tag IDs and list IDs (populated on first use)
let tagIdCache = {};
let listIdCache = {};
let cacheLoaded = false;

/**
 * Check if ActiveCampaign integration is configured
 */
function isConfigured() {
    return !!(AC_API_URL && AC_API_KEY);
}

/**
 * Make an API request to ActiveCampaign v3
 */
async function apiRequest(method, endpoint, body = null) {
    if (!isConfigured()) {
        console.log('⚠️ ActiveCampaign not configured, skipping API call');
        return null;
    }

    const url = `${AC_API_URL}/api/3/${endpoint}`;
    const options = {
        method,
        headers: {
            'Api-Token': AC_API_KEY,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    };

    if (body && (method === 'POST' || method === 'PUT')) {
        options.body = JSON.stringify(body);
    }

    try {
        const response = await fetch(url, options);
        const data = await response.json();

        if (!response.ok) {
            console.error(`❌ AC API Error [${method} ${endpoint}]:`, data);
            return null;
        }

        return data;
    } catch (error) {
        console.error(`❌ AC API Request failed [${method} ${endpoint}]:`, error.message);
        return null;
    }
}

/**
 * Make an API v1 request to ActiveCampaign (for message/campaign operations)
 */
async function apiV1Request(action, params = {}) {
    if (!isConfigured()) return null;

    const url = `${AC_API_URL}/admin/api.php`;
    const searchParams = new URLSearchParams({
        api_key: AC_API_KEY,
        api_action: action,
        api_output: 'json',
        ...params
    });

    try {
        const response = await fetch(`${url}?${searchParams.toString()}`, {
            method: 'GET'
        });
        return await response.json();
    } catch (error) {
        console.error(`❌ AC API v1 Request failed [${action}]:`, error.message);
        return null;
    }
}

/**
 * Load tag and list IDs from ActiveCampaign (cached)
 */
async function loadCache() {
    if (cacheLoaded || !isConfigured()) return;

    try {
        // Load all tags
        let offset = 0;
        const allTags = [];
        while (true) {
            const data = await apiRequest('GET', `tags?limit=100&offset=${offset}`);
            if (!data || !data.tags || data.tags.length === 0) break;
            allTags.push(...data.tags);
            if (data.tags.length < 100) break;
            offset += 100;
        }

        for (const tag of allTags) {
            tagIdCache[tag.tag] = tag.id;
        }

        // Load all lists
        const listsData = await apiRequest('GET', 'lists?limit=100');
        if (listsData && listsData.lists) {
            for (const list of listsData.lists) {
                listIdCache[list.name] = list.id;
            }
        }

        cacheLoaded = true;
        console.log(`📧 ActiveCampaign cache loaded: ${Object.keys(tagIdCache).length} tags, ${Object.keys(listIdCache).length} lists`);
    } catch (error) {
        console.error('❌ Failed to load AC cache:', error.message);
    }
}

/**
 * Get or create a tag by name
 */
async function getOrCreateTag(tagName) {
    await loadCache();

    if (tagIdCache[tagName]) {
        return tagIdCache[tagName];
    }

    // Create tag
    const data = await apiRequest('POST', 'tags', {
        tag: {
            tag: tagName,
            tagType: 'contact',
            description: `ZapSpy auto-created tag: ${tagName}`
        }
    });

    if (data && data.tag) {
        tagIdCache[tagName] = data.tag.id;
        return data.tag.id;
    }

    return null;
}

/**
 * Get or create a list by name
 */
async function getOrCreateList(listName) {
    await loadCache();

    if (listIdCache[listName]) {
        return listIdCache[listName];
    }

    // Create list
    const data = await apiRequest('POST', 'lists', {
        list: {
            name: listName,
            stringid: listName.toLowerCase().replace(/[^a-z0-9]/g, '-'),
            sender_url: 'https://zapspy.ai',
            sender_reminder: 'You signed up for ZapSpy.ai monitoring service.'
        }
    });

    if (data && data.list) {
        listIdCache[listName] = data.list.id;
        return data.list.id;
    }

    return null;
}

/**
 * Create or update a contact in ActiveCampaign
 */
async function syncContact(email, firstName = '', phone = '', customFields = {}) {
    if (!isConfigured() || !email) return null;

    const contactData = {
        contact: {
            email: email,
            firstName: firstName || '',
            phone: phone || ''
        }
    };

    // Try to sync (create or update)
    const data = await apiRequest('POST', 'contact/sync', contactData);

    if (data && data.contact) {
        return data.contact.id;
    }

    return null;
}

/**
 * Add a tag to a contact
 */
async function addTagToContact(contactId, tagId) {
    if (!contactId || !tagId) return false;

    const data = await apiRequest('POST', 'contactTags', {
        contactTag: {
            contact: String(contactId),
            tag: String(tagId)
        }
    });

    return !!data;
}

/**
 * Remove a tag from a contact
 */
async function removeTagFromContact(contactId, tagName) {
    if (!contactId || !tagName) return false;

    await loadCache();
    const tagId = tagIdCache[tagName];
    if (!tagId) return false;

    // Find the contactTag ID first
    const data = await apiRequest('GET', `contacts/${contactId}/contactTags`);
    if (!data || !data.contactTags) return false;

    const contactTag = data.contactTags.find(ct => ct.tag === String(tagId));
    if (!contactTag) return false;

    await apiRequest('DELETE', `contactTags/${contactTag.id}`);
    return true;
}

/**
 * Subscribe a contact to a list
 */
async function subscribeToList(contactId, listId) {
    if (!contactId || !listId) return false;

    const data = await apiRequest('POST', 'contactLists', {
        contactList: {
            list: String(listId),
            contact: String(contactId),
            status: 1 // 1 = subscribed
        }
    });

    return !!data;
}

/**
 * MAIN FUNCTION: Process a funnel event and sync with ActiveCampaign
 * 
 * @param {string} eventType - One of: lead_captured, checkout_abandoned, sale_cancelled, sale_approved, sale_refunded, sale_chargeback
 * @param {string} language - 'en' or 'es'
 * @param {object} contactInfo - { email, name, phone, targetPhone, whatsapp }
 */
async function processEvent(eventType, language, contactInfo) {
    if (!isConfigured()) {
        return { success: false, reason: 'ActiveCampaign not configured' };
    }

    const lang = language === 'es' ? 'es' : 'en';
    const { email, name, phone, targetPhone, whatsapp } = contactInfo;

    if (!email) {
        return { success: false, reason: 'No email provided' };
    }

    try {
        console.log(`📧 AC Event: ${eventType} [${lang.toUpperCase()}] for ${email}`);

        // 1. Create/update contact
        const contactId = await syncContact(email, name, phone || whatsapp);
        if (!contactId) {
            return { success: false, reason: 'Failed to sync contact' };
        }

        // 2. Get the tag for this event + language
        const tagMapping = TAG_MAP[eventType];
        if (!tagMapping || !tagMapping[lang]) {
            return { success: false, reason: `Unknown event type: ${eventType}` };
        }

        const tagName = tagMapping[lang];
        const tagId = await getOrCreateTag(tagName);
        if (!tagId) {
            return { success: false, reason: `Failed to get/create tag: ${tagName}` };
        }

        // 3. Add tag to contact (this triggers the automation in AC)
        await addTagToContact(contactId, tagId);

        // 4. Subscribe to corresponding list (if exists)
        const listMapping = LIST_MAP[eventType];
        if (listMapping && listMapping[lang]) {
            const listId = await getOrCreateList(listMapping[lang]);
            if (listId) {
                await subscribeToList(contactId, listId);
            }
        }

        // 5. If buyer/approved, remove recovery tags (stop recovery emails)
        if (eventType === 'sale_approved') {
            const recoveryTags = [
                TAG_MAP['lead_captured']?.[lang],
                TAG_MAP['checkout_abandoned']?.[lang],
                TAG_MAP['sale_cancelled']?.[lang]
            ].filter(Boolean);

            for (const recoveryTag of recoveryTags) {
                await removeTagFromContact(contactId, recoveryTag);
            }
            console.log(`📧 AC: Removed recovery tags for buyer ${email}`);
        }

        console.log(`✅ AC Event processed: ${eventType} [${lang.toUpperCase()}] → tag "${tagName}" added to ${email}`);
        return { success: true, contactId, tagName };

    } catch (error) {
        console.error(`❌ AC processEvent error:`, error.message);
        return { success: false, reason: error.message };
    }
}

module.exports = {
    isConfigured,
    processEvent,
    syncContact,
    addTagToContact,
    removeTagFromContact,
    subscribeToList,
    getOrCreateTag,
    getOrCreateList,
    loadCache,
    apiRequest,
    apiV1Request,
    TAG_MAP,
    LIST_MAP
};
