/**
 * Facebook Conversions API Client v2.0
 * Full integration with Browser Pixel + Server CAPI for 10/10 event quality
 *
 * Features:
 * - Dual tracking (Browser + Server)
 * - Event ID deduplication
 * - External ID for cross-device tracking
 * - Advanced Matching data
 * - Automatic fbc/fbp capture
 */

const FacebookCAPI = {
    // Sempre usar o backend Railway para CAPI (os funis podem estar em zappdetect.com, afiliado.whatstalker.com, etc.)
    API_URL: window.ZAPSPY_API_URL || 'https://zapspy-funnel-production.up.railway.app',

    // Generate unique event ID for deduplication
    generateEventId: function(eventName) {
        const timestamp = Date.now();
        const random = Math.random().toString(36).substr(2, 9);
        return `${eventName}_${timestamp}_${random}`;
    },

    // Get or create visitor ID (external_id for Facebook)
    getVisitorId: function() {
        let visitorId = localStorage.getItem('funnelVisitorId');
        if (!visitorId) {
            visitorId = 'v_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
            localStorage.setItem('funnelVisitorId', visitorId);
        }
        return visitorId;
    },

    // Helper: read a cookie by name
    _getCookie: function(name) {
        const match = document.cookie.split(';').map(c => c.trim()).find(c => c.startsWith(name + '='));
        return match ? match.split('=')[1] : null;
    },

    // Get Facebook click ID from URL, cookie or localStorage
    getFbc: function() {
        const urlParams = new URLSearchParams(window.location.search);
        const fbclid = urlParams.get('fbclid');
        if (fbclid) {
            const fbc = `fb.1.${Date.now()}.${fbclid}`;
            localStorage.setItem('_fbc', fbc);
            return fbc;
        }
        const cookieFbc = this._getCookie('_fbc');
        if (cookieFbc) {
            localStorage.setItem('_fbc', cookieFbc);
            return cookieFbc;
        }
        return localStorage.getItem('_fbc') || null;
    },

    // Get Facebook browser ID from cookie (_fbp set by Meta Pixel) or generate one
    getFbp: function() {
        const cookieFbp = this._getCookie('_fbp');
        if (cookieFbp) {
            localStorage.setItem('_fbp', cookieFbp);
            return cookieFbp;
        }
        let fbp = localStorage.getItem('_fbp');
        if (!fbp) {
            fbp = `fb.1.${Date.now()}.${Math.floor(Math.random() * 10000000000)}`;
            localStorage.setItem('_fbp', fbp);
        }
        return fbp;
    },

    // Get user data from localStorage (including geo data for better match quality)
    getUserData: function() {
        // Try to get city/state from individual keys first, then from userGeo JSON as fallback
        var city = localStorage.getItem('userCity') || null;
        var state = localStorage.getItem('userState') || null;
        var country = localStorage.getItem('userCountryCode') || null;
        if (!city || !country) {
            try {
                var geo = JSON.parse(localStorage.getItem('userGeo') || '{}');
                if (!city && geo.city) city = geo.city;
                if (!country && geo.country) country = geo.country;
            } catch(e) {}
        }
        return {
            email: localStorage.getItem('userEmail') || null,
            phone: localStorage.getItem('userWhatsApp') || null,
            firstName: localStorage.getItem('userName') || null,
            country: country,
            city: city,
            state: state,
            gender: localStorage.getItem('targetGender') || null,
            visitorId: this.getVisitorId(),
            fbc: this.getFbc(),
            fbp: this.getFbp()
        };
    },

    // Send event to both Browser Pixel and Server CAPI
    trackEvent: function(eventName, customData = {}, options = {}) {
        const eventId = this.generateEventId(eventName);
        const userData = this.getUserData();

        // 1. Send to Browser Pixel with event_id
        if (typeof fbq !== 'undefined') {
            const pixelData = {
                ...customData,
                eventID: eventId  // For deduplication
            };
            fbq('track', eventName, pixelData, { eventID: eventId });
            console.log(`📊 Browser Pixel: ${eventName} (${eventId})`);
        }

        // 2. Send to Server CAPI
        this.sendToServer(eventName, eventId, userData, customData, options);

        return eventId;
    },

    // Send event only to Server CAPI (no browser pixel)
    sendToServer: async function(eventName, eventId, userData, customData = {}, options = {}) {
        try {
            const payload = {
                eventName: eventName,
                eventId: eventId,
                email: userData.email,
                phone: userData.phone,
                firstName: userData.firstName,
                country: userData.country,
                city: userData.city,
                state: userData.state,
                gender: userData.gender,
                externalId: userData.visitorId,
                fbc: userData.fbc,
                fbp: userData.fbp,
                eventSourceUrl: window.location.href,
                funnelLanguage: 'en',
                ...customData
            };

            const response = await fetch(`${this.API_URL}/api/capi/event`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            if (response.ok) {
                console.log(`✅ CAPI: ${eventName} (${eventId})`);
            } else {
                console.warn(`⚠️ CAPI failed: ${eventName}`, await response.text());
            }

            return response.ok;
        } catch (error) {
            console.error(`❌ CAPI error: ${eventName}`, error);
            return false;
        }
    },

    // ==================== STANDARD EVENTS ====================

    // PageView - call on every page load
    trackPageView: function(pageName) {
        return this.trackEvent('PageView', {
            content_name: pageName || document.title
        });
    },

    // ViewContent - when user views important content
    trackViewContent: function(contentName, contentCategory, value = 49) {
        return this.trackEvent('ViewContent', {
            content_name: contentName,
            content_category: contentCategory,
            value: value > 0 ? value : 49,
            currency: 'USD'
        });
    },

    // Lead - when user submits contact info
    trackLead: function(email, userData = {}) {
        const fbc = this.getFbc();
        const fbp = this.getFbp();
        const visitorId = this.getVisitorId();

        return this.trackEvent('Lead', {
            content_name: 'Lead Capture',
            currency: 'USD',
            value: 49,
            email: email,
            phone: userData.phone || null,
            firstName: userData.name || null,
            fbc: fbc,
            fbp: fbp,
            externalId: visitorId
        });
    },

    // InitiateCheckout - when user clicks to buy
    trackInitiateCheckout: function(value, productName) {
        return this.trackEvent('InitiateCheckout', {
            value: value,
            currency: 'USD',
            content_name: productName,
            content_type: 'product',
            num_items: 1
        });
    },

    // AddToCart - for granular tracking
    trackAddToCart: function(value, productName) {
        return this.trackEvent('AddToCart', {
            value: value,
            currency: 'USD',
            content_name: productName,
            content_type: 'product'
        });
    },

    // Purchase - if needed from frontend
    trackPurchase: function(value, productName, transactionId) {
        return this.trackEvent('Purchase', {
            value: value,
            currency: 'USD',
            content_name: productName,
            content_type: 'product',
            content_ids: [transactionId]
        });
    },

    // ==================== INITIALIZATION ====================

    init: function(pageName) {
        this.getFbc();
        this.getFbp();
        this.getVisitorId();

        if (pageName) {
            this.trackPageView(pageName);
        }

        console.log('📊 Facebook CAPI v2.0 initialized');
        console.log('   Visitor ID:', this.getVisitorId());
        console.log('   FBP:', this.getFbp());
        console.log('   FBC:', this.getFbc() || 'not set');
    }
};
