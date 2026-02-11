/**
 * Facebook Conversions API Client v2.0 - Spanish Version
 * Full integration with Browser Pixel + Server CAPI for 10/10 event quality
 *
 * Pixel para funil espanhol:
 * - Pixel: 534495082571779 (PIXEL SPY ESPANHOL)
 */

const FacebookCAPI = {
    // Siempre usar el backend Railway para CAPI (los funnels pueden estar en otro dominio)
    API_URL: 'https://zapspy-funnel-production.up.railway.app',

    PIXEL_IDS: ['534495082571779'],
    ACCESS_TOKEN: 'EAALZCphpZCmcIBQh5zHSNNj666RUi8XybMe3ZBRE31J9czSE04LBY4nZC9PBNG8SFNL4yCJf6zb9V88JkjNz55nTaIZC2wKSW22OhohIBY0IyYPYXTBFQTBVWUUIYDHhgZBf1CDVye724ekcSA6UbwSqJQPK8XYLEkvUfoJtXq7ktPv7qMOjloAx3jXdjUdJM3TgZDZD',

    generateEventId: function(eventName) {
        const timestamp = Date.now();
        const random = Math.random().toString(36).substr(2, 9);
        return `${eventName}_${timestamp}_${random}`;
    },

    getVisitorId: function() {
        let visitorId = localStorage.getItem('funnelVisitorId');
        if (!visitorId) {
            visitorId = 'v_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
            localStorage.setItem('funnelVisitorId', visitorId);
        }
        return visitorId;
    },

    _getCookie: function(name) {
        const match = document.cookie.split(';').map(c => c.trim()).find(c => c.startsWith(name + '='));
        return match ? match.split('=')[1] : null;
    },

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

    trackEvent: function(eventName, customData = {}, options = {}) {
        const eventId = this.generateEventId(eventName);
        const userData = this.getUserData();
        if (typeof fbq !== 'undefined') {
            const pixelData = { ...customData, eventID: eventId };
            fbq('track', eventName, pixelData, { eventID: eventId });
            console.log(`📊 Browser Pixel: ${eventName} (${eventId})`);
        }
        this.sendToServer(eventName, eventId, userData, customData, options);
        return eventId;
    },

    sendToServer: async function(eventName, eventId, userData, customData = {}, options = {}) {
        try {
            const payload = {
                eventName: eventName,
                eventId: eventId,
                email: userData.email,
                phone: userData.phone,
                firstName: userData.firstName,
                externalId: userData.visitorId,
                country: userData.country,
                city: userData.city,
                state: userData.state,
                gender: userData.gender,
                fbc: userData.fbc,
                fbp: userData.fbp,
                eventSourceUrl: window.location.href,
                pixelIds: this.PIXEL_IDS,
                accessToken: this.ACCESS_TOKEN,
                funnelLanguage: 'es',
                ...customData
            };
            const response = await fetch(`${this.API_URL}/api/capi/event`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (response.ok) console.log(`✅ CAPI (ES): ${eventName} (${eventId})`);
            else console.warn(`⚠️ CAPI failed: ${eventName}`, await response.text());
            return response.ok;
        } catch (error) {
            console.error(`❌ CAPI error: ${eventName}`, error);
            return false;
        }
    },

    trackPageView: function(pageName) {
        return this.trackEvent('PageView', { content_name: pageName || document.title });
    },

    trackViewContent: function(contentName, contentCategory, value = 39) {
        return this.trackEvent('ViewContent', {
            content_name: contentName,
            content_category: contentCategory,
            value: value > 0 ? value : 39,
            currency: 'USD'
        });
    },

    trackLead: function(email, userData = {}) {
        const fbc = this.getFbc();
        const fbp = this.getFbp();
        const visitorId = this.getVisitorId();
        return this.trackEvent('Lead', {
            content_name: 'Lead Capture',
            currency: 'USD',
            value: 39,
            email: email,
            phone: userData.phone || null,
            firstName: userData.name || null,
            fbc: fbc,
            fbp: fbp,
            externalId: visitorId
        });
    },

    trackInitiateCheckout: function(value, productName) {
        return this.trackEvent('InitiateCheckout', {
            value: value,
            currency: 'USD',
            content_name: productName,
            content_type: 'product',
            num_items: 1
        });
    },

    trackAddToCart: function(value, productName) {
        return this.trackEvent('AddToCart', {
            value: value,
            currency: 'USD',
            content_name: productName,
            content_type: 'product'
        });
    },

    trackPurchase: function(value, productName, transactionId) {
        return this.trackEvent('Purchase', {
            value: value,
            currency: 'USD',
            content_name: productName,
            content_type: 'product',
            content_ids: [transactionId]
        });
    },

    init: function(pageName) {
        this.getFbc();
        this.getFbp();
        this.getVisitorId();
        if (pageName) this.trackPageView(pageName);
        console.log('📊 Facebook CAPI v2.0 (ES) initialized');
        console.log('   Visitor ID:', this.getVisitorId());
        console.log('   FBP:', this.getFbp());
        console.log('   FBC:', this.getFbc() || 'not set');
    }
};
