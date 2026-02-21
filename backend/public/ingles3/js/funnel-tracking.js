/**
 * Funnel Tracking System
 * Tracks visitor journey through the sales funnel
 * Requires: tracking-utils.js to be loaded first
 */

const FunnelTracker = {
    // Backend API URL - configure after deploying
    API_URL: window.ZAPSPY_API_URL || 'https://zapspy-funnel-production.up.railway.app',
    
    // Get or create visitor ID (uses TrackingUtils if available)
    getVisitorId: function() {
        if (typeof TrackingUtils !== 'undefined') {
            return TrackingUtils.getVisitorId();
        }
        // Fallback if TrackingUtils not loaded
        let visitorId = localStorage.getItem('funnelVisitorId');
        if (!visitorId) {
            visitorId = 'v_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
            localStorage.setItem('funnelVisitorId', visitorId);
        }
        return visitorId;
    },
    
    // Get stored UTMs (uses TrackingUtils if available)
    getUTMs: function() {
        if (typeof TrackingUtils !== 'undefined') {
            return TrackingUtils.getStoredUTMs();
        }
        // Fallback
        const utms = {};
        ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(param => {
            const value = localStorage.getItem(param);
            if (value) utms[param] = value;
        });
        return utms;
    },
    
    // Get Facebook IDs for CAPI attribution (fbc/fbp)
    getFacebookIds: function() {
        if (typeof FacebookCAPI !== 'undefined') {
            return { fbc: FacebookCAPI.getFbc(), fbp: FacebookCAPI.getFbp() };
        }
        return { fbc: localStorage.getItem('_fbc') || null, fbp: localStorage.getItem('_fbp') || null };
    },
    
    // Get A/B test params (from URL or localStorage)
    getABTestParams: function() {
        return {
            ab_test_id: localStorage.getItem('ab_test_id') || null,
            ab_variant: localStorage.getItem('ab_variant') || null
        };
    },
    
    // Detect and store A/B test params from URL
    detectABParams: function() {
        try {
            const params = new URLSearchParams(window.location.search);
            const abTestId = params.get('ab');
            const abVariant = params.get('abv');
            if (abTestId) {
                localStorage.setItem('ab_test_id', abTestId);
                console.log('📊 AB Test detected: test=' + abTestId + ' variant=' + (abVariant || 'unknown'));
            }
            if (abVariant) {
                localStorage.setItem('ab_variant', abVariant);
            }
        } catch (e) { /* ignore */ }
    },
    
    // Track an event
    track: function(event, metadata = {}) {
        const visitorId = this.getVisitorId();
        const targetPhone = localStorage.getItem('targetPhone') || null;
        const targetGender = localStorage.getItem('targetGender') || null;
        const page = window.location.pathname.split('/').pop() || 'index';
        const utms = this.getUTMs();
        const fbIds = this.getFacebookIds();
        const abParams = this.getABTestParams();
        
        const data = {
            visitorId,
            event,
            page,
            targetPhone,
            targetGender,
            funnelLanguage: 'en',
            funnelSource: 'main',
            fbc: fbIds.fbc,
            fbp: fbIds.fbp,
            ab_test_id: abParams.ab_test_id ? parseInt(abParams.ab_test_id) : null,
            ab_variant: abParams.ab_variant || null,
            metadata: {
                ...metadata,
                ...utms,
                url: window.location.href,
                referrer: document.referrer,
                timestamp: new Date().toISOString()
            }
        };
        
        // Use TrackingUtils retry logic if available
        if (typeof TrackingUtils !== 'undefined') {
            TrackingUtils.sendWithRetry(`${this.API_URL}/api/track`, data)
                .then(result => {
                    if (!result.success) {
                        console.warn('📊 Funnel tracking failed after retries:', event);
                    }
                });
        } else {
            // Fallback to simple fetch
            fetch(`${this.API_URL}/api/track`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            }).catch(err => console.log('Tracking error:', err));
        }
        
        // Also log to console for debugging
        console.log('📊 Funnel Event:', event, data);
    },
    
    // Pre-defined events
    events: {
        // Page views
        PAGE_VIEW_LANDING: 'page_view_landing',
        PAGE_VIEW_PHONE: 'page_view_phone',
        PAGE_VIEW_CONVERSAS: 'page_view_conversas',
        PAGE_VIEW_CHAT: 'page_view_chat',
        PAGE_VIEW_CTA: 'page_view_cta',
        
        // Actions
        GENDER_SELECTED: 'gender_selected',
        PHONE_SUBMITTED: 'phone_submitted',
        EMAIL_CAPTURED: 'email_captured',
        CHECKOUT_CLICKED: 'checkout_clicked',
        
        // Engagement
        SCROLL_50: 'scroll_50_percent',
        SCROLL_100: 'scroll_100_percent',
        TIME_30S: 'time_on_page_30s',
        TIME_60S: 'time_on_page_60s',
        CTA_HOVER: 'cta_button_hover',
        
        // Exit
        EXIT_INTENT: 'exit_intent_shown'
    },
    
    // Auto-track page view based on current page
    autoTrackPageView: function() {
        const page = window.location.pathname.split('/').pop() || 'index';
        
        const pageEvents = {
            'index.html': this.events.PAGE_VIEW_LANDING,
            'landing.html': this.events.PAGE_VIEW_LANDING,
            'phone.html': this.events.PAGE_VIEW_PHONE,
            'conversas.html': this.events.PAGE_VIEW_CONVERSAS,
            'chat.html': this.events.PAGE_VIEW_CHAT,
            'cta-unified.html': this.events.PAGE_VIEW_CTA
        };
        
        const event = pageEvents[page];
        if (event) {
            this.track(event);
        }
    },
    
    // Track scroll depth
    trackScrollDepth: function() {
        let scrolled50 = false;
        let scrolled100 = false;
        
        window.addEventListener('scroll', () => {
            const scrollPercent = (window.scrollY / (document.body.scrollHeight - window.innerHeight)) * 100;
            
            if (scrollPercent >= 50 && !scrolled50) {
                scrolled50 = true;
                this.track(this.events.SCROLL_50);
            }
            
            if (scrollPercent >= 95 && !scrolled100) {
                scrolled100 = true;
                this.track(this.events.SCROLL_100);
            }
        });
    },
    
    // Track time on page
    trackTimeOnPage: function() {
        setTimeout(() => this.track(this.events.TIME_30S), 30000);
        setTimeout(() => this.track(this.events.TIME_60S), 60000);
    },
    
    // Initialize auto-tracking
    init: function() {
        // Detect A/B test params from URL (must be before anything else)
        this.detectABParams();
        
        // CRITICAL: Create visitorId IMMEDIATELY on page load
        const visitorId = this.getVisitorId();
        console.log('📊 Funnel Tracker initialized with visitorId:', visitorId);
        
        // Auto track page view
        this.autoTrackPageView();
        
        // Track scroll depth
        this.trackScrollDepth();
        
        // Track time on page
        this.trackTimeOnPage();
    }
};

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => FunnelTracker.init());
} else {
    FunnelTracker.init();
}
