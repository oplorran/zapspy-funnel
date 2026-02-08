/**
 * Funnel Tracking System - Spanish Version
 * Tracks visitor journey through the sales funnel
 */

const FunnelTracker = {
    // Backend API URL
    API_URL: 'https://zapspy-funnel-production.up.railway.app',
    
    // Get or create visitor ID
    getVisitorId: function() {
        let visitorId = localStorage.getItem('funnelVisitorId');
        if (!visitorId) {
            visitorId = 'v_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
            localStorage.setItem('funnelVisitorId', visitorId);
        }
        return visitorId;
    },
    
    // Track an event
    track: function(event, metadata = {}) {
        const visitorId = this.getVisitorId();
        const targetPhone = localStorage.getItem('targetPhone') || null;
        const targetGender = localStorage.getItem('targetGender') || null;
        const page = window.location.pathname.split('/').pop() || 'index';
        
        const data = {
            visitorId,
            event,
            page,
            targetPhone,
            targetGender,
            funnelLanguage: 'es',
            metadata: {
                ...metadata,
                url: window.location.href,
                referrer: document.referrer,
                timestamp: new Date().toISOString()
            }
        };
        
        // Send to backend (fire and forget)
        fetch(`${this.API_URL}/api/track`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        }).catch(err => console.log('Tracking error:', err));
        
        console.log('📊 Funnel Event:', event, data);
    },
    
    // Pre-defined events
    events: {
        PAGE_VIEW_LANDING: 'page_view_landing',
        PAGE_VIEW_PHONE: 'page_view_phone',
        PAGE_VIEW_CONVERSAS: 'page_view_conversas',
        PAGE_VIEW_CHAT: 'page_view_chat',
        PAGE_VIEW_CTA: 'page_view_cta',
        GENDER_SELECTED: 'gender_selected',
        PHONE_SUBMITTED: 'phone_submitted',
        EMAIL_CAPTURED: 'email_captured',
        CHECKOUT_CLICKED: 'checkout_clicked',
        SCROLL_50: 'scroll_50_percent',
        SCROLL_100: 'scroll_100_percent',
        TIME_30S: 'time_on_page_30s',
        TIME_60S: 'time_on_page_60s',
        CTA_HOVER: 'cta_button_hover',
        EXIT_INTENT: 'exit_intent_shown'
    },
    
    // Auto-track page view
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
    
    // Initialize
    init: function() {
        // CRITICAL: Create visitorId IMMEDIATELY on page load
        // This ensures visitorId exists before any email capture
        const visitorId = this.getVisitorId();
        console.log('📊 Funnel Tracker (ES) initialized with visitorId:', visitorId);
        
        this.autoTrackPageView();
        this.trackScrollDepth();
        this.trackTimeOnPage();
    }
};

// Auto-initialize
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => FunnelTracker.init());
} else {
    FunnelTracker.init();
}
