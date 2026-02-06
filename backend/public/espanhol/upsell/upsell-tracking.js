/**
 * Upsell Funnel Tracking
 * Tracks visitor journey through upsell pages
 * Uses the same visitorId from main funnel for complete journey tracking
 */

const UpsellTracker = {
    API_URL: 'https://zapspy-funnel-production.up.railway.app',
    
    // Get existing visitor ID from main funnel
    getVisitorId: function() {
        let visitorId = localStorage.getItem('funnelVisitorId');
        if (!visitorId) {
            // Create new one if somehow doesn't exist
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
        const page = window.location.pathname;
        
        const data = {
            visitorId,
            event,
            page,
            targetPhone,
            targetGender,
            metadata: {
                ...metadata,
                url: window.location.href,
                referrer: document.referrer,
                timestamp: new Date().toISOString(),
                upsellFlow: true,
                funnel: 'spanish'
            }
        };
        
        // Send to backend
        fetch(`${this.API_URL}/api/track`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        }).catch(err => console.log('Tracking error:', err));
        
        console.log('📊 Upsell Event:', event, data);
    },
    
    // Pre-defined events
    events: {
        // Page views
        VIEW_UPSELL_1: 'upsell_1_view',
        VIEW_UPSELL_2: 'upsell_2_view',
        VIEW_UPSELL_3: 'upsell_3_view',
        VIEW_THANKYOU: 'thankyou_view',
        
        // Accepts
        ACCEPT_UPSELL_1: 'upsell_1_accepted',
        ACCEPT_UPSELL_2: 'upsell_2_accepted',
        ACCEPT_UPSELL_3: 'upsell_3_accepted',
        
        // Declines
        DECLINE_UPSELL_1: 'upsell_1_declined',
        DECLINE_UPSELL_2: 'upsell_2_declined',
        DECLINE_UPSELL_3: 'upsell_3_declined'
    },
    
    // Get current upsell number from URL
    getCurrentUpsell: function() {
        const path = window.location.pathname;
        if (path.includes('/up1/')) return 1;
        if (path.includes('/up2/')) return 2;
        if (path.includes('/up3/')) return 3;
        if (path.includes('/gracias/') || path.includes('/thankyou/')) return 'thankyou';
        return null;
    },
    
    // Auto-track page view
    trackPageView: function() {
        const upsell = this.getCurrentUpsell();
        
        if (upsell === 1) {
            this.track(this.events.VIEW_UPSELL_1);
        } else if (upsell === 2) {
            this.track(this.events.VIEW_UPSELL_2);
        } else if (upsell === 3) {
            this.track(this.events.VIEW_UPSELL_3);
        } else if (upsell === 'thankyou') {
            this.track(this.events.VIEW_THANKYOU);
        }
    },
    
    // Track accept (buy button click)
    trackAccept: function() {
        const upsell = this.getCurrentUpsell();
        
        if (upsell === 1) {
            this.track(this.events.ACCEPT_UPSELL_1, { action: 'buy_clicked' });
        } else if (upsell === 2) {
            this.track(this.events.ACCEPT_UPSELL_2, { action: 'buy_clicked' });
        } else if (upsell === 3) {
            this.track(this.events.ACCEPT_UPSELL_3, { action: 'buy_clicked' });
        }
    },
    
    // Track decline (no thanks click)
    trackDecline: function() {
        const upsell = this.getCurrentUpsell();
        
        if (upsell === 1) {
            this.track(this.events.DECLINE_UPSELL_1, { action: 'declined' });
        } else if (upsell === 2) {
            this.track(this.events.DECLINE_UPSELL_2, { action: 'declined' });
        } else if (upsell === 3) {
            this.track(this.events.DECLINE_UPSELL_3, { action: 'declined' });
        }
    },
    
    // Setup click listeners
    setupListeners: function() {
        // Track buy button clicks (Monetizze 1-click)
        document.querySelectorAll('a[href="#monetizzeCompra"], a[data-upsell]').forEach(btn => {
            btn.addEventListener('click', () => {
                this.trackAccept();
            });
        });
        
        // Track decline link clicks
        document.querySelectorAll('.decline-link, a[href*="up2"], a[href*="up3"], a[href*="gracias"], a[href*="thankyou"]').forEach(link => {
            // Only track if it's a decline action (not a buy button)
            if (!link.hasAttribute('data-upsell') && !link.href.includes('#monetizzeCompra')) {
                link.addEventListener('click', () => {
                    this.trackDecline();
                });
            }
        });
    },
    
    // Initialize
    init: function() {
        // Track page view
        this.trackPageView();
        
        // Setup click listeners when DOM is ready
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.setupListeners());
        } else {
            this.setupListeners();
        }
        
        console.log('📊 Upsell Tracker initialized (Spanish) - Visitor:', this.getVisitorId());
    }
};

// Auto-initialize
UpsellTracker.init();
