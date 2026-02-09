/**
 * Upsell Funnel Tracking
 * Tracks visitor journey through upsell pages
 * Uses the same visitorId from main funnel for complete journey tracking
 * 
 * ENHANCED v2.1 - Added URL parameter support for cross-domain tracking
 * When Monetizze loads upsell pages, localStorage is not available from original domain
 * So we pass visitorId via URL parameter to maintain tracking continuity
 */

const UpsellTracker = {
    API_URL: 'https://zapspy-funnel-production.up.railway.app',
    pageLoadTime: Date.now(),
    scrollDepth: 0,
    
    // Get visitor ID from URL parameter first, then localStorage, then create new
    getVisitorId: function() {
        // Priority 1: Check URL parameter (for cross-domain tracking via Monetizze)
        const urlParams = new URLSearchParams(window.location.search);
        let visitorId = urlParams.get('vid') || urlParams.get('visitorId');
        
        // Priority 2: Check localStorage
        if (!visitorId) {
            visitorId = localStorage.getItem('funnelVisitorId');
        }
        
        // Priority 3: Create new one if nothing found
        if (!visitorId) {
            visitorId = 'v_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
            console.log('⚠️ UpsellTracker: Created new visitorId (no URL param or localStorage found)');
        }
        
        // Always save to localStorage for future use
        try {
            localStorage.setItem('funnelVisitorId', visitorId);
        } catch (e) {
            // localStorage might not be available in some contexts
        }
        
        return visitorId;
    },
    
    // Track an event
    track: function(event, metadata = {}) {
        const visitorId = this.getVisitorId();
        const targetPhone = localStorage.getItem('targetPhone') || null;
        const targetGender = localStorage.getItem('targetGender') || null;
        const page = window.location.pathname;
        const timeOnPage = Math.round((Date.now() - this.pageLoadTime) / 1000);
        
        const data = {
            visitorId,
            event,
            page,
            targetPhone,
            targetGender,
            funnelLanguage: 'en',
            funnelSource: 'affiliate',
            metadata: {
                ...metadata,
                url: window.location.href,
                referrer: document.referrer,
                timestamp: new Date().toISOString(),
                timeOnPage: timeOnPage,
                scrollDepth: this.scrollDepth,
                upsellFlow: true,
                userAgent: navigator.userAgent,
                screenWidth: window.innerWidth,
                screenHeight: window.innerHeight
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
        
        // Page ready (after any overlays/loading)
        READY_UPSELL_1: 'upsell_1_ready',
        READY_UPSELL_2: 'upsell_2_ready',
        READY_UPSELL_3: 'upsell_3_ready',
        
        // Accepts
        ACCEPT_UPSELL_1: 'upsell_1_accepted',
        ACCEPT_UPSELL_2: 'upsell_2_accepted',
        ACCEPT_UPSELL_3: 'upsell_3_accepted',
        
        // Declines
        DECLINE_UPSELL_1: 'upsell_1_declined',
        DECLINE_UPSELL_2: 'upsell_2_declined',
        DECLINE_UPSELL_3: 'upsell_3_declined',
        
        // CTA Visibility (user scrolled to CTA)
        CTA_VISIBLE_UPSELL_1: 'upsell_1_cta_visible',
        CTA_VISIBLE_UPSELL_2: 'upsell_2_cta_visible',
        CTA_VISIBLE_UPSELL_3: 'upsell_3_cta_visible',
        
        // Page exit (before leaving)
        EXIT_UPSELL_1: 'upsell_1_exit',
        EXIT_UPSELL_2: 'upsell_2_exit',
        EXIT_UPSELL_3: 'upsell_3_exit',
        
        // Engagement milestones
        ENGAGED_10S: 'engaged_10s',
        ENGAGED_30S: 'engaged_30s',
        ENGAGED_60S: 'engaged_60s',
        SCROLL_50: 'scroll_50_percent',
        SCROLL_90: 'scroll_90_percent'
    },
    
    // Get current upsell number from URL
    getCurrentUpsell: function() {
        const path = window.location.pathname;
        if (path.includes('/up1/')) return 1;
        if (path.includes('/up2/')) return 2;
        if (path.includes('/up3/')) return 3;
        if (path.includes('/thankyou/')) return 'thankyou';
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
        const self = this;
        
        // Track buy button clicks (Monetizze 1-click)
        document.querySelectorAll('a[href="#monetizzeCompra"], a[data-upsell]').forEach(btn => {
            btn.addEventListener('click', () => {
                this.trackAccept();
            });
        });
        
        // Track decline link clicks
        document.querySelectorAll('.decline-link, a[href*="up2"], a[href*="up3"], a[href*="thankyou"]').forEach(link => {
            // Only track if it's a decline action (not a buy button)
            if (!link.hasAttribute('data-upsell') && !link.href.includes('#monetizzeCompra')) {
                link.addEventListener('click', () => {
                    this.trackDecline();
                });
            }
        });
    },
    
    // Track page ready (after overlay is hidden or immediately if no overlay)
    trackPageReady: function() {
        const upsell = this.getCurrentUpsell();
        
        if (upsell === 1) {
            this.track(this.events.READY_UPSELL_1);
        } else if (upsell === 2) {
            this.track(this.events.READY_UPSELL_2);
        } else if (upsell === 3) {
            this.track(this.events.READY_UPSELL_3);
        }
    },
    
    // Setup engagement tracking
    setupEngagementTracking: function() {
        const self = this;
        const upsell = this.getCurrentUpsell();
        if (!upsell || upsell === 'thankyou') return;
        
        let tracked10s = false;
        let tracked30s = false;
        let tracked60s = false;
        let tracked50Scroll = false;
        let tracked90Scroll = false;
        let trackedCTA = false;
        
        // Time on page tracking
        setTimeout(() => {
            if (!tracked10s) {
                tracked10s = true;
                self.track(self.events.ENGAGED_10S, { upsell: upsell });
            }
        }, 10000);
        
        setTimeout(() => {
            if (!tracked30s) {
                tracked30s = true;
                self.track(self.events.ENGAGED_30S, { upsell: upsell });
            }
        }, 30000);
        
        setTimeout(() => {
            if (!tracked60s) {
                tracked60s = true;
                self.track(self.events.ENGAGED_60S, { upsell: upsell });
            }
        }, 60000);
        
        // Scroll depth tracking
        window.addEventListener('scroll', function() {
            const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
            const docHeight = document.documentElement.scrollHeight - window.innerHeight;
            const scrollPercent = Math.round((scrollTop / docHeight) * 100);
            
            self.scrollDepth = Math.max(self.scrollDepth, scrollPercent);
            
            if (scrollPercent >= 50 && !tracked50Scroll) {
                tracked50Scroll = true;
                self.track(self.events.SCROLL_50, { upsell: upsell });
            }
            
            if (scrollPercent >= 90 && !tracked90Scroll) {
                tracked90Scroll = true;
                self.track(self.events.SCROLL_90, { upsell: upsell });
            }
            
            // CTA visibility tracking
            if (!trackedCTA) {
                const ctaButton = document.querySelector('.btn-primary[data-upsell], a[href="#monetizzeCompra"]');
                if (ctaButton) {
                    const rect = ctaButton.getBoundingClientRect();
                    if (rect.top < window.innerHeight && rect.bottom > 0) {
                        trackedCTA = true;
                        if (upsell === 1) self.track(self.events.CTA_VISIBLE_UPSELL_1);
                        else if (upsell === 2) self.track(self.events.CTA_VISIBLE_UPSELL_2);
                        else if (upsell === 3) self.track(self.events.CTA_VISIBLE_UPSELL_3);
                    }
                }
            }
        }, { passive: true });
        
        // Track exit (before unload)
        window.addEventListener('beforeunload', function() {
            const timeOnPage = Math.round((Date.now() - self.pageLoadTime) / 1000);
            const exitEvent = upsell === 1 ? self.events.EXIT_UPSELL_1 : 
                              upsell === 2 ? self.events.EXIT_UPSELL_2 : 
                              self.events.EXIT_UPSELL_3;
            
            // Use sendBeacon for reliable exit tracking
            const data = {
                visitorId: self.getVisitorId(),
                event: exitEvent,
                page: window.location.pathname,
                metadata: {
                    timeOnPage: timeOnPage,
                    scrollDepth: self.scrollDepth,
                    exitType: 'beforeunload'
                }
            };
            
            navigator.sendBeacon(`${self.API_URL}/api/track`, JSON.stringify(data));
        });
    },
    
    // Initialize
    init: function() {
        const self = this;
        
        // Track page view immediately
        this.trackPageView();
        
        // Setup click listeners when DOM is ready
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => {
                self.setupListeners();
                self.setupEngagementTracking();
                
                // Track page ready immediately for UP2 and UP3 (no overlay)
                // For UP1, it will be tracked when overlay completes
                const upsell = self.getCurrentUpsell();
                if (upsell === 2 || upsell === 3) {
                    self.trackPageReady();
                }
            });
        } else {
            this.setupListeners();
            this.setupEngagementTracking();
            
            // Track page ready immediately for UP2 and UP3 (no overlay)
            const upsell = this.getCurrentUpsell();
            if (upsell === 2 || upsell === 3) {
                this.trackPageReady();
            }
        }
        
        console.log('📊 Upsell Tracker v2.0 initialized - Visitor:', this.getVisitorId());
    }
};

// Auto-initialize
UpsellTracker.init();

// ============================================
// MONETIZZE 1-CLICK DEBUG
// ============================================
// This helps diagnose issues with the 1-click upsell process
(function() {
    console.log('🔍 Monetizze 1-Click Debug Starting...');
    
    // Check if Monetizze script loaded
    window.addEventListener('load', function() {
        const hasMonetizzeScript = document.querySelector('script[src*="1buyclick.php"]');
        console.log('📦 Monetizze 1buyclick.php script present:', !!hasMonetizzeScript);
        
        // Check for Monetizze global variables/functions
        const monetizzeGlobals = [];
        for (let key in window) {
            if (key.toLowerCase().includes('monetizze') || key.toLowerCase().includes('mtz')) {
                monetizzeGlobals.push(key);
            }
        }
        if (monetizzeGlobals.length > 0) {
            console.log('📊 Monetizze globals found:', monetizzeGlobals);
        }
        
        // Check cookies related to Monetizze
        const cookies = document.cookie.split(';');
        const monetizzeCookies = cookies.filter(c => 
            c.toLowerCase().includes('monetizze') || 
            c.toLowerCase().includes('mtz') ||
            c.toLowerCase().includes('session')
        );
        console.log('🍪 Session-related cookies:', monetizzeCookies.length > 0 ? monetizzeCookies : 'None found');
        
        // Monitor CTA button clicks
        document.querySelectorAll('a[data-upsell], a[href="#monetizzeCompra"]').forEach(function(btn, i) {
            console.log('🔘 Found upsell button #' + (i+1) + ':', {
                href: btn.getAttribute('href'),
                dataUpsell: btn.getAttribute('data-upsell'),
                text: btn.textContent.substring(0, 50)
            });
            
            btn.addEventListener('click', function(e) {
                console.log('🖱️ Upsell button clicked!', {
                    timestamp: new Date().toISOString(),
                    button: e.target.textContent.substring(0, 30),
                    href: e.target.getAttribute('href'),
                    defaultPrevented: e.defaultPrevented
                });
                
                // Track if click was processed
                setTimeout(function() {
                    console.log('⏱️ 1 second after click - page should be redirecting or showing Monetizze popup');
                }, 1000);
                
                setTimeout(function() {
                    console.log('⏱️ 3 seconds after click - if you see this, redirect may have failed');
                }, 3000);
            }, true); // Use capture phase to log before other handlers
        });
        
        console.log('✅ Monetizze debug listeners attached');
    });
})();
