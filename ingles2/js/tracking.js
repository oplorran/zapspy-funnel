/**
 * ZapSpy.ai - Enhanced Tracking System
 * Granular tracking events for Meta Pixel and scroll depth
 */

const ZapSpyTracking = {
    // Scroll depth milestones
    scrollMilestones: [25, 50, 75, 100],
    scrollMilestonesReached: {},
    
    // Time on page tracking
    pageLoadTime: Date.now(),
    timeEvents: [30, 60, 120, 300], // seconds
    timeEventsTriggered: {},
    
    /**
     * Initialize tracking
     */
    init: function() {
        this.trackScrollDepth();
        this.trackTimeOnPage();
        this.trackButtonClicks();
        this.trackFormInteractions();
    },
    
    /**
     * Track scroll depth percentage
     */
    trackScrollDepth: function() {
        const self = this;
        
        const handleScroll = () => {
            const scrollTop = window.scrollY || document.documentElement.scrollTop;
            const docHeight = document.documentElement.scrollHeight - window.innerHeight;
            const scrollPercent = Math.round((scrollTop / docHeight) * 100);
            
            self.scrollMilestones.forEach(milestone => {
                if (scrollPercent >= milestone && !self.scrollMilestonesReached[milestone]) {
                    self.scrollMilestonesReached[milestone] = true;
                    self.trackEvent('ScrollDepth', { 
                        percent: milestone,
                        page: window.location.pathname
                    });
                }
            });
        };
        
        // Throttle scroll events
        let ticking = false;
        window.addEventListener('scroll', () => {
            if (!ticking) {
                window.requestAnimationFrame(() => {
                    handleScroll();
                    ticking = false;
                });
                ticking = true;
            }
        });
    },
    
    /**
     * Track time on page
     */
    trackTimeOnPage: function() {
        const self = this;
        
        const checkTime = () => {
            const secondsOnPage = Math.floor((Date.now() - self.pageLoadTime) / 1000);
            
            self.timeEvents.forEach(seconds => {
                if (secondsOnPage >= seconds && !self.timeEventsTriggered[seconds]) {
                    self.timeEventsTriggered[seconds] = true;
                    self.trackEvent('TimeOnPage', { 
                        seconds: seconds,
                        page: window.location.pathname
                    });
                }
            });
        };
        
        setInterval(checkTime, 5000);
    },
    
    /**
     * Track button clicks
     */
    trackButtonClicks: function() {
        document.addEventListener('click', (e) => {
            const button = e.target.closest('button, .btn, [role="button"]');
            if (button) {
                const buttonText = button.textContent?.trim().substring(0, 50) || 'Unknown';
                const buttonClass = button.className || '';
                
                this.trackEvent('ButtonClick', {
                    button_text: buttonText,
                    button_class: buttonClass,
                    page: window.location.pathname
                });
            }
        });
    },
    
    /**
     * Track form interactions
     */
    trackFormInteractions: function() {
        // Track phone input focus
        const phoneInput = document.getElementById('phoneInput');
        if (phoneInput) {
            phoneInput.addEventListener('focus', () => {
                this.trackEvent('FormStart', {
                    field: 'phone',
                    page: window.location.pathname
                });
            }, { once: true });
        }
    },
    
    /**
     * Generate unique event ID for deduplication
     */
    generateEventId: function(eventName) {
        return eventName + '_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
    },

    /**
     * Track AddToCart event (used before checkout) - Standard event
     */
    trackAddToCart: function(value = 49.00, currency = 'USD') {
        this.trackStandardEvent('AddToCart', {
            content_name: 'ZapSpy.ai VIP Access',
            content_category: 'Subscription',
            currency: currency,
            value: value
        });
    },
    
    /**
     * Track custom event with eventID for deduplication
     * @param {string} eventName - The event name
     * @param {Object} params - Event parameters
     */
    trackEvent: function(eventName, params = {}) {
        const eventId = this.generateEventId(eventName);
        
        if (typeof fbq !== 'undefined') {
            fbq('trackCustom', eventName, params, { eventID: eventId });
        }
        
        if (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1') {
            console.log('Track Event:', eventName, params, 'eventID:', eventId);
        }
    },
    
    /**
     * Track standard Meta Pixel events with eventID for deduplication
     * @param {string} eventName - Standard event name (PageView, Lead, InitiateCheckout, etc.)
     * @param {Object} params - Event parameters
     */
    trackStandardEvent: function(eventName, params = {}) {
        const eventId = this.generateEventId(eventName);
        
        if (typeof fbq !== 'undefined') {
            fbq('track', eventName, params, { eventID: eventId });
        }
    }
};

// Auto-initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    ZapSpyTracking.init();
});

// Export for use in other scripts
window.ZapSpyTracking = ZapSpyTracking;
