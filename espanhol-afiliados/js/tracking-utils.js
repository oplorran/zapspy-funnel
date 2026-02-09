/**
 * Tracking Utilities
 * Centralized UTM capture, Visitor ID generation, and retry logic
 * Must be loaded FIRST before other tracking scripts
 */

const TrackingUtils = {
    // ============================================
    // UTM CAPTURE AND PRESERVATION
    // ============================================
    
    utmParams: ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'],
    
    /**
     * Capture UTMs from URL and save to localStorage
     * Only saves if utm_source or utm_campaign present (fresh visit from ad)
     */
    captureUTMs: function() {
        const urlParams = new URLSearchParams(window.location.search);
        
        if (urlParams.has('utm_source') || urlParams.has('utm_campaign')) {
            this.utmParams.forEach(param => {
                const value = urlParams.get(param);
                if (value) {
                    localStorage.setItem(param, value);
                }
            });
            console.log('[TrackingUtils] Captured UTMs from URL:', this.getStoredUTMs());
        }
    },
    
    /**
     * Get all stored UTMs from localStorage
     * @returns {Object} UTM values
     */
    getStoredUTMs: function() {
        const utms = {};
        this.utmParams.forEach(param => {
            const value = localStorage.getItem(param);
            if (value) {
                utms[param] = value;
            }
        });
        return utms;
    },
    
    /**
     * Build UTM query string for checkout URLs
     * @param {Object} defaults - Default values if UTMs not in localStorage
     * @returns {String} Query string (without leading ?)
     */
    buildUTMQueryString: function(defaults = {}) {
        const params = [];
        
        this.utmParams.forEach(param => {
            const value = localStorage.getItem(param) || defaults[param];
            if (value) {
                params.push(`${param}=${encodeURIComponent(value)}`);
            }
        });
        
        return params.join('&');
    },
    
    // ============================================
    // VISITOR ID (Centralized)
    // ============================================
    
    /**
     * Get or create a unique visitor ID
     * @returns {String} Visitor ID
     */
    getVisitorId: function() {
        let visitorId = localStorage.getItem('funnelVisitorId');
        if (!visitorId) {
            visitorId = 'v_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
            localStorage.setItem('funnelVisitorId', visitorId);
            console.log('[TrackingUtils] Generated new Visitor ID:', visitorId);
        }
        return visitorId;
    },
    
    // ============================================
    // RETRY LOGIC FOR TRACKING CALLS
    // ============================================
    
    /**
     * Send tracking request with retry logic
     * @param {String} url - API endpoint
     * @param {Object} data - Data to send
     * @param {Object} options - Options (retries, backoff)
     * @returns {Promise}
     */
    sendWithRetry: async function(url, data, options = {}) {
        const maxRetries = options.retries || 3;
        const baseBackoff = options.backoff || 1000; // 1 second
        
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                const response = await fetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(data)
                });
                
                if (response.ok) {
                    return { success: true, response };
                }
                
                // If server error (5xx), retry; if client error (4xx), don't retry
                if (response.status < 500) {
                    console.warn('[TrackingUtils] Client error, not retrying:', response.status);
                    return { success: false, status: response.status };
                }
                
            } catch (error) {
                console.warn(`[TrackingUtils] Attempt ${attempt + 1}/${maxRetries} failed:`, error.message);
            }
            
            // Wait before retry (exponential backoff)
            if (attempt < maxRetries - 1) {
                const waitTime = baseBackoff * Math.pow(2, attempt);
                await new Promise(resolve => setTimeout(resolve, waitTime));
            }
        }
        
        console.error('[TrackingUtils] All retry attempts failed for:', url);
        return { success: false, error: 'Max retries reached' };
    },
    
    /**
     * Send tracking using sendBeacon (for page unload)
     * Falls back to fetch if sendBeacon not available
     * @param {String} url - API endpoint
     * @param {Object} data - Data to send
     */
    sendBeacon: function(url, data) {
        const blob = new Blob([JSON.stringify(data)], { type: 'application/json' });
        
        if (navigator.sendBeacon) {
            const sent = navigator.sendBeacon(url, blob);
            if (sent) {
                console.log('[TrackingUtils] Beacon sent successfully');
                return true;
            }
        }
        
        // Fallback to fetch with keepalive
        fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
            keepalive: true
        }).catch(err => console.warn('[TrackingUtils] Beacon fallback error:', err));
        
        return false;
    },
    
    // ============================================
    // INITIALIZATION
    // ============================================
    
    /**
     * Initialize tracking utilities
     * Call this on every page load
     */
    init: function() {
        // Capture UTMs from URL (if present)
        this.captureUTMs();
        
        // Ensure visitor ID exists
        this.getVisitorId();
        
        console.log('[TrackingUtils] Initialized', {
            visitorId: localStorage.getItem('funnelVisitorId'),
            utms: this.getStoredUTMs()
        });
    }
};

// Auto-initialize on script load
TrackingUtils.init();

// Export for module systems (if needed)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = TrackingUtils;
}
