/**
 * A/B Testing System v1.0
 * Split traffic between variants and track conversions
 * 
 * Usage:
 *   await ABTesting.init('espanhol-afiliados');  // Initialize on page load
 *   ABTesting.shouldShowVSL();                   // Check if should show VSL
 *   ABTesting.trackConversion('lead', 37);       // Track conversion
 */

const ABTesting = {
    API_URL: 'https://zapspy-funnel-production.up.railway.app',
    
    // Get funnel name from URL or default
    getFunnelName: function() {
        const path = window.location.pathname;
        const host = window.location.host;
        
        // Check for Spanish funnels
        if (path.includes('espanhol-afiliados')) return 'espanhol-afiliados';
        if (path.includes('espanhol')) return 'espanhol';
        
        // Check for English affiliate funnel
        if (path.includes('ingles-afiliados')) return 'ingles-afiliados';
        if (host.includes('afiliado')) return 'ingles-afiliados';
        
        // Default to English main funnel
        return 'ingles';
    },
    
    // Get visitor ID (uses FacebookCAPI if available)
    getVisitorId: function() {
        if (typeof FacebookCAPI !== 'undefined') {
            return FacebookCAPI.getVisitorId();
        }
        
        // Fallback if FacebookCAPI not loaded
        let visitorId = localStorage.getItem('funnelVisitorId');
        if (!visitorId) {
            visitorId = 'v_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
            localStorage.setItem('funnelVisitorId', visitorId);
        }
        return visitorId;
    },
    
    // Get or assign variant for current visitor
    getVariant: async function(funnel = null) {
        const funnelName = funnel || this.getFunnelName();
        const visitorId = this.getVisitorId();
        
        // Check URL for forced variant (useful for testing)
        const urlParams = new URLSearchParams(window.location.search);
        const forcedVariant = urlParams.get('variant');
        const forcedTestId = urlParams.get('ab_test');
        
        if (forcedVariant && ['A', 'B'].includes(forcedVariant.toUpperCase())) {
            const v = forcedVariant.toUpperCase();
            localStorage.setItem('ab_variant', v);
            localStorage.setItem('ab_test_id', forcedTestId || '');
            console.log('🧪 A/B: Forced variant', v);
            return { variant: v, param: v === 'A' ? 'control' : 'test' };
        }
        
        // Check if already assigned in this session
        const savedVariant = localStorage.getItem('ab_variant');
        const savedTestId = localStorage.getItem('ab_test_id');
        if (savedVariant && savedTestId) {
            console.log('🧪 A/B: Using saved variant', savedVariant);
            return { 
                variant: savedVariant, 
                test_id: savedTestId, 
                param: savedVariant === 'A' ? 'control' : 'test' 
            };
        }
        
        // Request variant from API
        try {
            const response = await fetch(
                `${this.API_URL}/api/ab/variant?funnel=${encodeURIComponent(funnelName)}&visitor_id=${encodeURIComponent(visitorId)}`
            );
            const data = await response.json();
            
            if (data.variant) {
                localStorage.setItem('ab_variant', data.variant);
                localStorage.setItem('ab_test_id', data.test_id || '');
                console.log('🧪 A/B: Assigned variant', data.variant, 'for test', data.test_id);
            } else {
                console.log('🧪 A/B: No active test for this funnel');
            }
            
            return data;
        } catch (error) {
            console.warn('🧪 A/B: Error getting variant:', error);
            return { variant: null, test_id: null };
        }
    },
    
    // Track conversion event
    trackConversion: async function(eventType, value = 0, metadata = {}) {
        const testId = localStorage.getItem('ab_test_id');
        const visitorId = this.getVisitorId();
        
        if (!testId) {
            console.log('🧪 A/B: No test ID, skipping conversion');
            return;
        }
        
        try {
            const response = await fetch(`${this.API_URL}/api/ab/convert`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    test_id: parseInt(testId),
                    visitor_id: visitorId,
                    event_type: eventType,
                    value: value,
                    metadata: metadata
                })
            });
            
            if (response.ok) {
                console.log('🧪 A/B: Conversion tracked:', eventType, value ? `$${value}` : '');
            }
        } catch (error) {
            console.warn('🧪 A/B: Error tracking conversion:', error);
        }
    },
    
    // Get current variant (sync, from localStorage)
    getCurrentVariant: function() {
        return localStorage.getItem('ab_variant');
    },
    
    // Get current test ID (sync, from localStorage)
    getCurrentTestId: function() {
        return localStorage.getItem('ab_test_id');
    },
    
    // Check if should show VSL (Variant B = test = with VSL)
    shouldShowVSL: function() {
        const variant = this.getCurrentVariant();
        // If no test running (variant is null), default to showing VSL
        if (!variant) return true;
        // Variant B = test version = with VSL
        return variant === 'B';
    },
    
    // Check if should skip VSL (Variant A = control = without VSL)
    shouldSkipVSL: function() {
        const variant = this.getCurrentVariant();
        // Only skip if explicitly assigned to variant A
        return variant === 'A';
    },
    
    // Check if there's an active test
    hasActiveTest: function() {
        return !!localStorage.getItem('ab_test_id');
    },
    
    // Clear test data (useful for testing)
    reset: function() {
        localStorage.removeItem('ab_variant');
        localStorage.removeItem('ab_test_id');
        console.log('🧪 A/B: Test data cleared');
    },
    
    // Initialize A/B testing
    init: async function(funnel = null) {
        console.log('🧪 A/B Testing: Initializing...');
        const result = await this.getVariant(funnel);
        
        if (result.variant) {
            console.log('🧪 A/B Testing: Ready');
            console.log('   Funnel:', funnel || this.getFunnelName());
            console.log('   Variant:', result.variant);
            console.log('   Test ID:', result.test_id);
        } else {
            console.log('🧪 A/B Testing: No active test');
        }
        
        return result;
    }
};

// Export for module systems (optional)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ABTesting;
}
