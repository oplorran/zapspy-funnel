/**
 * A/B Testing System v2.0
 * Split traffic between variants and track conversions
 * 
 * Usage:
 *   await ABTesting.init('ingles');  // Initialize on page load
 *   ABTesting.shouldShowVSL();       // Check if should show VSL (for VSL tests)
 *   ABTesting.getConfig();           // Get current variant config
 *   ABTesting.applyConfig('cta');    // Apply config to page (cta or phone)
 *   ABTesting.trackConversion('lead', 47);  // Track conversion
 * 
 * Test Types:
 *   - vsl: Show/hide VSL video
 *   - price: Different prices and checkout URLs
 *   - headline: Different headlines and subheadlines
 *   - page: Redirect to different page URLs
 */

const ABTesting = {
    API_URL: 'https://zapspy-funnel-production.up.railway.app',
    
    // Current test config (set during init)
    _currentConfig: null,
    _currentTestType: null,
    
    // Get funnel name from URL or default
    getFunnelName: function() {
        const path = window.location.pathname;
        const host = window.location.host;
        
        // Check for Spanish funnels
        if (path.includes('espanhol-afiliados')) return 'espanhol-afiliados';
        if (path.includes('espanhol')) return 'espanhol';
        
        // Check for PerfectPay funnel
        if (path.includes('perfectpay')) return 'perfectpay';
        
        // Check for English affiliate funnel
        if (path.includes('ingles-afiliados')) return 'ingles-afiliados';
        if (host.includes('afiliado')) return 'ingles-afiliados';
        
        // Default to PerfectPay funnel
        return 'perfectpay';
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
        const savedConfig = localStorage.getItem('ab_config');
        const savedTestType = localStorage.getItem('ab_test_type');
        
        if (savedVariant && savedTestId) {
            console.log('🧪 A/B: Using saved variant', savedVariant);
            
            // Restore config from localStorage
            if (savedConfig) {
                try {
                    this._currentConfig = JSON.parse(savedConfig);
                    this._currentTestType = savedTestType || 'vsl';
                } catch (e) {
                    this._currentConfig = null;
                }
            }
            
            return { 
                variant: savedVariant, 
                test_id: savedTestId, 
                param: savedVariant === 'A' ? 'control' : 'test',
                config: this._currentConfig,
                test_type: this._currentTestType
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
                
                // Store config and test type
                if (data.config) {
                    localStorage.setItem('ab_config', JSON.stringify(data.config));
                    this._currentConfig = data.config;
                }
                if (data.test_type) {
                    localStorage.setItem('ab_test_type', data.test_type);
                    this._currentTestType = data.test_type;
                }
                
                console.log('🧪 A/B: Assigned variant', data.variant, 'for test', data.test_id);
                console.log('🧪 A/B: Test type:', data.test_type, 'Config:', data.config);
            } else {
                console.log('🧪 A/B: No active test for this funnel');
            }
            
            return data;
        } catch (error) {
            console.warn('🧪 A/B: Error getting variant:', error);
            return { variant: null, test_id: null, config: null };
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
        localStorage.removeItem('ab_config');
        localStorage.removeItem('ab_test_type');
        this._currentConfig = null;
        this._currentTestType = null;
        console.log('🧪 A/B: Test data cleared');
    },
    
    // Get current config (for the assigned variant)
    getConfig: function() {
        if (this._currentConfig) {
            return this._currentConfig;
        }
        
        // Try to load from localStorage
        const savedConfig = localStorage.getItem('ab_config');
        if (savedConfig) {
            try {
                this._currentConfig = JSON.parse(savedConfig);
                return this._currentConfig;
            } catch (e) {
                return null;
            }
        }
        
        return null;
    },
    
    // Get current test type
    getTestType: function() {
        if (this._currentTestType) {
            return this._currentTestType;
        }
        return localStorage.getItem('ab_test_type') || 'vsl';
    },
    
    // Get price from config (for price tests)
    getPrice: function() {
        const config = this.getConfig();
        if (config && typeof config.price !== 'undefined') {
            return config.price;
        }
        return null; // Return null to use default
    },
    
    // Get original price from config (for price tests)
    getOriginalPrice: function() {
        const config = this.getConfig();
        if (config && typeof config.original_price !== 'undefined') {
            return config.original_price;
        }
        return null;
    },
    
    // Get checkout code from config (for price tests)
    getCheckoutCode: function() {
        const config = this.getConfig();
        if (config && config.checkout_code) {
            return config.checkout_code;
        }
        return null;
    },
    
    // Get headline from config (for headline tests)
    getHeadline: function() {
        const config = this.getConfig();
        if (config && config.headline) {
            return config.headline;
        }
        return null;
    },
    
    // Get subheadline from config (for headline tests)
    getSubheadline: function() {
        const config = this.getConfig();
        if (config && config.subheadline) {
            return config.subheadline;
        }
        return null;
    },
    
    // Get redirect URL from config (for page tests)
    getRedirectUrl: function() {
        const config = this.getConfig();
        if (config && config.redirect_url) {
            return config.redirect_url;
        }
        return null;
    },
    
    // Apply config to a page type
    applyConfig: function(pageType) {
        const config = this.getConfig();
        const testType = this.getTestType();
        
        if (!config) {
            console.log('🧪 A/B: No config to apply');
            return false;
        }
        
        console.log('🧪 A/B: Applying config for', pageType, 'page. Type:', testType);
        
        switch (testType) {
            case 'vsl':
                // VSL handling is done in phone.html startHacking function
                console.log('🧪 A/B: VSL test - show_vsl:', config.show_vsl);
                break;
                
            case 'price':
                this._applyPriceConfig(config);
                break;
                
            case 'headline':
                this._applyHeadlineConfig(config);
                break;
                
            case 'page':
                // Page redirect - only apply on landing page
                if (config.redirect_url && pageType === 'landing') {
                    console.log('🧪 A/B: Redirecting to:', config.redirect_url);
                    window.location.href = config.redirect_url;
                }
                break;
        }
        
        return true;
    },
    
    // Internal: Apply price config
    _applyPriceConfig: function(config) {
        if (!config.price) return;
        
        console.log('🧪 A/B: Applying price config - $' + config.price);
        
        // Update price display elements
        const priceElements = document.querySelectorAll('.price-current, .current-price, [data-ab-price]');
        priceElements.forEach(el => {
            el.textContent = '$' + config.price;
        });
        
        // Update original price (strikethrough)
        if (config.original_price) {
            const originalPriceElements = document.querySelectorAll('.price-original, .original-price, [data-ab-original-price]');
            originalPriceElements.forEach(el => {
                el.textContent = '$' + config.original_price;
            });
            
            // Update discount badge
            const discount = Math.round((1 - config.price / config.original_price) * 100);
            const discountElements = document.querySelectorAll('.discount-badge, [data-ab-discount]');
            discountElements.forEach(el => {
                el.textContent = discount + '% OFF';
            });
        }
        
        // Update checkout URL if provided
        if (config.checkout_code) {
            // Update window.checkoutUrl if it exists
            if (typeof window.checkoutUrl !== 'undefined') {
                window.checkoutUrl = 'https://go.centerpag.com/' + config.checkout_code;
                console.log('🧪 A/B: Updated checkoutUrl to:', window.checkoutUrl);
            }
            
            // Update window.checkoutCode for PerfectPay
            window.checkoutCode = config.checkout_code;
            
            // Update any checkout links
            const checkoutLinks = document.querySelectorAll('a[href*="centerpag"], [data-checkout-link]');
            checkoutLinks.forEach(link => {
                const currentHref = link.getAttribute('href');
                if (currentHref && currentHref.includes('centerpag.com/')) {
                    link.setAttribute('href', 'https://go.centerpag.com/' + config.checkout_code);
                }
            });
        }
        
        // Update Facebook CAPI value tracking
        if (typeof window.productValue !== 'undefined') {
            window.productValue = config.price;
        }
    },
    
    // Internal: Apply headline config
    _applyHeadlineConfig: function(config) {
        if (!config.headline) return;
        
        console.log('🧪 A/B: Applying headline config');
        
        // Update main headline
        const headlineElements = document.querySelectorAll('h1, .main-headline, [data-ab-headline]');
        if (headlineElements.length > 0) {
            headlineElements[0].textContent = config.headline;
        }
        
        // Update subheadline if provided
        if (config.subheadline) {
            const subheadlineElements = document.querySelectorAll('.subheadline, .subtitle, [data-ab-subheadline]');
            if (subheadlineElements.length > 0) {
                subheadlineElements[0].textContent = config.subheadline;
            }
        }
    },
    
    // Initialize A/B testing
    init: async function(funnel = null, pageType = null) {
        console.log('🧪 A/B Testing v2.0: Initializing...');
        const result = await this.getVariant(funnel);
        
        if (result.variant) {
            console.log('🧪 A/B Testing: Ready');
            console.log('   Funnel:', funnel || this.getFunnelName());
            console.log('   Variant:', result.variant);
            console.log('   Test ID:', result.test_id);
            console.log('   Test Type:', result.test_type || 'vsl');
            console.log('   Config:', result.config);
            
            // Auto-apply config if page type is provided
            if (pageType) {
                this.applyConfig(pageType);
            }
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
