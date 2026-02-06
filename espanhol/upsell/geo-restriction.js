/**
 * Geo-Restriction Script
 * Logic:
 * - Desktop: ALWAYS show prices (any country)
 * - Mobile + Brazil: show prices
 * - Mobile + Other countries: hide prices
 * Strict mode: if API fails on mobile, price stays HIDDEN
 */
(function() {
    var elements = document.querySelectorAll('.geo-br-only');
    var TIMEOUT_MS = 5000; // 5 second timeout
    var requestCompleted = false;
    
    // Detect if user is on mobile device
    function isMobileDevice() {
        var userAgent = navigator.userAgent || navigator.vendor || window.opera;
        
        // Check for mobile keywords in user agent
        var mobileKeywords = /android|webos|iphone|ipad|ipod|blackberry|iemobile|opera mini|mobile|tablet/i;
        
        // Also check screen width as backup
        var isSmallScreen = window.innerWidth <= 768;
        
        return mobileKeywords.test(userAgent.toLowerCase()) || isSmallScreen;
    }
    
    function showPrices() {
        elements.forEach(function(el) {
            el.classList.add('geo-show');
        });
    }
    
    // If desktop, show prices immediately and skip geo check
    if (!isMobileDevice()) {
        console.log('Desktop detected - showing prices (no geo check needed)');
        showPrices();
        return; // Exit early, no need for API call
    }
    
    // Mobile device - need to check geo location
    console.log('Mobile detected - checking geo location...');
    
    // Timeout: if API takes too long, keep prices HIDDEN (strict mode for mobile)
    var fallbackTimer = setTimeout(function() {
        if (!requestCompleted) {
            console.log('Geo API timeout - prices remain hidden (mobile strict mode)');
            requestCompleted = true;
            // Do NOT show prices - keep them hidden
        }
    }, TIMEOUT_MS);
    
    // Using XMLHttpRequest per RapidAPI documentation
    var xhr = new XMLHttpRequest();
    xhr.withCredentials = true;
    
    xhr.addEventListener('readystatechange', function () {
        if (this.readyState === this.DONE && !requestCompleted) {
            requestCompleted = true;
            clearTimeout(fallbackTimer);
            
            try {
                // Check for HTTP errors - keep prices hidden
                if (this.status !== 200) {
                    console.log('Geo API error (status ' + this.status + ') - prices remain hidden');
                    return; // Keep prices hidden
                }
                
                var data = JSON.parse(this.responseText);
                console.log('Geo API response:', data);
                
                // Check for Brazil - ONLY case where we show prices on mobile
                if (data.country && (
                    data.country.code === 'BR' || 
                    data.country.name === 'Brazil' ||
                    data.country.iso3 === 'BRA'
                )) {
                    showPrices();
                    console.log('BRAZIL DETECTED (mobile) - showing price');
                } else if (data.country_code === 'BR' || data.country_name === 'Brazil') {
                    // Alternative response format
                    showPrices();
                    console.log('BRAZIL DETECTED (mobile) - showing price');
                } else {
                    console.log('Not Brazil (mobile) - price hidden. Country:', data.country || data.country_name);
                    // Keep prices hidden
                }
            } catch (err) {
                console.log('Geo API parsing error - prices remain hidden:', err);
                // Keep prices hidden on parse error
            }
        }
    });
    
    xhr.addEventListener('error', function() {
        if (!requestCompleted) {
            requestCompleted = true;
            clearTimeout(fallbackTimer);
            console.log('Geo API network error - prices remain hidden');
            // Keep prices hidden on network error
        }
    });
    
    // Visitor Lookup endpoint - auto-detects visitor IP
    xhr.open('GET', 'https://ip-geo-location.p.rapidapi.com/ip/check?format=json');
    xhr.setRequestHeader('x-rapidapi-key', 'd03f07c7c6mshb0e213b53734dcbp1c2ccfjsnc7937b7aa611');
    xhr.setRequestHeader('x-rapidapi-host', 'ip-geo-location.p.rapidapi.com');
    
    xhr.send(null);
})();
