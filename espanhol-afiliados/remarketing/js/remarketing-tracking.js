/**
 * ZapSpy.ai - Remarketing Tracking v1.0
 * Facebook Pixel + CAPI + localStorage integration
 */

const RmkTracking = {
    API_URL: 'https://zapspy-funnel-production.up.railway.app',

    generateEventId: function(eventName) {
        return eventName + '_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
    },

    getVisitorId: function() {
        var id = localStorage.getItem('funnelVisitorId');
        if (!id) {
            id = 'v_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
            localStorage.setItem('funnelVisitorId', id);
        }
        return id;
    },

    _getCookie: function(name) {
        var match = document.cookie.split(';').map(function(c) { return c.trim(); }).find(function(c) { return c.indexOf(name + '=') === 0; });
        return match ? match.split('=')[1] : null;
    },

    getFbc: function() {
        var params = new URLSearchParams(window.location.search);
        var fbclid = params.get('fbclid');
        if (fbclid) {
            var fbc = 'fb.1.' + Date.now() + '.' + fbclid;
            localStorage.setItem('_fbc', fbc);
            return fbc;
        }
        var ck = this._getCookie('_fbc');
        if (ck) { localStorage.setItem('_fbc', ck); return ck; }
        return localStorage.getItem('_fbc') || null;
    },

    getFbp: function() {
        var ck = this._getCookie('_fbp');
        if (ck) { localStorage.setItem('_fbp', ck); return ck; }
        var fbp = localStorage.getItem('_fbp');
        if (!fbp) {
            fbp = 'fb.1.' + Date.now() + '.' + Math.floor(Math.random() * 10000000000);
            localStorage.setItem('_fbp', fbp);
        }
        return fbp;
    },

    getUserData: function() {
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

    getTargetData: function() {
        return {
            phone: localStorage.getItem('targetPhone') || null,
            gender: localStorage.getItem('targetGender') || 'male'
        };
    },

    trackEvent: function(eventName, customData) {
        customData = customData || {};
        var eventId = this.generateEventId(eventName);
        var userData = this.getUserData();

        if (typeof fbq !== 'undefined') {
            var pixelData = {};
            for (var k in customData) pixelData[k] = customData[k];
            pixelData.eventID = eventId;
            fbq('track', eventName, pixelData, { eventID: eventId });
        }

        this.sendToServer(eventName, eventId, userData, customData);
        return eventId;
    },

    sendToServer: function(eventName, eventId, userData, customData) {
        customData = customData || {};
        try {
            var payload = {
                eventName: eventName,
                eventId: eventId,
                email: userData.email,
                phone: userData.phone,
                firstName: userData.firstName,
                country: userData.country,
                city: userData.city,
                state: userData.state,
                gender: userData.gender,
                externalId: userData.visitorId,
                fbc: userData.fbc,
                fbp: userData.fbp,
                eventSourceUrl: window.location.href,
                funnelLanguage: 'es'
            };
            for (var k in customData) payload[k] = customData[k];

            fetch(this.API_URL + '/api/capi/event', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            }).catch(function() {});
        } catch(e) {}
    },

    trackPageView: function(pageName) {
        return this.trackEvent('PageView', { content_name: pageName || document.title });
    },

    trackViewContent: function(contentName, contentCategory, value) {
        return this.trackEvent('ViewContent', {
            content_name: contentName,
            content_category: contentCategory,
            value: value || 49,
            currency: 'USD'
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

    buildCheckoutUrl: function(baseCode, value) {
        var url = 'https://pay.monetizze.com.br/' + baseCode;
        var params = [];
        var email = localStorage.getItem('userEmail');
        var name = localStorage.getItem('userName');
        var phone = localStorage.getItem('userWhatsApp');
        var visitorId = this.getVisitorId();
        var fbc = this.getFbc();
        var fbp = this.getFbp();

        if (email) params.push('email=' + encodeURIComponent(email));
        if (name) params.push('name=' + encodeURIComponent(name));
        if (phone) params.push('phone=' + encodeURIComponent(phone));
        if (visitorId) params.push('zs_vid=' + encodeURIComponent(visitorId));
        if (fbc) params.push('fbc=' + encodeURIComponent(fbc));
        if (fbp) params.push('fbp=' + encodeURIComponent(fbp));
        params.push('zs_source=zapspy_remarketing');

        var utmKeys = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'];
        var urlParams = new URLSearchParams(window.location.search);
        utmKeys.forEach(function(k) {
            var v = urlParams.get(k) || localStorage.getItem(k);
            if (v) params.push(k + '=' + encodeURIComponent(v));
        });

        if (params.length > 0) url += '?' + params.join('&');
        return url;
    },

    init: function(pageName, contentCategory, value) {
        this.getFbc();
        this.getFbp();
        this.getVisitorId();
        this.trackPageView(pageName);
        if (contentCategory) {
            this.trackViewContent(pageName, contentCategory, value);
        }
    }
};

// Timer utility
function startTimer(elementId, totalSeconds, onExpire) {
    var el = document.getElementById(elementId);
    if (!el) return;
    var remaining = totalSeconds;
    function update() {
        var m = Math.floor(remaining / 60);
        var s = remaining % 60;
        el.textContent = (m < 10 ? '0' : '') + m + ':' + (s < 10 ? '0' : '') + s;
        if (remaining <= 0) {
            if (onExpire) onExpire();
            return;
        }
        remaining--;
        setTimeout(update, 1000);
    }
    update();
}

// FAQ toggle utility
document.addEventListener('DOMContentLoaded', function() {
    document.querySelectorAll('.rmk-faq-item').forEach(function(item) {
        item.querySelector('.rmk-faq-question').addEventListener('click', function() {
            item.classList.toggle('open');
        });
    });
});

// Live Activity Feed - shows social proof notifications
function startActivityFeed(messages, intervalMs) {
    intervalMs = intervalMs || 8000;
    var idx = 0;
    var container = document.createElement('div');
    container.className = 'rmk-activity-feed';
    document.body.appendChild(container);

    function showNext() {
        var msg = messages[idx % messages.length];
        var item = document.createElement('div');
        item.className = 'rmk-activity-item';
        item.innerHTML = '<div class="rmk-activity-avatar">' + msg.emoji + '</div>' +
            '<div class="rmk-activity-text"><strong>' + msg.name + '</strong> ' + msg.action +
            ' <span class="rmk-activity-time">' + msg.time + '</span></div>';
        container.appendChild(item);
        setTimeout(function() { if (item.parentNode) item.parentNode.removeChild(item); }, 5000);
        idx++;
    }
    setTimeout(showNext, 3000);
    setInterval(showNext, intervalMs);
}

// Tab title manipulation - changes title when user leaves tab
function initTabTitle(urgentTitle) {
    var originalTitle = document.title;
    var isOriginal = true;
    var interval;
    document.addEventListener('visibilitychange', function() {
        if (document.hidden) {
            interval = setInterval(function() {
                document.title = isOriginal ? urgentTitle : originalTitle;
                isOriginal = !isOriginal;
            }, 1500);
        } else {
            clearInterval(interval);
            document.title = originalTitle;
            isOriginal = true;
        }
    });
}

// Exit intent detection
function initExitIntent(config) {
    var shown = false;
    var threshold = config.scrollThreshold || 0.15;

    function showOverlay() {
        if (shown) return;
        shown = true;
        var overlay = document.createElement('div');
        overlay.className = 'rmk-exit-overlay';
        overlay.id = 'exitOverlay';
        overlay.innerHTML = '<div class="rmk-exit-modal">' +
            '<div class="rmk-exit-modal-icon">' + (config.icon || '\u26A0\uFE0F') + '</div>' +
            '<div class="rmk-exit-modal-title">' + config.title + '</div>' +
            '<div class="rmk-exit-modal-text">' + config.text + '</div>' +
            '<button class="rmk-cta ' + (config.ctaClass || 'danger') + '" onclick="' + config.ctaAction + '">' +
            config.ctaLabel + '</button>' +
            '<button class="rmk-exit-modal-close" onclick="document.getElementById(\'exitOverlay\').remove()">No gracias, lo dejo</button>' +
            '</div>';
        overlay.addEventListener('click', function(e) {
            if (e.target === overlay) overlay.remove();
        });
        document.body.appendChild(overlay);
    }

    // Scroll-based detection (mobile-friendly)
    var lastScroll = 0;
    var rapidUpCount = 0;
    window.addEventListener('scroll', function() {
        var curr = window.scrollY;
        if (curr < lastScroll && curr < window.innerHeight * threshold) {
            rapidUpCount++;
            if (rapidUpCount > 3) showOverlay();
        } else {
            rapidUpCount = 0;
        }
        lastScroll = curr;
    });

    // Back button detection
    history.pushState(null, '', location.href);
    window.addEventListener('popstate', function() {
        history.pushState(null, '', location.href);
        showOverlay();
    });
}
