# Facebook Conversions API - Implementation Guide

## Overview

The Facebook Conversions API (CAPI) allows you to send web events directly from your server to Facebook's servers, rather than relying solely on browser-side tracking. This provides several benefits:

1. **Better data accuracy** - Not affected by browser privacy settings or ad blockers
2. **Improved attribution** - Events can be matched to users more reliably
3. **Higher event match quality** - By sending user identifiers from the server

## Requirements

To implement the Conversions API, you need:

1. **Facebook Business Manager account**
2. **Facebook Pixel ID**: `955477126807496`
3. **Access Token**: Generate from Events Manager > Settings > Conversions API
4. **Server-side infrastructure** (Node.js, PHP, Python, or any server language)

## Implementation Options

### Option 1: Direct API Integration (Recommended)

Create a server endpoint that receives events from your frontend and forwards them to Facebook.

#### Node.js Example:

```javascript
const axios = require('axios');
const crypto = require('crypto');

const PIXEL_ID = '955477126807496';
const ACCESS_TOKEN = 'YOUR_ACCESS_TOKEN_HERE';
const API_VERSION = 'v18.0';

function hashData(data) {
    return crypto.createHash('sha256').update(data.toLowerCase().trim()).digest('hex');
}

async function sendEvent(eventData) {
    const { eventName, eventTime, userData, customData, sourceUrl, actionSource } = eventData;
    
    const payload = {
        data: [{
            event_name: eventName,
            event_time: eventTime || Math.floor(Date.now() / 1000),
            action_source: actionSource || 'website',
            event_source_url: sourceUrl,
            user_data: {
                em: userData.email ? hashData(userData.email) : undefined,
                ph: userData.phone ? hashData(userData.phone.replace(/\D/g, '')) : undefined,
                client_ip_address: userData.ip,
                client_user_agent: userData.userAgent,
                fbc: userData.fbc, // Facebook Click ID
                fbp: userData.fbp  // Facebook Browser ID
            },
            custom_data: customData
        }],
        access_token: ACCESS_TOKEN
    };
    
    try {
        const response = await axios.post(
            `https://graph.facebook.com/${API_VERSION}/${PIXEL_ID}/events`,
            payload
        );
        return response.data;
    } catch (error) {
        console.error('CAPI Error:', error.response?.data || error.message);
        throw error;
    }
}

// Express.js endpoint example
app.post('/api/track-event', async (req, res) => {
    try {
        const result = await sendEvent({
            eventName: req.body.event_name,
            sourceUrl: req.body.source_url,
            userData: {
                email: req.body.email,
                phone: req.body.phone,
                ip: req.ip,
                userAgent: req.get('User-Agent'),
                fbc: req.cookies._fbc,
                fbp: req.cookies._fbp
            },
            customData: req.body.custom_data
        });
        res.json({ success: true, result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});
```

### Option 2: Third-party Integration

Use third-party services that handle CAPI for you:

- **Stape.io** - Easy server-side tracking
- **GTM Server-Side** - Google Tag Manager with server container
- **Segment** - Customer data platform with CAPI integration

## Events to Track

Based on your current funnel, here are the events to send via CAPI:

| Event | Page | Custom Data |
|-------|------|-------------|
| `PageView` | All pages | page_url, page_title |
| `Lead` | phone.html | content_name, phone (hashed) |
| `ViewContent` | conversas.html, chat.html | content_name, content_category |
| `AddToCart` | cta-unified.html | value, currency, content_name |
| `InitiateCheckout` | cta-unified.html (on CTA click) | value, currency |
| `Purchase` | Thank you page | value, currency, order_id |

## Frontend Integration

Update your `js/tracking.js` to also send events to your server:

```javascript
// Add to ZapSpyTracking object
sendToServer: async function(eventName, params = {}) {
    try {
        // Get Facebook cookies
        const fbc = document.cookie.match(/_fbc=([^;]+)/)?.[1];
        const fbp = document.cookie.match(/_fbp=([^;]+)/)?.[1];
        
        await fetch('/api/track-event', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                event_name: eventName,
                source_url: window.location.href,
                email: localStorage.getItem('userEmail'),
                phone: localStorage.getItem('userWhatsApp'),
                fbc: fbc,
                fbp: fbp,
                custom_data: params
            })
        });
    } catch (error) {
        console.error('Server tracking error:', error);
    }
}
```

## Testing

1. Use **Facebook Events Manager** > Test Events
2. Send test events and verify they appear
3. Check **Event Match Quality** score (aim for 7+)
4. Use **Aggregated Event Measurement** for iOS users

## Best Practices

1. **Always deduplicate** - Use `event_id` parameter with same value in both Pixel and CAPI
2. **Hash user data** - Always SHA-256 hash PII before sending
3. **Send as much data as possible** - More user data = better match rate
4. **Include external_id** - A consistent user identifier improves matching
5. **Monitor Event Match Quality** - Available in Events Manager

## Resources

- [Facebook Conversions API Documentation](https://developers.facebook.com/docs/marketing-api/conversions-api)
- [Facebook Pixel and CAPI Integration Guide](https://www.facebook.com/business/help/308855623839366)
- [Events Manager](https://business.facebook.com/events_manager2)

## Note

The Conversions API requires server-side implementation which is beyond simple frontend changes. This documentation serves as a guide for implementation when server infrastructure is available.
