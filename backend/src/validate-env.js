/**
 * Environment variable validation
 * Checks required variables on startup and warns about missing optional ones.
 */

function validateEnv() {
    const errors = [];
    const warnings = [];

    // Required variables
    const required = [
        { name: 'DATABASE_URL', desc: 'PostgreSQL connection string' },
        { name: 'JWT_SECRET', desc: 'JWT signing secret for admin authentication' }
    ];

    for (const { name, desc } of required) {
        if (!process.env[name]) {
            errors.push(`  ❌ ${name} - ${desc}`);
        }
    }

    // Optional but recommended
    const optional = [
        { name: 'ADMIN_EMAIL', desc: 'Master admin email', default: 'admin@zapspy.ai' },
        { name: 'ADMIN_PASSWORD', desc: 'Master admin password', default: '(insecure default)' },
        { name: 'MONETIZZE_CONSUMER_KEY', desc: 'Monetizze API key (sync disabled without it)' },
        { name: 'FB_PIXEL_TOKEN_EN', desc: 'Facebook Pixel token for English funnel' },
        { name: 'FB_PIXEL_TOKEN_ES', desc: 'Facebook Pixel token for Spanish funnel' },
        { name: 'ZAPI_INSTANCE_ID', desc: 'Z-API instance for WhatsApp (using fallback)' },
        { name: 'ZAPI_TOKEN', desc: 'Z-API token (using fallback)' },
        { name: 'FRONTEND_URL', desc: 'Allowed CORS origins (using defaults)' },
        { name: 'AC_API_URL', desc: 'ActiveCampaign API URL (recovery automations disabled without it)' },
        { name: 'AC_API_KEY', desc: 'ActiveCampaign API Key (recovery automations disabled without it)' }
    ];

    for (const { name, desc } of optional) {
        if (!process.env[name]) {
            warnings.push(`  ⚠️  ${name} - ${desc}`);
        }
    }

    // Print results
    if (errors.length > 0) {
        console.error('\n🚨 MISSING REQUIRED ENVIRONMENT VARIABLES:');
        errors.forEach(e => console.error(e));
        console.error('\nThe server will start but some features will not work correctly.\n');
    }

    if (warnings.length > 0) {
        console.log('\n📋 Missing optional environment variables (using defaults):');
        warnings.forEach(w => console.log(w));
        console.log('');
    }

    if (errors.length === 0 && warnings.length === 0) {
        console.log('✅ All environment variables configured');
    }

    return { errors: errors.length, warnings: warnings.length };
}

module.exports = { validateEnv };
