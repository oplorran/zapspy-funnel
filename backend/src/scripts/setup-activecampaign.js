#!/usr/bin/env node
/**
 * ActiveCampaign Setup Script
 * 
 * Creates all tags, lists, and automations needed for the ZapSpy recovery funnel.
 * Run once after configuring ACTIVECAMPAIGN_API_URL and ACTIVECAMPAIGN_API_KEY.
 * 
 * Usage:
 *   node src/scripts/setup-activecampaign.js
 * 
 * Environment variables required:
 *   AC_API_URL - e.g. https://yourname.api-us1.com
 *   AC_API_KEY - API key from Settings > Developer
 */

require('dotenv').config();

const AC_API_URL = process.env.AC_API_URL;
const AC_API_KEY = process.env.AC_API_KEY;

if (!AC_API_URL || !AC_API_KEY) {
    console.error('❌ Missing AC_API_URL or AC_API_KEY');
    console.error('Set them in your .env file or environment variables.');
    process.exit(1);
}

// ==================== API HELPERS ====================

async function apiV3(method, endpoint, body = null) {
    const url = `${AC_API_URL}/api/3/${endpoint}`;
    const options = {
        method,
        headers: {
            'Api-Token': AC_API_KEY,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    };
    if (body && (method === 'POST' || method === 'PUT')) {
        options.body = JSON.stringify(body);
    }
    const response = await fetch(url, options);
    const data = await response.json();
    if (!response.ok) {
        throw new Error(`API Error [${method} ${endpoint}]: ${JSON.stringify(data)}`);
    }
    return data;
}

async function apiV1(action, params = {}) {
    const url = `${AC_API_URL}/admin/api.php`;
    const searchParams = new URLSearchParams({
        api_key: AC_API_KEY,
        api_action: action,
        api_output: 'json',
        ...params
    });
    const response = await fetch(`${url}?${searchParams.toString()}`);
    return await response.json();
}

// ==================== TAGS ====================

const TAGS_TO_CREATE = [
    // Recovery tags (triggers for automations)
    { tag: 'zapspy-lead-en', desc: 'Lead captured - English funnel' },
    { tag: 'zapspy-lead-es', desc: 'Lead captured - Spanish funnel' },
    { tag: 'zapspy-checkout-abandon-en', desc: 'Checkout abandoned - English' },
    { tag: 'zapspy-checkout-abandon-es', desc: 'Checkout abandoned - Spanish' },
    { tag: 'zapspy-sale-cancelled-en', desc: 'Sale cancelled - English' },
    { tag: 'zapspy-sale-cancelled-es', desc: 'Sale cancelled - Spanish' },
    // Status tags
    { tag: 'zapspy-buyer-en', desc: 'Buyer - English funnel' },
    { tag: 'zapspy-buyer-es', desc: 'Buyer - Spanish funnel' },
    { tag: 'zapspy-refunded-en', desc: 'Refunded - English' },
    { tag: 'zapspy-refunded-es', desc: 'Refunded - Spanish' },
    { tag: 'zapspy-chargeback-en', desc: 'Chargeback - English' },
    { tag: 'zapspy-chargeback-es', desc: 'Chargeback - Spanish' },
    // Exclusion tags (stop automations)
    { tag: 'zapspy-do-not-email', desc: 'Do not send recovery emails' },
    { tag: 'zapspy-recovery-completed', desc: 'Recovery sequence completed' }
];

// ==================== LISTS ====================

const LISTS_TO_CREATE = [
    { name: 'ZapSpy - Leads EN', stringid: 'zapspy-leads-en' },
    { name: 'ZapSpy - Leads ES', stringid: 'zapspy-leads-es' },
    { name: 'ZapSpy - Checkout Abandon EN', stringid: 'zapspy-checkout-abandon-en' },
    { name: 'ZapSpy - Checkout Abandon ES', stringid: 'zapspy-checkout-abandon-es' },
    { name: 'ZapSpy - Sale Cancelled EN', stringid: 'zapspy-sale-cancelled-en' },
    { name: 'ZapSpy - Sale Cancelled ES', stringid: 'zapspy-sale-cancelled-es' }
];

// ==================== AUTOMATIONS ====================

const AUTOMATIONS_TO_CREATE = [
    {
        name: 'ZapSpy - Recovery Checkout Abandon EN',
        triggerTag: 'zapspy-checkout-abandon-en',
        excludeTag: 'zapspy-buyer-en',
        language: 'en',
        emails: [
            {
                name: 'Email 1 - Reminder (1h)',
                subject: 'Your ZapSpy.ai report is ready (and waiting)',
                preheader: "The data we found won't be available forever...",
                waitBefore: { hours: 1 }
            },
            {
                name: 'Email 2 - Urgency (24h)',
                subject: '⚠️ Your ZapSpy.ai data is scheduled for deletion',
                preheader: 'We found real data linked to that number. Deletion in 24h.',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 3 - 30% Discount (48h)',
                subject: 'A special 30% discount for you',
                preheader: 'Because you showed interest, we have a special offer...',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 4 - Final 50% OFF (72h)',
                subject: 'Final Offer: 50% OFF your ZapSpy.ai report',
                preheader: 'This is your absolute last chance. 50% OFF expires at midnight.',
                waitBefore: { hours: 24 }
            }
        ]
    },
    {
        name: 'ZapSpy - Recovery Checkout Abandon ES',
        triggerTag: 'zapspy-checkout-abandon-es',
        excludeTag: 'zapspy-buyer-es',
        language: 'es',
        emails: [
            {
                name: 'Email 1 - Recordatorio (1h)',
                subject: 'Tu informe de ZapSpy.ai está listo (y esperando)',
                preheader: 'Los datos que encontramos no estarán disponibles para siempre...',
                waitBefore: { hours: 1 }
            },
            {
                name: 'Email 2 - Urgencia (24h)',
                subject: '⚠️ Tus datos de ZapSpy.ai serán eliminados',
                preheader: 'Encontramos datos reales vinculados a ese número. Eliminación en 24h.',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 3 - 30% Descuento (48h)',
                subject: 'Un descuento especial del 30% para ti',
                preheader: 'Porque mostraste interés, tenemos una oferta especial...',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 4 - Oferta Final 50% (72h)',
                subject: 'Oferta Final: 50% de descuento en tu informe ZapSpy.ai',
                preheader: 'Esta es tu última oportunidad. 50% de descuento expira a medianoche.',
                waitBefore: { hours: 24 }
            }
        ]
    },
    {
        name: 'ZapSpy - Recovery Sale Cancelled EN',
        triggerTag: 'zapspy-sale-cancelled-en',
        excludeTag: 'zapspy-buyer-en',
        language: 'en',
        emails: [
            {
                name: 'Email 1 - Payment Issue (1h)',
                subject: 'There was a problem with your ZapSpy.ai payment',
                preheader: 'Your report is ready but your payment could not be processed.',
                waitBefore: { hours: 1 }
            },
            {
                name: 'Email 2 - Data Expiring (24h)',
                subject: '⚠️ Your investigation data expires in 24 hours',
                preheader: 'The data we collected is real and time-sensitive.',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 3 - 30% Discount (48h)',
                subject: 'We want to help: 30% OFF your ZapSpy.ai report',
                preheader: 'Try again with a special discount just for you.',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 4 - Final 50% OFF (72h)',
                subject: 'Last chance: 50% OFF - Your data will be permanently deleted',
                preheader: 'After midnight, all collected data will be erased forever.',
                waitBefore: { hours: 24 }
            }
        ]
    },
    {
        name: 'ZapSpy - Recovery Sale Cancelled ES',
        triggerTag: 'zapspy-sale-cancelled-es',
        excludeTag: 'zapspy-buyer-es',
        language: 'es',
        emails: [
            {
                name: 'Email 1 - Problema de Pago (1h)',
                subject: 'Hubo un problema con tu pago en ZapSpy.ai',
                preheader: 'Tu informe está listo pero no pudimos procesar tu pago.',
                waitBefore: { hours: 1 }
            },
            {
                name: 'Email 2 - Datos Expirando (24h)',
                subject: '⚠️ Tus datos de investigación expiran en 24 horas',
                preheader: 'Los datos que recopilamos son reales y sensibles al tiempo.',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 3 - 30% Descuento (48h)',
                subject: 'Queremos ayudarte: 30% de descuento en tu informe ZapSpy.ai',
                preheader: 'Intenta de nuevo con un descuento especial solo para ti.',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 4 - Oferta Final 50% (72h)',
                subject: 'Última oportunidad: 50% OFF - Tus datos serán eliminados',
                preheader: 'Después de medianoche, todos los datos recopilados serán borrados.',
                waitBefore: { hours: 24 }
            }
        ]
    },
    {
        name: 'ZapSpy - Recovery Funnel Abandon EN',
        triggerTag: 'zapspy-lead-en',
        excludeTag: 'zapspy-buyer-en',
        language: 'en',
        emails: [
            {
                name: 'Email 1 - Investigation Started (2h)',
                subject: 'Your investigation has already started...',
                preheader: 'We already found data linked to the number you entered.',
                waitBefore: { hours: 2 }
            },
            {
                name: 'Email 2 - Data Found (24h)',
                subject: '🔍 We found something about that number',
                preheader: 'Our system detected activity on multiple platforms.',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 3 - Social Proof (48h)',
                subject: '47,832 people used ZapSpy.ai this week',
                preheader: 'See what they discovered about the numbers they searched.',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 4 - Last Chance 40% OFF (72h)',
                subject: 'Your data will be deleted in 12 hours (40% OFF inside)',
                preheader: 'This is your final chance to see the full report.',
                waitBefore: { hours: 24 }
            }
        ]
    },
    {
        name: 'ZapSpy - Recovery Funnel Abandon ES',
        triggerTag: 'zapspy-lead-es',
        excludeTag: 'zapspy-buyer-es',
        language: 'es',
        emails: [
            {
                name: 'Email 1 - Investigación Iniciada (2h)',
                subject: 'Tu investigación ya ha comenzado...',
                preheader: 'Ya encontramos datos vinculados al número que ingresaste.',
                waitBefore: { hours: 2 }
            },
            {
                name: 'Email 2 - Datos Encontrados (24h)',
                subject: '🔍 Encontramos algo sobre ese número',
                preheader: 'Nuestro sistema detectó actividad en múltiples plataformas.',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 3 - Prueba Social (48h)',
                subject: '47.832 personas usaron ZapSpy.ai esta semana',
                preheader: 'Mira lo que descubrieron sobre los números que buscaron.',
                waitBefore: { hours: 24 }
            },
            {
                name: 'Email 4 - Última Oportunidad 40% (72h)',
                subject: 'Tus datos serán eliminados en 12 horas (40% OFF adentro)',
                preheader: 'Esta es tu última oportunidad de ver el informe completo.',
                waitBefore: { hours: 24 }
            }
        ]
    }
];

// ==================== MAIN SETUP FUNCTION ====================

async function setup() {
    console.log('🚀 ActiveCampaign Setup Starting...');
    console.log(`📡 API URL: ${AC_API_URL}`);
    console.log('');

    // 1. Create Tags
    console.log('📌 Creating Tags...');
    const tagIds = {};
    for (const { tag, desc } of TAGS_TO_CREATE) {
        try {
            // Check if tag already exists
            const existing = await apiV3('GET', `tags?search=${encodeURIComponent(tag)}`);
            const found = existing.tags?.find(t => t.tag === tag);
            if (found) {
                tagIds[tag] = found.id;
                console.log(`  ✅ Tag "${tag}" already exists (ID: ${found.id})`);
            } else {
                const data = await apiV3('POST', 'tags', {
                    tag: { tag, tagType: 'contact', description: desc }
                });
                tagIds[tag] = data.tag.id;
                console.log(`  ✅ Tag "${tag}" created (ID: ${data.tag.id})`);
            }
        } catch (err) {
            console.error(`  ❌ Tag "${tag}" failed: ${err.message}`);
        }
    }
    console.log('');

    // 2. Create Lists
    console.log('📋 Creating Lists...');
    const listIds = {};
    for (const { name, stringid } of LISTS_TO_CREATE) {
        try {
            const existing = await apiV3('GET', `lists?filters[name]=${encodeURIComponent(name)}`);
            const found = existing.lists?.find(l => l.name === name);
            if (found) {
                listIds[name] = found.id;
                console.log(`  ✅ List "${name}" already exists (ID: ${found.id})`);
            } else {
                const data = await apiV3('POST', 'lists', {
                    list: {
                        name,
                        stringid,
                        sender_url: 'https://zapspy.ai',
                        sender_reminder: 'You signed up for ZapSpy.ai monitoring service.'
                    }
                });
                listIds[name] = data.list.id;
                console.log(`  ✅ List "${name}" created (ID: ${data.list.id})`);
            }
        } catch (err) {
            console.error(`  ❌ List "${name}" failed: ${err.message}`);
        }
    }
    console.log('');

    // 3. Create Automations
    console.log('🤖 Creating Automations...');
    for (const automation of AUTOMATIONS_TO_CREATE) {
        try {
            // Check if automation already exists
            const existing = await apiV3('GET', `automations?search=${encodeURIComponent(automation.name)}`);
            const found = existing.automations?.find(a => a.name === automation.name);
            
            if (found) {
                console.log(`  ⏭️  Automation "${automation.name}" already exists (ID: ${found.id}) - skipping`);
                continue;
            }

            // Create automation
            const autoData = await apiV3('POST', 'automations', {
                automation: {
                    name: automation.name,
                    status: 0, // 0 = inactive (activate manually after review)
                    hidden: 0
                }
            });
            const autoId = autoData.automation.id;
            console.log(`  ✅ Automation "${automation.name}" created (ID: ${autoId})`);

            // Get the start block
            const blocksData = await apiV3('GET', `automations/${autoId}/blocks`);
            const startBlock = blocksData.blocks?.find(b => b.type === 'start');
            
            if (!startBlock) {
                console.error(`  ❌ No start block found for automation ${autoId}`);
                continue;
            }

            // Configure trigger: tag added
            const triggerTagId = tagIds[automation.triggerTag];
            if (triggerTagId) {
                await apiV3('PUT', `blocks/${startBlock.id}`, {
                    block: {
                        automation: String(autoId),
                        type: 'start',
                        config: JSON.stringify({
                            trigger: 'tag_add',
                            tag: String(triggerTagId)
                        })
                    }
                });
                console.log(`    → Trigger set: tag "${automation.triggerTag}" added`);
            }

            // Add email sequence with waits
            let lastBlockId = startBlock.id;
            let blockOrder = 2;

            for (const email of automation.emails) {
                // Add wait block
                const waitHours = email.waitBefore.hours || 1;
                const waitBlock = await apiV3('POST', 'blocks', {
                    block: {
                        automation: String(autoId),
                        type: 'wait',
                        parent: String(lastBlockId),
                        order: blockOrder++,
                        config: JSON.stringify({
                            wait_type: 'delay',
                            delay_amount: waitHours,
                            delay_unit: 'hours'
                        })
                    }
                });
                lastBlockId = waitBlock.block.id;
                console.log(`    → Wait ${waitHours}h block added (ID: ${lastBlockId})`);

                // Add send email block
                const sendBlock = await apiV3('POST', 'blocks', {
                    block: {
                        automation: String(autoId),
                        type: 'send',
                        parent: String(lastBlockId),
                        order: blockOrder++,
                        config: JSON.stringify({
                            subject: email.subject,
                            preheader: email.preheader,
                            name: email.name
                        })
                    }
                });
                lastBlockId = sendBlock.block.id;
                console.log(`    → Email "${email.name}" block added (ID: ${lastBlockId})`);
            }

            // Add final tag (recovery completed)
            await apiV3('POST', 'blocks', {
                block: {
                    automation: String(autoId),
                    type: 'action',
                    parent: String(lastBlockId),
                    order: blockOrder++,
                    config: JSON.stringify({
                        action: 'tag_add',
                        tag: tagIds['zapspy-recovery-completed'] || ''
                    })
                }
            });
            console.log(`    → Final tag "zapspy-recovery-completed" block added`);

            console.log(`  🎉 Automation "${automation.name}" fully configured!`);
            console.log('');

        } catch (err) {
            console.error(`  ❌ Automation "${automation.name}" failed: ${err.message}`);
        }
    }

    // 4. Summary
    console.log('');
    console.log('==========================================');
    console.log('🎉 SETUP COMPLETE!');
    console.log('==========================================');
    console.log('');
    console.log('Tags created:', Object.keys(tagIds).length);
    console.log('Lists created:', Object.keys(listIds).length);
    console.log('Automations created:', AUTOMATIONS_TO_CREATE.length);
    console.log('');
    console.log('⚠️  IMPORTANT NEXT STEPS:');
    console.log('1. Go to ActiveCampaign > Automations');
    console.log('2. Open each automation and add the HTML email templates');
    console.log('   (templates are in /backend/email-templates/)');
    console.log('3. Configure the sender email address');
    console.log('4. Activate each automation (they are created as INACTIVE)');
    console.log('5. Add these env vars to your server:');
    console.log(`   AC_API_URL=${AC_API_URL}`);
    console.log('   AC_API_KEY=your_api_key');
    console.log('');
}

setup().catch(err => {
    console.error('❌ Setup failed:', err);
    process.exit(1);
});
