const express = require('express');
const router = express.Router();
const pool = require('../database');
const { authenticateToken, requireAdmin, invalidateCache } = require('../middleware');
const { sendToFacebookCAPI, hashData, normalizePhone, normalizeGender, sendMissingCAPIPurchases } = require('../services/facebook-capi');
const { parseMonetizzeDate } = require('../helpers');
const { ZAPI_BASE_URL, ZAPI_CLIENT_TOKEN } = require('../config');

// ==================== MONETIZZE POSTBACK API ====================

const recentPostbacks = [];

const recentPerfectPayWebhooks = [];

// Simple test endpoint for postback (no DB)
router.post('/api/postback/test', (req, res) => {
    console.log('🧪 Test postback received:', req.body);
    res.json({ 
        status: 'ok', 
        received: req.body,
        keys: Object.keys(req.body || {})
    });
});

// Monetizze postback endpoint (public - no auth, uses token validation)
// Also accepts GET for testing
router.all('/api/postback/monetizze', async (req, res) => {
    // Handle GET request for testing
    if (req.method === 'GET') {
        return res.json({ 
            status: 'ok', 
            message: 'Postback endpoint is working! Use POST to send transaction data.',
            timestamp: new Date().toISOString()
        });
    }
    
    try {
        const rawBody = req.body || {};
        
        console.log('📥 Monetizze Postback received');
        console.log('📥 Content-Type:', req.headers['content-type']);
        console.log('📥 Body keys:', Object.keys(rawBody));
        console.log('📥 Raw body:', JSON.stringify(rawBody).substring(0, 1000));
        
        // ==================== HANDLE FLAT DOT-NOTATION KEYS ====================
        // Monetizze may send data as flat keys with dot notation:
        //   "produto.nome" = "X AI Monitor"
        //   "comprador.email" = "test@email.com"
        // We need to convert these to nested objects
        function unflattenObject(obj) {
            const result = {};
            for (const key of Object.keys(obj)) {
                if (key.includes('.')) {
                    const parts = key.split('.');
                    let current = result;
                    for (let i = 0; i < parts.length - 1; i++) {
                        if (!current[parts[i]] || typeof current[parts[i]] !== 'object') {
                            current[parts[i]] = {};
                        }
                        current = current[parts[i]];
                    }
                    current[parts[parts.length - 1]] = obj[key];
                } else {
                    // Don't overwrite nested objects already built
                    if (result[key] === undefined) {
                        result[key] = obj[key];
                    }
                }
            }
            // Merge non-dot keys that weren't set
            for (const key of Object.keys(obj)) {
                if (!key.includes('.') && result[key] === undefined) {
                    result[key] = obj[key];
                }
            }
            return result;
        }
        
        // Check if body has dot-notation keys (flat format from Monetizze)
        const hasDotKeys = Object.keys(rawBody).some(k => k.includes('.'));
        const body = hasDotKeys ? { ...unflattenObject(rawBody), ...rawBody } : rawBody;
        
        if (hasDotKeys) {
            console.log('📥 Detected flat dot-notation format, converted to nested:', JSON.stringify(body).substring(0, 500));
        }
        
        // Store postback for debugging (also persist to DB)
        try {
            const postbackEntry = {
                timestamp: new Date().toISOString(),
                method: req.method,
                contentType: req.headers['content-type'],
                body: body,
                rawBody: rawBody,
                bodyKeys: Object.keys(rawBody),
                hasDotKeys
            };
            recentPostbacks.unshift(postbackEntry);
            if (recentPostbacks.length > 50) recentPostbacks.pop();
            
            // Persist to database for debugging (non-blocking)
            pool.query(`
                INSERT INTO postback_logs (content_type, body, created_at) 
                VALUES ($1, $2, NOW())
            `, [req.headers['content-type'], JSON.stringify(rawBody)]).catch(err => {
                console.log('Postback log DB error (non-blocking):', err.message);
            });
        } catch (debugErr) {
            console.log('Debug storage error:', debugErr.message);
        }
        
        // Monetizze sends nested objects: produto, venda, comprador, tipoEvento, tipoPostback
        // Extract nested objects safely - handle both nested and flat formats
        const venda = (body.venda && typeof body.venda === 'object') ? body.venda : {};
        const comprador = (body.comprador && typeof body.comprador === 'object') ? body.comprador : {};
        const produto = (body.produto && typeof body.produto === 'object') ? body.produto : {};
        const tipoEvento = (body.tipoEvento && typeof body.tipoEvento === 'object') ? body.tipoEvento : {};
        
        // Transaction ID from chave_unica or venda.codigo - with dot-notation fallbacks
        const chave_unica = body.chave_unica || venda.codigo || body['venda.codigo'] || body['venda[codigo]'] || ('auto_' + Date.now());
        
        // Status code from tipoEvento.codigo (numeric) - this is the actual status
        // tipoEvento.codigo: 1=Aguardando, 2=Aprovada, 3=Cancelada, 4=Reembolso, etc.
        const statusCode = tipoEvento.codigo || body['tipoEvento.codigo'] || body['tipoEvento[codigo]'] || body.status || '2';
        
        // Value - prioritize commission value (valor líquido/comissão) over gross value
        // Monetizze fields: 
        //   venda.comissao or comissao - comissão líquida do produtor/afiliado
        //   venda.valorLiquido - valor líquido após taxas
        //   venda.valorRecebido - valor que será recebido
        //   venda.valor - valor bruto da venda
        const comissao = body.comissao || venda.comissao || body['venda.comissao'] || body['venda[comissao]'] || null;
        const valorLiquido = venda.valorLiquido || body['venda.valorLiquido'] || body['venda[valorLiquido]'] || null;
        const valorRecebido = venda.valorRecebido || body['venda.valorRecebido'] || body['venda[valorRecebido]'] || null;
        const valorBruto = venda.valor || body.valor || body['venda.valor'] || body['venda[valor]'] || '0';
        
        // Use GROSS sale value (valorBruto) for CAPI reporting (Facebook needs the total product price for ROAS)
        // Previously used commission, which underreported revenue to Facebook
        const valor = valorBruto || valorRecebido || valorLiquido || comissao;
        
        console.log('💰 Value breakdown:', { 
            comissao: comissao || 'N/A', 
            valorLiquido: valorLiquido || 'N/A', 
            valorRecebido: valorRecebido || 'N/A', 
            valorBruto,
            finalValue: valor 
        });
        
        // Buyer info from comprador object - with multiple fallbacks for different formats
        const email = comprador.email || body.email || body['comprador.email'] || body['comprador[email]'] || null;
        const telefone = comprador.telefone || body.telefone || body['comprador.telefone'] || body['comprador[telefone]'] || null;
        const nome = comprador.nome || body.nome || body['comprador.nome'] || body['comprador[nome]'] || null;
        
        // Product name from produto.nome - with fallbacks
        const productNameRaw = produto.nome || body['produto.nome'] || body['produto[nome]'] || body.produto_nome || 'Unknown Product';
        const productCode = produto.codigo || body['produto.codigo'] || body['produto[codigo]'] || body.produto_codigo || null;
        
        // Funnel language from venda.idioma
        const idioma = venda.idioma || body['venda.idioma'] || body['venda[idioma]'] || 'en';
        
        // Sale date - prioritize dataInicio (Data Pedido) for consistent ordering with Monetizze UI
        const dataInicioRaw = venda.dataInicio || body['venda.dataInicio'] || body['venda[dataInicio]'] || null;
        const dataFinalizadaRaw = venda.dataFinalizada || body['venda.dataFinalizada'] || body['venda[dataFinalizada]'] || null;
        const dataVenda = dataInicioRaw || dataFinalizadaRaw || venda.dataVenda || body['venda.dataVenda'] || body['venda[dataVenda]'] || 
                          venda.data || body.data || body['venda.data'] || null;
        
        console.log('📥 Extracted:', { 
            chave_unica, 
            statusCode, 
            valor, 
            email: email || 'none', 
            nome: nome || 'none',
            productNameRaw,
            productCode,
            idioma
        });
        
        // Validate postback (optional: check chave_unica against your secret)
        const postbackToken = process.env.MONETIZZE_POSTBACK_TOKEN;
        if (postbackToken && chave_unica !== postbackToken) {
            // Log but still process (Monetizze sends transaction ID as chave_unica)
            console.log('⚠️ Postback token check skipped (chave_unica is transaction ID)');
        }
        
        // Map Monetizze tipoEvento.codigo to our status
        // Based on official docs: https://apidoc.monetizze.com.br/postback/index.html
        // tipoEvento.codigo:
        // 1 = Aguardando pagamento
        // 2 = Finalizada / Aprovada ✅
        // 3 = Cancelada
        // 4 = Devolvida (Reembolso)
        // 5 = Bloqueada
        // 6 = Completa ✅
        // 7 = Abandono de Checkout
        // 101 = Assinatura - Ativa
        // 102 = Assinatura - Inadimplente
        // 103 = Assinatura - Cancelada
        // 104 = Assinatura - Aguardando pagamento
        // 105 = Recuperação Parcelada - Ativa
        // 106 = Recuperação Parcelada - Cancelada
        
        const statusMap = {
            '1': 'pending_payment',
            '2': 'approved',        // Finalizada / Aprovada
            '3': 'cancelled',
            '4': 'refunded',        // Reembolso solicitado pelo cliente
            '5': 'blocked',
            '6': 'approved',        // Completa (also counts as approved)
            '7': 'abandoned_checkout',
            '8': 'chargeback',      // Chargeback (disputa de cartão)
            '9': 'chargeback',      // Chargeback alternativo
            '70': 'tickets',
            '101': 'subscription_active',
            '102': 'subscription_overdue',
            '103': 'subscription_cancelled',
            '104': 'subscription_pending',
            '105': 'recovery_active',
            '106': 'recovery_cancelled',
            '120': 'shipping_update'
        };
        
        // Check for chargeback in description or other fields
        const eventoDescricao = (tipoEvento.descricao || '').toLowerCase();
        let finalStatus = statusMap[String(statusCode)] || 'unknown';
        
        // Detect chargeback from description if not caught by code
        if (eventoDescricao.includes('chargeback') || eventoDescricao.includes('disputa') || eventoDescricao.includes('contestação')) {
            finalStatus = 'chargeback';
        }
        
        // IMPORTANT: Check if sale is actually finalized
        // Monetizze can send status='2' (Finalizada) but without valid dataFinalizada
        // Use dataFinalizadaRaw (which has flat-format fallbacks) instead of venda.dataFinalizada
        const dataFinalizada = dataFinalizadaRaw || '';
        const isFinalized = dataFinalizada && 
                           dataFinalizada !== '0000-00-00 00:00:00' && 
                           dataFinalizada !== '0000-00-00' &&
                           !dataFinalizada.startsWith('0000-00-00');
        
        const statusStr = String(statusCode);
        console.log('🔍 Purchase check:', { statusStr, isFinalized, dataFinalizada: dataFinalizada || '(empty)', dataFinalizadaRaw: dataFinalizadaRaw || '(null)' });
        console.log('🔍 CAPI Decision:', { 
            willSendPurchase: statusStr === '2' || statusStr === '6',
            willSendInitiateCheckout: statusStr === '1' || statusStr === '7',
            statusCode, statusStr, 
            tipoEventoCodigo: tipoEvento.codigo || 'MISSING',
            tipoEventoDesc: tipoEvento.descricao || 'MISSING',
            bodyStatus: body.status || 'MISSING',
            vendaStatus: venda.status || 'MISSING'
        });
        
        // Check venda.status text for additional status info
        // IMPORTANT: NEVER override refunded/chargeback status with approved!
        const vendaStatus = (venda.status || '').toLowerCase();
        const isAlreadyRefundOrChargeback = (finalStatus === 'refunded' || finalStatus === 'chargeback');
        
        if (!isAlreadyRefundOrChargeback) {
            // Only apply venda.status text overrides if status is NOT already refund/chargeback
            if (vendaStatus.includes('cancelada') || vendaStatus.includes('cancel')) {
                finalStatus = 'cancelled';
            } else if (vendaStatus.includes('aguardando') || vendaStatus.includes('pending')) {
                finalStatus = 'pending_payment';
            } else if (vendaStatus.includes('finalizada') || vendaStatus.includes('aprovada')) {
                // If Monetizze says "Finalizada"/"Aprovada" in text, trust it as approved
                // Don't downgrade to pending_payment just because dataFinalizada is missing
                finalStatus = 'approved';
                if (!isFinalized) {
                    console.log(`⚠️ POSTBACK: vendaStatus says "${venda.status}" but dataFinalizada invalid - trusting vendaStatus as approved`);
                }
            } else if (statusStr === '2' || statusStr === '6') {
                // Status code 2/6 = Monetizze confirmed payment - always mark as approved
                finalStatus = 'approved';
                if (!isFinalized) {
                    console.log(`⚠️ POSTBACK: statusCode=${statusStr} (approved) but dataFinalizada invalid - trusting statusCode`);
                }
            }
        } else {
            console.log(`🔒 POSTBACK: Preserving ${finalStatus} status (not overriding with vendaStatus="${venda.status}")`);
        }
        
        // Also check venda.status text for refund/chargeback keywords (may catch additional cases)
        if (vendaStatus.includes('chargeback') || vendaStatus.includes('disputa') || vendaStatus.includes('contestação') || vendaStatus.includes('contestacao')) {
            finalStatus = 'chargeback';
        } else if (vendaStatus.includes('devolvida') || vendaStatus.includes('reembolso') || vendaStatus.includes('reembolsada') || vendaStatus.includes('refund')) {
            finalStatus = 'refunded';
        }
        
        const mappedStatus = finalStatus;
        const buyerEmail = email;
        const buyerPhone = telefone;
        const buyerName = nome;
        const productName = productNameRaw;
        const transactionValue = valor;
        
        console.log('📥 Final values:', { mappedStatus, buyerEmail: buyerEmail || 'none', productName, productCode: productCode || 'none', transactionValue });
        
        // Determine funnel language
        // Monetizze sends venda.idioma ('es', 'pt', 'en' etc)
        // Also check product codes and names as fallback
        // Spanish: Main (349260-349267) + Affiliates (338375, 341452-341454)
        const spanishProductCodes = ['349260', '349261', '349266', '349267', '338375', '341452', '341453', '341454'];
        const spanishProductKeywords = ['Infidelidad', 'Recuperación', 'Visión Total', 'VIP Sin Esperas'];
        // English: Main (341972, 349241-349243) + Affiliates (330254, 341443-341448)
        const englishProductCodes = ['341972', '349241', '349242', '349243', '330254', '341443', '341444', '341448'];
        
        let funnelLanguage = 'en'; // default to English
        
        // First check idioma field from Monetizze
        if (idioma === 'es') {
            funnelLanguage = 'es';
        } else if (productCode && spanishProductCodes.includes(String(productCode))) {
            funnelLanguage = 'es';
        } else if (productName && spanishProductKeywords.some(kw => productName.includes(kw))) {
            funnelLanguage = 'es';
        }
        
        // Identify product type (front/upsell1/upsell2/upsell3)
        let productType = 'front';
        const productNameLower = (productName || '').toLowerCase();
        const productCodeStr = String(productCode || '');
        
        // English products - Main: 341972=Front, 349241=UP1, 349242=UP2, 349243=UP3
        //                  - Affiliates: 330254=Front, 341443=UP1, 341444=UP2, 341448=UP3
        if (productNameLower.includes('message vault') || ['349241', '341443'].includes(productCodeStr)) {
            productType = 'upsell1';
        } else if (productNameLower.includes('360') || productNameLower.includes('tracker') || ['349242', '341444'].includes(productCodeStr)) {
            productType = 'upsell2';
        } else if (productNameLower.includes('instant access') || ['349243', '341448'].includes(productCodeStr)) {
            productType = 'upsell3';
        }
        // Spanish products - Main: 349260=Front, 349261=UP1, 349266=UP2, 349267=UP3
        //                  - Affiliates: 338375=Front, 341452=UP1, 341453=UP2, 341454=UP3
        else if (productNameLower.includes('recuperación total') || ['349261', '341452'].includes(productCodeStr)) {
            productType = 'upsell1';
        } else if (productNameLower.includes('visión total') || ['349266', '341453'].includes(productCodeStr)) {
            productType = 'upsell2';
        } else if (productNameLower.includes('sin esperas') || ['349267', '341454'].includes(productCodeStr)) {
            productType = 'upsell3';
        }
        // Front products (if none of the upsells matched)
        // English Main: X AI Monitor (341972), Affiliates: (330254)
        // Spanish Main: Detector de Infidelidad (349260), Affiliates: (338375)
        
        // Determine funnel source (main vs affiliate)
        const affiliateProductCodes = [
            '330254', '341443', '341444', '341448',  // English Affiliates
            '338375', '341452', '341453', '341454'   // Spanish Affiliates
        ];
        const funnelSource = (productCode && affiliateProductCodes.includes(String(productCode))) ? 'affiliate' : 'main';
        
        console.log(`🌐 Funnel language: ${funnelLanguage}, source: ${funnelSource} (product: ${productName}, code: ${productCode}, type: ${productType})`);
        
        // Generate a transaction ID if none provided
        const transactionId = chave_unica || `monetizze_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        
        // If no email found via standard extraction, try deep scan
        let finalEmail = buyerEmail;
        if (!finalEmail) {
            console.log('⚠️ No buyer email found via standard extraction');
            console.log('⚠️ Available body keys:', Object.keys(rawBody));
            console.log('⚠️ comprador object:', JSON.stringify(comprador));
            console.log('⚠️ Flat email keys:', Object.keys(rawBody).filter(k => k.toLowerCase().includes('email')));
            
            // Deep scan: search all values recursively for an email
            const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/;
            function findEmailInObj(obj) {
                for (const [key, val] of Object.entries(obj)) {
                    if (typeof val === 'string' && emailRegex.test(val)) {
                        return val.match(emailRegex)[0];
                    }
                    if (typeof val === 'object' && val !== null) {
                        const found = findEmailInObj(val);
                        if (found) return found;
                    }
                }
                return null;
            }
            
            finalEmail = findEmailInObj(rawBody);
            if (finalEmail) {
                console.log(`🔍 Found email via deep scan: ${finalEmail}`);
            } else {
                console.log('❌ No email found anywhere in postback data');
                // Log to DB for debugging
                pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('ERROR_NO_EMAIL', $1, NOW())`, 
                    [JSON.stringify({ rawKeys: Object.keys(rawBody), rawBody: rawBody })]).catch(() => {});
                return res.status(200).json({ status: 'ok', message: 'No email found, skipped' });
            }
        }
        
        console.log(`💾 Saving transaction: ${transactionId} for ${finalEmail || buyerEmail}`);
        
        // Determine created_at: use real sale date if available, otherwise NOW()
        // Uses parseMonetizzeDate helper to handle both Brazilian (DD/MM/YYYY) and ISO formats
        const saleDate = parseMonetizzeDate(dataVenda);
        console.log(`📅 Sale date: ${saleDate ? saleDate.toISOString() : 'Using NOW()'} (raw: ${dataVenda || 'none'})`);
        
        // ==================== WHATSAPP FROM LEADS (OPTION C) ====================
        // Try to get the correct WhatsApp from leads table (captured in funnel)
        // This is more reliable than phone from Monetizze checkout (users often fill wrong DDI)
        let finalPhone = buyerPhone; // Default to Monetizze phone
        const emailForLeadLookup = finalEmail || buyerEmail;
        
        if (emailForLeadLookup) {
            try {
                const leadResult = await pool.query(
                    `SELECT whatsapp, name FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                    [emailForLeadLookup]
                );
                if (leadResult.rows.length > 0 && leadResult.rows[0].whatsapp) {
                    finalPhone = leadResult.rows[0].whatsapp;
                    console.log(`📱 Using WhatsApp from lead: ${finalPhone} (instead of Monetizze: ${buyerPhone || 'none'})`);
                } else {
                    console.log(`📱 No lead WhatsApp found for ${emailForLeadLookup}, using Monetizze phone: ${buyerPhone || 'none'}`);
                }
            } catch (leadErr) {
                console.log(`⚠️ Error looking up lead WhatsApp: ${leadErr.message}`);
            }
        }
        
        // ==================== EXTRACT TRACKING PARAMS (before transaction save) ====================
        // Extract custom tracking params from postback (passed via checkout URL)
        // Monetizze may return these as venda.src, body.vid, UTM params, or flat fields
        const postbackVid = body.vid || venda.vid || body['venda.vid'] || body['venda[vid]'] || 
                           body.zs_vid || venda.zs_vid || null;
        let postbackFbc = body.zs_fbc || venda.zs_fbc || body['venda.zs_fbc'] || body['venda[zs_fbc]'] || null;
        const postbackFbp = body.zs_fbp || venda.zs_fbp || body['venda.zs_fbp'] || body['venda[zs_fbp]'] || null;
        
        // Build fbc from fbclid if zs_fbc not available
        if (!postbackFbc) {
            const postbackFbclid = body.fbclid || venda.fbclid || body['venda.fbclid'] || body['venda[fbclid]'] || null;
            if (postbackFbclid) {
                postbackFbc = `fb.1.${Date.now()}.${postbackFbclid}`;
            }
        }
        
        // Also try to extract vid from UTM fields (Monetizze sometimes maps custom params to UTM fields)
        const utmSource = body.utm_source || venda.utm_source || body['venda.utm_source'] || null;
        const utmContent = body.utm_content || venda.utm_content || body['venda.utm_content'] || null;
        // vid might be passed inside src field as a composite
        const srcField = venda.src || body.src || body['venda.src'] || body['venda[src]'] || '';
        const vidFromSrc = srcField.includes('vid=') ? new URLSearchParams(srcField.split('?').pop()).get('vid') : null;
        
        const resolvedVid = postbackVid || vidFromSrc || null;
        
        // Store transaction in database with funnel_language and funnel_source
        try {
            await pool.query(`
                INSERT INTO transactions (
                    transaction_id, email, phone, name, product, value, 
                    monetizze_status, status, raw_data, funnel_language, funnel_source, created_at,
                    fbc, fbp, visitor_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, COALESCE($12, NOW()), $13, $14, $15)
                ON CONFLICT (transaction_id) 
                DO UPDATE SET 
                    monetizze_status = $7,
                    status = $8,
                    raw_data = $9,
                    funnel_language = $10,
                    funnel_source = $11,
                    phone = COALESCE($3, transactions.phone),
                    fbc = COALESCE($13, transactions.fbc),
                    fbp = COALESCE($14, transactions.fbp),
                    visitor_id = COALESCE($15, transactions.visitor_id),
                    updated_at = NOW()
            `, [
                transactionId,
                finalEmail || buyerEmail,
                finalPhone,  // Use WhatsApp from lead if available
                buyerName,
                productName,
                transactionValue,
                String(statusCode),
                mappedStatus,
                JSON.stringify(req.body),
                funnelLanguage,
                funnelSource,
                saleDate,
                postbackFbc || null,
                postbackFbp || null,
                resolvedVid || null
            ]);
            console.log(`✅ Transaction saved: ${transactionId}`);
        } catch (dbError) {
            console.error(`❌ DB ERROR saving transaction: ${dbError.message}`);
            console.error(`❌ DB ERROR details:`, { transactionId, email: finalEmail || buyerEmail, product: productName, value: transactionValue });
            // Log error to postback_logs
            pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('DB_ERROR', $1, NOW())`, 
                [JSON.stringify({ error: dbError.message, transactionId, email: finalEmail || buyerEmail, rawBody })]).catch(() => {});
            throw dbError; // Re-throw to be caught by outer catch
        }
        
        // Try to match with existing lead and update status + products
        const emailForLead = finalEmail || buyerEmail;
        if (emailForLead) {
            const purchaseValue = parseFloat(transactionValue) || 0;
            
            // Build product identifier (type + truncated name)
            const productIdentifier = `${productType}:${(productName || '').substring(0, 50)}`;
            
            const leadUpdate = await pool.query(`
                UPDATE leads 
                SET status = CASE 
                    WHEN $1 = 'approved' THEN 'converted'
                    WHEN $1 IN ('cancelled', 'refunded', 'chargeback') AND status != 'converted' THEN 'lost'
                    WHEN $1 = 'pending_payment' AND status NOT IN ('converted', 'lost') THEN 'contacted'
                    ELSE status
                END,
                notes = COALESCE(notes, '') || E'\n[Monetizze] ' || $2 || ' - ' || NOW()::text,
                products_purchased = CASE 
                    WHEN $1 = 'approved' THEN 
                        CASE 
                            WHEN products_purchased IS NULL THEN ARRAY[$4]::TEXT[]
                            WHEN NOT ($4 = ANY(products_purchased)) THEN array_append(products_purchased, $4)
                            ELSE products_purchased
                        END
                    ELSE products_purchased
                END,
                total_spent = CASE 
                    WHEN $1 = 'approved' THEN COALESCE(total_spent, 0) + $5
                    WHEN $1 IN ('refunded', 'chargeback') THEN GREATEST(COALESCE(total_spent, 0) - $5, 0)
                    ELSE total_spent
                END,
                first_purchase_at = CASE 
                    WHEN $1 = 'approved' AND first_purchase_at IS NULL THEN NOW()
                    ELSE first_purchase_at
                END,
                last_purchase_at = CASE 
                    WHEN $1 = 'approved' THEN NOW()
                    ELSE last_purchase_at
                END,
                updated_at = NOW()
                WHERE LOWER(email) = LOWER($3)
                RETURNING id, email, status, products_purchased, total_spent
            `, [mappedStatus, mappedStatus, emailForLead, productIdentifier, purchaseValue]);
            
            if (leadUpdate.rows.length > 0) {
                const lead = leadUpdate.rows[0];
                console.log(`✅ Lead updated: ${emailForLead} -> ${mappedStatus} | Products: ${lead.products_purchased?.join(', ') || 'none'} | Total: R$${lead.total_spent}`);
            } else {
                console.log(`⚠️ No matching lead found for: ${emailForLead}`);
                // Auto-create lead from postback if approved (customer bought without going through lead capture)
                if (mappedStatus === 'approved') {
                    try {
                        const newLead = await pool.query(`
                            INSERT INTO leads (email, name, whatsapp, status, funnel_language, funnel_source, 
                                products_purchased, total_spent, first_purchase_at, last_purchase_at, 
                                created_at, updated_at)
                            VALUES (LOWER($1), $2, $3, 'converted', $4, $5, 
                                ARRAY[$6]::TEXT[], $7, NOW(), NOW(), NOW(), NOW())
                            RETURNING id, email
                        `, [emailForLead, buyerName || '', finalPhone || '', funnelLanguage, funnelSource, 
                            productIdentifier, purchaseValue]);
                        console.log(`✅ New lead auto-created from Monetizze postback: ${emailForLead} (id: ${newLead.rows[0]?.id})`);
                    } catch (insertErr) {
                        console.error(`⚠️ Error auto-creating lead from Monetizze postback: ${insertErr.message}`);
                    }
                }
            }
        }
        
        // ==================== FACEBOOK CONVERSIONS API EVENTS ====================
        
        if (resolvedVid || postbackFbc || postbackFbp) {
            console.log(`📊 CAPI: Custom tracking params from postback: vid=${resolvedVid || 'none'}, fbc=${postbackFbc ? 'Yes' : 'No'}, fbp=${postbackFbp ? 'Yes' : 'No'}`);
        }
        
        // Try to get enriched data from the lead record (IP, userAgent, fbc, fbp, country, city)
        // Quality score depends heavily on fbc, fbp, ip, user_agent - we need the lead to have them
        // ENHANCED: 5-level fallback matching for maximum attribution coverage
        let leadData = null;
        let matchMethod = 'none';
        const emailForCAPI = finalEmail || buyerEmail;
        
        try {
            // ===== LEVEL 1: Match by email in leads table =====
            if (emailForCAPI) {
                const leadResult = await pool.query(
                    `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id, funnel_language, referrer 
                     FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                    [emailForCAPI]
                );
                if (leadResult.rows.length > 0) {
                    leadData = leadResult.rows[0];
                    matchMethod = 'email';
                }
            }
            
            // ===== LEVEL 2: Match by phone in leads table =====
            if (!leadData && buyerPhone) {
                const digitsOnly = (s) => (s || '').replace(/\D/g, '');
                const buyerDigits = digitsOnly(buyerPhone);
                if (buyerDigits.length >= 10) {
                    const phoneResult = await pool.query(
                        `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id, funnel_language, referrer 
                         FROM leads 
                         WHERE REGEXP_REPLACE(COALESCE(whatsapp, ''), '\\D', '', 'g') = $1 
                            OR REGEXP_REPLACE(COALESCE(whatsapp, ''), '\\D', '', 'g') LIKE $2 
                         ORDER BY created_at DESC LIMIT 1`,
                        [buyerDigits, '%' + buyerDigits.slice(-10)]
                    );
                    if (phoneResult.rows.length > 0) {
                        leadData = phoneResult.rows[0];
                        matchMethod = 'phone';
                    }
                }
            }
            
            // ===== LEVEL 3: Match by visitor_id in leads table =====
            if (!leadData && resolvedVid) {
                const vidLeadResult = await pool.query(
                    `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id, funnel_language, referrer 
                     FROM leads WHERE visitor_id = $1 ORDER BY created_at DESC LIMIT 1`,
                    [resolvedVid]
                );
                if (vidLeadResult.rows.length > 0) {
                    leadData = vidLeadResult.rows[0];
                    matchMethod = 'visitor_id_lead';
                }
            }
            
            // ===== LEVEL 4: Match by visitor_id in funnel_events (get fbc/fbp from page visits) =====
            if (!leadData && resolvedVid) {
                const vidEventResult = await pool.query(
                    `SELECT ip_address, user_agent, fbc, fbp 
                     FROM funnel_events 
                     WHERE visitor_id = $1 AND (fbc IS NOT NULL OR fbp IS NOT NULL)
                     ORDER BY created_at DESC LIMIT 1`,
                    [resolvedVid]
                );
                if (vidEventResult.rows.length > 0) {
                    const eventRow = vidEventResult.rows[0];
                    leadData = {
                        ip_address: eventRow.ip_address,
                        user_agent: eventRow.user_agent,
                        fbc: eventRow.fbc,
                        fbp: eventRow.fbp,
                        country_code: null, city: null, state: null,
                        name: buyerName, target_gender: null,
                        whatsapp: buyerPhone, visitor_id: resolvedVid,
                        funnel_language: funnelLanguage, referrer: null
                    };
                    matchMethod = 'visitor_id_events';
                }
            }
            
            // ===== LEVEL 5: Match by IP address in funnel_events (last 48h) =====
            if (!leadData) {
                const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;
                // Only try IP matching if we have a real IP (not localhost)
                if (clientIp && clientIp !== '127.0.0.1' && clientIp !== '::1') {
                    // Try to find a recent funnel event from the same IP
                    const ipEventResult = await pool.query(
                        `SELECT ip_address, user_agent, fbc, fbp, visitor_id 
                         FROM funnel_events 
                         WHERE ip_address = $1 AND (fbc IS NOT NULL OR fbp IS NOT NULL)
                         AND created_at > NOW() - INTERVAL '48 hours'
                         ORDER BY created_at DESC LIMIT 1`,
                        [clientIp]
                    );
                    if (ipEventResult.rows.length > 0) {
                        const eventRow = ipEventResult.rows[0];
                        leadData = {
                            ip_address: eventRow.ip_address,
                            user_agent: eventRow.user_agent,
                            fbc: eventRow.fbc,
                            fbp: eventRow.fbp,
                            country_code: null, city: null, state: null,
                            name: buyerName, target_gender: null,
                            whatsapp: buyerPhone, visitor_id: eventRow.visitor_id,
                            funnel_language: funnelLanguage, referrer: null
                        };
                        matchMethod = 'ip_events';
                    }
                }
            }
            
            // ===== LEVEL 6: Use fbc/fbp from checkout URL params (passed via Monetizze) =====
            if (!leadData && (postbackFbc || postbackFbp)) {
                leadData = {
                    ip_address: null,
                    user_agent: null,
                    fbc: postbackFbc,
                    fbp: postbackFbp,
                    country_code: null, city: null, state: null,
                    name: buyerName, target_gender: null,
                    whatsapp: buyerPhone, visitor_id: resolvedVid,
                    funnel_language: funnelLanguage, referrer: null
                };
                matchMethod = 'postback_params';
            }
            
            // ===== ENRICHMENT: If lead found but missing fbc/fbp, try to fill from other sources =====
            if (leadData && (!leadData.fbc || !leadData.fbp)) {
                // Try postback params first
                if (!leadData.fbc && postbackFbc) leadData.fbc = postbackFbc;
                if (!leadData.fbp && postbackFbp) leadData.fbp = postbackFbp;
                
                // Try funnel_events by visitor_id
                if ((!leadData.fbc || !leadData.fbp) && leadData.visitor_id) {
                    const enrichResult = await pool.query(
                        `SELECT fbc, fbp FROM funnel_events 
                         WHERE visitor_id = $1 AND (fbc IS NOT NULL OR fbp IS NOT NULL)
                         ORDER BY created_at DESC LIMIT 1`,
                        [leadData.visitor_id]
                    );
                    if (enrichResult.rows.length > 0) {
                        if (!leadData.fbc && enrichResult.rows[0].fbc) leadData.fbc = enrichResult.rows[0].fbc;
                        if (!leadData.fbp && enrichResult.rows[0].fbp) leadData.fbp = enrichResult.rows[0].fbp;
                        console.log(`📊 CAPI: Enriched lead with fbc/fbp from funnel_events (visitor_id: ${leadData.visitor_id})`);
                    }
                }
            }
            
            if (leadData) {
                console.log(`📊 CAPI: Lead matched via [${matchMethod}] - IP=${leadData.ip_address ? 'Yes' : 'No'}, UA=${leadData.user_agent ? 'Yes' : 'No'}, fbc=${leadData.fbc ? 'Yes' : 'No'}, fbp=${leadData.fbp ? 'Yes' : 'No'}, Country=${leadData.country_code || 'No'}, Gender=${leadData.target_gender || 'No'}, VisitorID=${leadData.visitor_id || 'No'}`);
            } else {
                console.log(`📊 CAPI: No lead found after all 6 fallback levels. Purchase will be sent with minimal parameters.`);
            }
        } catch (leadErr) {
            console.error('⚠️ Error fetching lead data for CAPI:', leadErr.message);
        }
        
        // User data for Facebook CAPI - enriched with lead data (more params = better match quality)
        const fbUserData = {
            email: emailForCAPI,
            phone: leadData?.whatsapp || finalPhone || buyerPhone,  // Prefer WhatsApp from lead (has country code)
            firstName: leadData?.name || buyerName,
            // Add enriched data from lead record
            ip: leadData?.ip_address || null,
            userAgent: leadData?.user_agent || null,
            fbc: leadData?.fbc || null,
            fbp: leadData?.fbp || null,
            country: leadData?.country_code || null,
            city: leadData?.city || null,
            state: leadData?.state || null,  // State/province for better matching
            gender: leadData?.target_gender || null,  // Gender for better matching
            externalId: leadData?.visitor_id || null,  // Cross-device tracking
            referrer: leadData?.referrer || null  // HTTP referrer for attribution
        };
        
        // Check if this is a new or returning customer
        let customerSegmentation = 'new_customer_to_business';
        if (emailForCAPI) {
            try {
                const prevPurchases = await pool.query(
                    `SELECT COUNT(*) as count FROM transactions WHERE LOWER(email) = LOWER($1) AND status = 'approved'`,
                    [emailForCAPI]
                );
                if (parseInt(prevPurchases.rows[0]?.count || 0) > 0) {
                    customerSegmentation = 'existing_customer_to_business';
                }
            } catch (segErr) {
                // Non-blocking
            }
        }
        
        // Convert Monetizze value (BRL) to USD so pixel and Facebook see one currency (same as frontend)
        const brlToUsdRate = parseFloat(process.env.CONVERSION_BRL_TO_USD || '0.18');
        const valueBRL = parseFloat(transactionValue) || 0;
        const valueUSD = Math.round((valueBRL * brlToUsdRate) * 100) / 100;
        if (valueBRL > 0) console.log(`📤 CAPI: Converting value R$${valueBRL} → $${valueUSD} USD (rate ${brlToUsdRate})`);

        // Custom data for Facebook CAPI - enriched with all available data (USD = same as frontend pixel)
        const fbCustomData = {
            content_name: productName,
            content_ids: [productCode || chave_unica],
            content_type: 'product',
            content_category: productType || 'digital_product',  // Product category
            value: valueUSD,
            currency: 'USD',
            order_id: chave_unica,  // Transaction ID for tracking
            num_items: 1,  // Number of items
            customer_segmentation: customerSegmentation  // New vs returning customer
        };
        
        // Build event_source_url based on funnel language AND source
        // MUST match the domain where the pixel fires (frontend uses window.location.href)
        let eventSourceUrl;
        if (funnelSource === 'perfectpay') {
            eventSourceUrl = funnelLanguage === 'es' 
                ? 'https://perfect.zappdetect.com/espanhol/' 
                : 'https://perfect.zappdetect.com/ingles/';
        } else if (funnelSource === 'affiliate') {
            eventSourceUrl = funnelLanguage === 'es' 
                ? 'https://afiliado.whatstalker.com/espanhol/' 
                : 'https://afiliado.whatstalker.com/ingles/';
        } else {
            eventSourceUrl = funnelLanguage === 'es' 
                ? 'https://espanhol.zappdetect.com/' 
                : 'https://ingles.zappdetect.com/';
        }
        
        // Generate event_id for deduplication (transaction-based)
        // For Purchase: use SAME event_id for status 2 and 6 to prevent Facebook counting twice
        const eventId = `monetizze_${chave_unica}_${statusCode}`;
        const purchaseEventIdFixed = `monetizze_${chave_unica}_purchase`; // Status-agnostic for dedup
        
        try {
            // statusStr already defined above (before isFinalized check)
            
            // Options with language for correct pixel selection
            // Include actual sale time so Facebook gets the correct event_time
            const capiOptions = { language: funnelLanguage, eventTime: saleDate || null };
            
            // Status 7 and 1: InitiateCheckout is already sent by the frontend (Browser Pixel + CAPI)
            // with proper event_id deduplication. Sending it again here from the postback would create
            // DUPLICATE events in the Ads Manager because the event_ids are different.
            // The frontend sends: Browser Pixel + CAPI (same eventID = 1 event after dedup)
            // If we also send here: CAPI with different eventID = Facebook counts as a 2nd event
            if (statusStr === '7' || statusStr === '1') {
                console.log(`ℹ️ Status ${statusStr}: InitiateCheckout NOT sent via postback (already sent by frontend to avoid duplicates)`);
            }
            
            // Status 2 or 6 = Aprovada/Completa -> DELAYED Purchase event
            // IMPORTANT: We delay sending CAPI Purchase by 30 seconds to give the enrichPurchase
            // mechanism (from upsell page) time to provide fbc/fbp data.
            // Flow: Postback arrives → transaction saved → buyer redirected to upsell → 
            //       enrichPurchase captures _fbc/_fbp from browser → sends to backend →
            //       backend updates transaction/lead → THEN the delayed catch-up sends CAPI with fbc/fbp
            // If enrichPurchase arrives within 30s, it triggers an IMMEDIATE catch-up (faster than the delay)
            if (statusStr === '2' || statusStr === '6') {
                
                // DEDUP CHECK: Skip if we already sent a Purchase for this transaction
                let alreadySent = false;
                try {
                    const dupCheck = await pool.query(
                        `SELECT id FROM capi_purchase_logs WHERE transaction_id = $1 AND capi_success = true LIMIT 1`,
                        [chave_unica]
                    );
                    alreadySent = dupCheck.rows.length > 0;
                } catch (dupErr) {
                    // Non-blocking - if check fails, proceed with delayed send
                }
                
                if (alreadySent) {
                    console.log(`⚠️ CAPI: Purchase already sent for transaction ${chave_unica} (status ${statusStr}) - SKIPPING to avoid duplicate`);
                } else {
                    if (!isFinalized) {
                        console.log(`⚠️ Status ${statusStr} but dataFinalizada invalid (${dataFinalizada || 'empty'}) - will send Purchase via delayed catch-up`);
                    }
                    
                    // DON'T send CAPI immediately! Schedule a delayed catch-up instead.
                    // This gives enrichPurchase (from upsell page) time to provide fbc/fbp
                    // The enrichPurchase endpoint triggers an immediate catch-up when it enriches,
                    // so if enrichPurchase arrives within 30s, CAPI is sent even faster.
                    const delayedTxId = chave_unica;
                    const delayedEmail = emailForCAPI;
                    console.log(`⏳ CAPI: Scheduling delayed Purchase for ${delayedTxId} (${delayedEmail}) in 30s - waiting for enrichPurchase from upsell page...`);
                    console.log(`⏳ CAPI: Current lead data: fbc=${leadData?.fbc ? 'Yes' : 'No'}, fbp=${leadData?.fbp ? 'Yes' : 'No'}, match=${matchMethod}`);
                    
                    setTimeout(() => {
                        console.log(`⏰ CAPI: Delayed trigger for ${delayedTxId} (${delayedEmail}) - running catch-up now...`);
                        sendMissingCAPIPurchases().catch(err => console.error('Delayed CAPI catch-up error:', err.message));
                    }, 30000);
                }
            }
            
            // Status 3 = Cancelled -> Send custom Cancel event
            if (statusStr === '3' || finalStatus === 'cancelled') {
                console.log('📤 Transaction cancelled - no Facebook event sent');
            }
            
            // Status 4 = Refund -> Refund event (custom)
            if (statusStr === '4' || finalStatus === 'refunded') {
                console.log(`📤 Sending Refund to Facebook CAPI (${funnelLanguage})...`);
                await sendToFacebookCAPI('Refund', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_refund`, capiOptions);
            }
            
            // Status 8/9 = Chargeback -> Refund event (custom, same as refund for CAPI tracking)
            if (statusStr === '8' || statusStr === '9' || finalStatus === 'chargeback') {
                console.log(`📤 Sending Refund (chargeback) to Facebook CAPI (${funnelLanguage})...`);
                await sendToFacebookCAPI('Refund', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_chargeback`, capiOptions);
            }
            
        } catch (capiError) {
            console.error('CAPI error (non-blocking):', capiError.message);
        }
        
        // Register refund/chargeback in refund_requests table for consolidated tracking
        if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
            try {
                const refundProtocol = `MON-${chave_unica.substring(0, 12).toUpperCase()}`;
                const refundType = mappedStatus === 'chargeback' ? 'chargeback' : 'refund';
                
                // Check if already exists
                const existing = await pool.query(
                    'SELECT id FROM refund_requests WHERE transaction_id = $1',
                    [chave_unica]
                );
                
                if (existing.rows.length === 0) {
                    await pool.query(`
                        INSERT INTO refund_requests (
                            protocol, full_name, email, phone, product, reason, 
                            status, source, refund_type, transaction_id, value, funnel_language, created_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
                    `, [
                        refundProtocol,
                        buyerName || 'N/A',
                        finalEmail,
                        buyerPhone || null,
                        productName,
                        refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                        'approved', // Monetizze already processed it
                        'monetizze',
                        refundType,
                        chave_unica,
                        parseFloat(transactionValue) || 0,
                        funnelLanguage
                    ]);
                    
                    console.log(`📥 ${refundType.toUpperCase()} registered: ${refundProtocol} - ${finalEmail} - ${productName}`);
                }
            } catch (refundError) {
                console.error('Error registering refund:', refundError.message);
            }
        }
        
        // Summary log for easy tracking in Railway
        console.log(`📋 POSTBACK SUMMARY: tx=${chave_unica} | email=${finalEmail || 'none'} | status=${statusStr} (${mappedStatus}) | product=${productName} | value=R$${transactionValue} | lang=${funnelLanguage} | source=${funnelSource} | match=${matchMethod} | CAPI_Purchase=${statusStr === '2' || statusStr === '6' ? 'YES' : 'NO'} | CAPI_IC=${statusStr === '1' || statusStr === '7' ? 'YES' : 'NO'}`);
        
        // Return success (Monetizze expects 200 OK)
        res.status(200).send('OK');
        
    } catch (error) {
        console.error('❌ Postback CRITICAL error:', error.message);
        console.error('❌ Stack:', error.stack);
        console.error('❌ Raw body was:', JSON.stringify(req.body || {}).substring(0, 500));
        
        // Log error to DB for persistent debugging
        try {
            await pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('CRITICAL_ERROR', $1, NOW())`, 
                [JSON.stringify({ 
                    error: error.message, 
                    stack: error.stack?.substring(0, 500),
                    bodyKeys: Object.keys(req.body || {}),
                    rawBody: req.body || {},
                    contentType: req.headers['content-type']
                })]);
        } catch (logErr) {
            console.error('Failed to log error to DB:', logErr.message);
        }
        
        // Still return 200 to prevent Monetizze from retrying
        res.status(200).send('OK');
    }
});

// ==================== PERFECTPAY WEBHOOK API ====================
// Docs: https://support.perfectpay.com.br/doc/perfect-pay/postback/integracao-via-webhook-com-a-perfect-pay

router.all('/api/postback/perfectpay', async (req, res) => {
    // Handle GET request for testing
    if (req.method === 'GET') {
        return res.json({ 
            status: 'ok', 
            message: 'PerfectPay webhook endpoint is working! Use POST to send transaction data.',
            timestamp: new Date().toISOString()
        });
    }
    
    try {
        const body = req.body || {};
        
        console.log('📥 PerfectPay Webhook received');
        console.log('📥 Content-Type:', req.headers['content-type']);
        console.log('📥 Body keys:', Object.keys(body));
        console.log('📥 Raw body:', JSON.stringify(body).substring(0, 1000));
        
        // Store webhook for debugging (also persist to DB)
        try {
            const webhookEntry = {
                timestamp: new Date().toISOString(),
                method: req.method,
                contentType: req.headers['content-type'],
                body: body,
                bodyKeys: Object.keys(body)
            };
            recentPerfectPayWebhooks.unshift(webhookEntry);
            if (recentPerfectPayWebhooks.length > 50) recentPerfectPayWebhooks.pop();
            
            // Persist to database for debugging (non-blocking)
            pool.query(`
                INSERT INTO postback_logs (content_type, body, created_at) 
                VALUES ($1, $2, NOW())
            `, ['perfectpay_webhook', JSON.stringify(body)]).catch(err => {
                console.log('PerfectPay webhook log DB error (non-blocking):', err.message);
            });
        } catch (debugErr) {
            console.log('Debug storage error:', debugErr.message);
        }
        
        // ==================== EXTRACT PERFECTPAY FIELDS ====================
        // PerfectPay webhook JSON structure:
        // token, code, sale_amount, sale_status_enum, sale_status_detail
        // product: { code, name, external_reference, guarantee }
        // plan: { code, name, quantity }
        // customer: { full_name, email, phone_area_code, phone_number, country, state, city }
        // metadata: { src, utm_source, utm_medium, utm_campaign, utm_term, utm_content }
        // commission: [{ affiliation_code, affiliation_type_enum, name, email, commission_amount }]
        
        const webhookToken = body.token || null;
        const transactionCode = body.code || `pp_auto_${Date.now()}`;
        const saleAmount = body.sale_amount || 0;
        const statusEnum = String(body.sale_status_enum || '0');
        const statusDetail = body.sale_status_detail || '';
        const installments = body.installments || 1;
        const paymentType = body.payment_type_enum || 0;
        const dateCreated = body.date_created || null;
        const dateApproved = body.date_approved || null;
        
        // Product info
        const product = body.product || {};
        const productCode = product.code || null;
        const productName = product.name || 'Unknown Product';
        const productExternalRef = product.external_reference || null;
        
        // Plan info
        const plan = body.plan || {};
        const planCode = plan.code || null;
        const planName = plan.name || null;
        
        // Customer info
        const customer = body.customer || {};
        const buyerName = customer.full_name || null;
        const buyerEmail = customer.email || null;
        const buyerPhoneArea = customer.phone_area_code || '';
        const buyerPhoneNumber = customer.phone_number || '';
        const buyerPhone = buyerPhoneArea && buyerPhoneNumber 
            ? `+${buyerPhoneArea}${buyerPhoneNumber}` 
            : (buyerPhoneNumber || null);
        const buyerCountry = customer.country || null;
        const buyerState = customer.state || null;
        const buyerCity = customer.city || null;
        
        // Metadata (UTMs)
        const metadata = body.metadata || {};
        const utmSource = metadata.utm_source || null;
        const utmMedium = metadata.utm_medium || null;
        const utmCampaign = metadata.utm_campaign || null;
        const utmContent = metadata.utm_content || null;
        const utmTerm = metadata.utm_term || null;
        const metadataSrc = metadata.src || null;
        
        console.log('📥 PerfectPay Extracted:', { 
            transactionCode, 
            statusEnum, 
            statusDetail,
            saleAmount, 
            email: buyerEmail || 'none', 
            name: buyerName || 'none',
            productName,
            productCode,
            paymentType
        });
        
        // ==================== VALIDATE TOKEN ====================
        const perfectPayToken = process.env.PERFECTPAY_WEBHOOK_TOKEN;
        if (perfectPayToken && webhookToken && webhookToken !== perfectPayToken) {
            console.log('⚠️ PerfectPay token mismatch - processing anyway');
        }
        
        // ==================== MAP STATUS ====================
        // PerfectPay sale_status_enum:
        // 0 = none
        // 1 = pending (boleto pendente)
        // 2 = approved (venda aprovada)
        // 3 = in_process (revisão manual)
        // 4 = in_mediation (moderação)
        // 5 = rejected (rejeitado)
        // 6 = cancelled (cancelado)
        // 7 = refunded (devolvido)
        // 8 = authorized (autorizada)
        // 9 = charged_back (chargeback)
        // 10 = completed (30 dias após aprovação)
        // 11 = checkout_error
        // 12 = precheckout (abandono)
        // 13 = expired (boleto expirado)
        // 16 = in_review (em análise)
        
        const ppStatusMap = {
            '0': 'unknown',
            '1': 'pending_payment',
            '2': 'approved',
            '3': 'pending_payment',    // in_process -> treat as pending
            '4': 'pending_payment',    // in_mediation -> treat as pending
            '5': 'cancelled',          // rejected
            '6': 'cancelled',
            '7': 'refunded',
            '8': 'approved',           // authorized -> treat as approved
            '9': 'chargeback',
            '10': 'approved',          // completed -> treat as approved
            '11': 'cancelled',         // checkout_error
            '12': 'abandoned_checkout',// precheckout
            '13': 'cancelled',         // expired
            '16': 'pending_payment'    // in_review
        };
        
        // Also check sale_status_detail for additional info
        let mappedStatus = ppStatusMap[statusEnum] || 'unknown';
        const detailLower = (statusDetail || '').toLowerCase();
        
        if (detailLower.includes('chargeback') || detailLower.includes('charged_back')) {
            mappedStatus = 'chargeback';
        } else if (detailLower.includes('refund') || detailLower.includes('devolvid')) {
            mappedStatus = 'refunded';
        }
        
        console.log(`📥 PerfectPay Status: enum=${statusEnum} detail="${statusDetail}" -> mapped="${mappedStatus}"`);
        
        // ==================== DETERMINE FUNNEL LANGUAGE & SOURCE ====================
        // PerfectPay funnel is English by default (perfect.zappdetect.com/ingles)
        let funnelLanguage = 'en';
        let funnelSource = 'perfectpay';
        
        // Check UTM or product name for language hints
        const productNameLower = (productName || '').toLowerCase();
        if (productNameLower.includes('infidelidad') || productNameLower.includes('recuperación') || 
            utmCampaign === 'es' || utmSource === 'es') {
            funnelLanguage = 'es';
        }
        
        // Identify product type (front/upsell1/upsell2/upsell3)
        // PerfectPay English: PPPBE8FE=Front, PPPBE8FH=UP1, PPPBE8FI=UP2, PPPBE8FJ=UP3
        let productType = 'front';
        const ppCodeStr = String(productCode || '').toUpperCase();
        if (ppCodeStr === 'PPPBE8FH' || productNameLower.includes('message vault') || productNameLower.includes('recover')) {
            productType = 'upsell1';
        } else if (ppCodeStr === 'PPPBE8FI' || productNameLower.includes('360') || productNameLower.includes('tracker') || productNameLower.includes('social')) {
            productType = 'upsell2';
        } else if (ppCodeStr === 'PPPBE8FJ' || productNameLower.includes('instant') || productNameLower.includes('vip') || productNameLower.includes('priority')) {
            productType = 'upsell3';
        }
        
        console.log(`🌐 PerfectPay Funnel: lang=${funnelLanguage}, source=${funnelSource}, type=${productType}`);
        
        // ==================== RESOLVE EMAIL ====================
        if (!buyerEmail) {
            console.log('❌ PerfectPay: No buyer email found in webhook');
            pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('PP_ERROR_NO_EMAIL', $1, NOW())`, 
                [JSON.stringify(body)]).catch(() => {});
            return res.status(200).json({ status: 'ok', message: 'No email found, skipped' });
        }
        
        // ==================== RESOLVE PHONE FROM LEADS ====================
        let finalPhone = buyerPhone;
        try {
            const leadResult = await pool.query(
                `SELECT whatsapp, name FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                [buyerEmail]
            );
            if (leadResult.rows.length > 0 && leadResult.rows[0].whatsapp) {
                finalPhone = leadResult.rows[0].whatsapp;
                console.log(`📱 PerfectPay: Using WhatsApp from lead: ${finalPhone}`);
            }
        } catch (leadErr) {
            console.log(`⚠️ PerfectPay: Error looking up lead WhatsApp: ${leadErr.message}`);
        }
        
        // ==================== EXTRACT TRACKING PARAMS ====================
        const postbackVid = metadataSrc || null; // src field may contain visitor ID
        let postbackFbc = null;
        let postbackFbp = null;
        
        // Try to extract fbc/fbp from UTM fields (may be passed via checkout URL)
        if (utmContent) {
            // Sometimes fbc/fbp are encoded in utm_content
            const fbcMatch = utmContent.match(/fb\.1\.\d+\.\w+/);
            if (fbcMatch) postbackFbc = fbcMatch[0];
        }
        
        // ==================== PARSE DATES ====================
        // PerfectPay date format: "2019-04-10 18:50:56"
        let saleDate = null;
        try {
            if (dateCreated) {
                saleDate = new Date(dateCreated);
                if (isNaN(saleDate.getTime())) saleDate = null;
            }
        } catch (e) { saleDate = null; }
        
        // ==================== SAVE TRANSACTION ====================
        const transactionId = `PP_${transactionCode}`;
        
        console.log(`💾 PerfectPay: Saving transaction: ${transactionId} for ${buyerEmail}`);
        
        try {
            await pool.query(`
                INSERT INTO transactions (
                    transaction_id, email, phone, name, product, value, 
                    monetizze_status, status, raw_data, funnel_language, funnel_source, created_at,
                    fbc, fbp, visitor_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, COALESCE($12, NOW()), $13, $14, $15)
                ON CONFLICT (transaction_id) 
                DO UPDATE SET 
                    monetizze_status = $7,
                    status = $8,
                    raw_data = $9,
                    funnel_language = $10,
                    funnel_source = $11,
                    phone = COALESCE($3, transactions.phone),
                    fbc = COALESCE($13, transactions.fbc),
                    fbp = COALESCE($14, transactions.fbp),
                    visitor_id = COALESCE($15, transactions.visitor_id),
                    updated_at = NOW()
            `, [
                transactionId,
                buyerEmail,
                finalPhone,
                buyerName,
                productName,
                saleAmount,
                `pp_${statusEnum}`,  // Store original PerfectPay status with prefix
                mappedStatus,
                JSON.stringify(body),
                funnelLanguage,
                funnelSource,
                saleDate,
                postbackFbc || null,
                postbackFbp || null,
                postbackVid || null
            ]);
            console.log(`✅ PerfectPay: Transaction saved: ${transactionId}`);
        } catch (dbError) {
            console.error(`❌ PerfectPay DB ERROR: ${dbError.message}`);
            pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('PP_DB_ERROR', $1, NOW())`, 
                [JSON.stringify({ error: dbError.message, transactionId, email: buyerEmail, body })]).catch(() => {});
            throw dbError;
        }
        
        // ==================== UPDATE LEAD ====================
        if (buyerEmail) {
            const purchaseValue = parseFloat(saleAmount) || 0;
            const productIdentifier = `${productType}:${(productName || '').substring(0, 50)}`;
            
            try {
                const leadUpdate = await pool.query(`
                    UPDATE leads 
                    SET status = CASE 
                        WHEN $1 = 'approved' THEN 'converted'
                        WHEN $1 IN ('cancelled', 'refunded', 'chargeback') AND status != 'converted' THEN 'lost'
                        WHEN $1 = 'pending_payment' AND status NOT IN ('converted', 'lost') THEN 'contacted'
                        ELSE status
                    END,
                    notes = COALESCE(notes, '') || E'\n[PerfectPay] ' || $2 || ' - ' || NOW()::text,
                    products_purchased = CASE 
                        WHEN $1 = 'approved' THEN 
                            CASE 
                                WHEN products_purchased IS NULL THEN ARRAY[$4]::TEXT[]
                                WHEN NOT ($4 = ANY(products_purchased)) THEN array_append(products_purchased, $4)
                                ELSE products_purchased
                            END
                        ELSE products_purchased
                    END,
                    total_spent = CASE 
                        WHEN $1 = 'approved' THEN COALESCE(total_spent, 0) + $5
                        WHEN $1 IN ('refunded', 'chargeback') THEN GREATEST(COALESCE(total_spent, 0) - $5, 0)
                        ELSE total_spent
                    END,
                    first_purchase_at = CASE 
                        WHEN $1 = 'approved' AND first_purchase_at IS NULL THEN NOW()
                        ELSE first_purchase_at
                    END,
                    last_purchase_at = CASE 
                        WHEN $1 = 'approved' THEN NOW()
                        ELSE last_purchase_at
                    END,
                    updated_at = NOW()
                    WHERE LOWER(email) = LOWER($3)
                    RETURNING id, email, status, products_purchased, total_spent
                `, [mappedStatus, mappedStatus, buyerEmail, productIdentifier, purchaseValue]);
                
                if (leadUpdate.rows.length > 0) {
                    const lead = leadUpdate.rows[0];
                    console.log(`✅ PerfectPay Lead updated: ${buyerEmail} -> ${mappedStatus} | Products: ${lead.products_purchased?.join(', ') || 'none'} | Total: $${lead.total_spent}`);
                } else {
                    console.log(`⚠️ PerfectPay: No matching lead found for: ${buyerEmail}`);
                    // Auto-create lead from PerfectPay postback if approved
                    if (mappedStatus === 'approved') {
                        try {
                            const newLead = await pool.query(`
                                INSERT INTO leads (email, name, whatsapp, status, funnel_language, funnel_source,
                                    products_purchased, total_spent, first_purchase_at, last_purchase_at,
                                    created_at, updated_at)
                                VALUES (LOWER($1), $2, $3, 'converted', $4, $5,
                                    ARRAY[$6]::TEXT[], $7, NOW(), NOW(), NOW(), NOW())
                                RETURNING id, email
                            `, [buyerEmail, buyerName || '', finalPhone || '', funnelLanguage, funnelSource,
                                productIdentifier, purchaseValue]);
                            console.log(`✅ New lead auto-created from PerfectPay postback: ${buyerEmail} (id: ${newLead.rows[0]?.id})`);
                        } catch (insertErr) {
                            console.error(`⚠️ PerfectPay: Error auto-creating lead: ${insertErr.message}`);
                        }
                    }
                }
            } catch (leadErr) {
                console.error(`⚠️ PerfectPay: Error updating lead: ${leadErr.message}`);
            }
        }
        
        // ==================== FACEBOOK CAPI ====================
        try {
            // Try to get enriched data from the lead record
            let leadData = null;
            let matchMethod = 'none';
            
            if (buyerEmail) {
                const leadResult = await pool.query(
                    `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id, funnel_language, referrer 
                     FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                    [buyerEmail]
                );
                if (leadResult.rows.length > 0) {
                    leadData = leadResult.rows[0];
                    matchMethod = 'email';
                }
            }
            
            // Enrichment from funnel_events if needed
            if (leadData && (!leadData.fbc || !leadData.fbp) && leadData.visitor_id) {
                const enrichResult = await pool.query(
                    `SELECT fbc, fbp FROM funnel_events 
                     WHERE visitor_id = $1 AND (fbc IS NOT NULL OR fbp IS NOT NULL)
                     ORDER BY created_at DESC LIMIT 1`,
                    [leadData.visitor_id]
                );
                if (enrichResult.rows.length > 0) {
                    if (!leadData.fbc && enrichResult.rows[0].fbc) leadData.fbc = enrichResult.rows[0].fbc;
                    if (!leadData.fbp && enrichResult.rows[0].fbp) leadData.fbp = enrichResult.rows[0].fbp;
                }
            }
            
            if (leadData) {
                console.log(`📊 PerfectPay CAPI: Lead matched via [${matchMethod}] - fbc=${leadData.fbc ? 'Yes' : 'No'}, fbp=${leadData.fbp ? 'Yes' : 'No'}`);
            }
            
            const fbUserData = {
                email: buyerEmail,
                phone: leadData?.whatsapp || finalPhone || buyerPhone,
                firstName: leadData?.name || buyerName,
                ip: leadData?.ip_address || null,
                userAgent: leadData?.user_agent || null,
                fbc: leadData?.fbc || postbackFbc || null,
                fbp: leadData?.fbp || postbackFbp || null,
                country: leadData?.country_code || buyerCountry || null,
                city: leadData?.city || buyerCity || null,
                state: leadData?.state || buyerState || null,
                gender: leadData?.target_gender || null,
                externalId: leadData?.visitor_id || postbackVid || null
            };
            
            // Convert to USD (PerfectPay may send in USD already for international products)
            const valueRaw = parseFloat(saleAmount) || 0;
            // Check if currency is BRL (currency_enum 1) or assume USD for international
            const currencyEnum = body.currency_enum || 0;
            const isBRL = currencyEnum === 1;
            const brlToUsdRate = parseFloat(process.env.CONVERSION_BRL_TO_USD || '0.18');
            const valueUSD = isBRL ? Math.round((valueRaw * brlToUsdRate) * 100) / 100 : valueRaw;
            
            const fbCustomData = {
                content_name: productName,
                content_ids: [productCode || transactionCode],
                content_type: 'product',
                content_category: productType || 'digital_product',
                value: valueUSD,
                currency: 'USD',
                order_id: transactionId,
                num_items: 1
            };
            
            const eventSourceUrl = 'https://perfect.zappdetect.com/ingles/';
            const eventId = `perfectpay_${transactionCode}_${statusEnum}`;
            const capiOptions = { language: funnelLanguage, eventTime: saleDate || null };
            
            // Status 12 and 1: InitiateCheckout already sent by frontend (Browser Pixel + CAPI)
            // Sending again here would create DUPLICATE events in Ads Manager
            if (statusEnum === '12' || statusEnum === '1') {
                console.log(`ℹ️ PerfectPay Status ${statusEnum}: InitiateCheckout NOT sent via postback (already sent by frontend)`);
            }
            
            // Status 2, 8, 10 = approved/authorized/completed -> Purchase
            if (statusEnum === '2' || statusEnum === '8' || statusEnum === '10') {
                // Dedup check
                let alreadySent = false;
                try {
                    const dupCheck = await pool.query(
                        `SELECT id FROM capi_purchase_logs WHERE transaction_id = $1 AND capi_success = true LIMIT 1`,
                        [transactionId]
                    );
                    alreadySent = dupCheck.rows.length > 0;
                } catch (dupErr) {}
                
                if (alreadySent) {
                    console.log(`⚠️ PerfectPay CAPI: Purchase already sent for ${transactionId} - SKIPPING`);
                } else {
                    // Schedule delayed CAPI (same pattern as Monetizze - wait for enrichPurchase)
                    console.log(`⏳ PerfectPay CAPI: Scheduling delayed Purchase for ${transactionId} in 30s...`);
                    setTimeout(() => {
                        console.log(`⏰ PerfectPay CAPI: Delayed trigger for ${transactionId} - running catch-up...`);
                        sendMissingCAPIPurchases().catch(err => console.error('PerfectPay delayed CAPI error:', err.message));
                    }, 30000);
                }
            }
            
            // Status 7 = refunded
            if (statusEnum === '7' || mappedStatus === 'refunded') {
                console.log(`📤 PerfectPay: Sending Refund to Facebook CAPI...`);
                await sendToFacebookCAPI('Refund', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_refund`, capiOptions);
            }
            
            // Status 9 = chargeback
            if (statusEnum === '9' || mappedStatus === 'chargeback') {
                console.log(`📤 PerfectPay: Sending Refund (chargeback) to Facebook CAPI...`);
                await sendToFacebookCAPI('Refund', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_chargeback`, capiOptions);
            }
            
        } catch (capiError) {
            console.error('PerfectPay CAPI error (non-blocking):', capiError.message);
        }
        
        // ==================== REGISTER REFUND/CHARGEBACK ====================
        if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
            try {
                const refundProtocol = `PP-${transactionCode.substring(0, 12).toUpperCase()}`;
                const refundType = mappedStatus === 'chargeback' ? 'chargeback' : 'refund';
                
                const existing = await pool.query(
                    'SELECT id FROM refund_requests WHERE transaction_id = $1',
                    [transactionId]
                );
                
                if (existing.rows.length === 0) {
                    await pool.query(`
                        INSERT INTO refund_requests (
                            protocol, full_name, email, phone, product, reason, 
                            status, source, refund_type, transaction_id, value, funnel_language, created_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
                    `, [
                        refundProtocol,
                        buyerName || 'N/A',
                        buyerEmail,
                        buyerPhone || null,
                        productName,
                        refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via PerfectPay',
                        'approved',
                        'perfectpay',
                        refundType,
                        transactionId,
                        parseFloat(saleAmount) || 0,
                        funnelLanguage
                    ]);
                    console.log(`📥 PerfectPay ${refundType.toUpperCase()} registered: ${refundProtocol}`);
                }
            } catch (refundError) {
                console.error('PerfectPay refund registration error:', refundError.message);
            }
        }
        
        // Summary log
        console.log(`📋 PERFECTPAY SUMMARY: tx=${transactionId} | email=${buyerEmail || 'none'} | status=${statusEnum} (${mappedStatus}) | detail=${statusDetail} | product=${productName} | value=$${saleAmount} | lang=${funnelLanguage} | type=${productType}`);
        
        // Return success (PerfectPay expects 200 OK)
        res.status(200).json({ status: 'ok' });
        
    } catch (error) {
        console.error('❌ PerfectPay Webhook CRITICAL error:', error.message);
        console.error('❌ Stack:', error.stack);
        
        try {
            await pool.query(`INSERT INTO postback_logs (content_type, body, created_at) VALUES ('PP_CRITICAL_ERROR', $1, NOW())`, 
                [JSON.stringify({ 
                    error: error.message, 
                    stack: error.stack?.substring(0, 500),
                    body: req.body || {}
                })]);
        } catch (logErr) {
            console.error('Failed to log PerfectPay error to DB:', logErr.message);
        }
        
        // Still return 200 to prevent PerfectPay from retrying
        res.status(200).json({ status: 'ok' });
    }
});

// Admin: Debug PerfectPay webhooks
router.get('/api/admin/debug/perfectpay-webhooks', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const dbLogs = await pool.query(`
            SELECT id, content_type, body, created_at 
            FROM postback_logs 
            WHERE content_type LIKE 'perfectpay%' OR content_type LIKE 'PP_%'
            ORDER BY created_at DESC 
            LIMIT 30
        `);
        
        res.json({
            memoryCount: recentPerfectPayWebhooks.length,
            dbLogCount: dbLogs.rows.length,
            info: 'PerfectPay webhook debug. Configure webhook URL in PerfectPay dashboard.',
            webhookUrl: 'https://zapspy-funnel-production.up.railway.app/api/postback/perfectpay',
            alternateUrl: 'https://painel.xaimonitor.com/api/postback/perfectpay',
            recentWebhooks: recentPerfectPayWebhooks,
            dbLogs: dbLogs.rows
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

router.recentPostbacks = recentPostbacks;
router.recentPerfectPayWebhooks = recentPerfectPayWebhooks;

module.exports = router;
