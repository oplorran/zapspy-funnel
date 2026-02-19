const pool = require('../database');
const { VALID_PRODUCT_CODES } = require('../config');
const { parseMonetizzeDate } = require('../helpers');
const { sendToFacebookCAPI, sendMissingCAPIPurchases } = require('./facebook-capi');
const { backfillTransactionFbcFbp } = require('./facebook-capi');

// Helper: Authenticate with Monetizze API 2.1 (2-step auth)
// Step 1: GET /token with X_CONSUMER_KEY header → returns temporary token
// Step 2: Use TOKEN header for all subsequent requests
async function getMonetizzeToken() {
    const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
    if (!consumerKey) throw new Error('MONETIZZE_CONSUMER_KEY not configured');
    
    console.log('🔑 Authenticating with Monetizze API 2.1...');
    const response = await fetch('https://api.monetizze.com.br/2.1/token', {
        method: 'GET',
        headers: {
            'X_CONSUMER_KEY': consumerKey,
            'Accept': 'application/json'
        }
    });
    
    if (!response.ok) {
        const errorText = await response.text();
        console.error('❌ Monetizze auth failed:', response.status, errorText);
        throw new Error(`Auth failed (${response.status}): ${errorText}`);
    }
    
    const data = await response.json();
    console.log('✅ Monetizze token obtained successfully');
    return data.token;
}

// Test Monetizze API connectivity (debug helper - returns results object)
async function testMonetizzeApi() {
    const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
    const results = { tests: [], consumerKeyPresent: !!consumerKey, keyLength: consumerKey?.length };
    
    if (!consumerKey) {
        return { error: 'MONETIZZE_CONSUMER_KEY not configured', results };
    }
    
    // Step 1: Try to get auth token
    let monetizzeToken = null;
    try {
        console.log('Step 1: Getting auth token...');
        const authResponse = await fetch('https://api.monetizze.com.br/2.1/token', {
            method: 'GET',
            headers: {
                'X_CONSUMER_KEY': consumerKey,
                'Accept': 'application/json'
            }
        });
        const authText = await authResponse.text();
        results.tests.push({
            name: 'Step 1: GET /token (X_CONSUMER_KEY header)',
            status: authResponse.status,
            ok: authResponse.ok,
            body: authText.substring(0, 500)
        });
        
        if (authResponse.ok) {
            try {
                const authData = JSON.parse(authText);
                monetizzeToken = authData.token;
                results.tokenObtained = true;
                results.tokenPreview = monetizzeToken ? monetizzeToken.substring(0, 20) + '...' : 'null';
            } catch (e) {
                results.tokenObtained = false;
            }
        }
    } catch (err) {
        results.tests.push({ name: 'Step 1: GET /token', error: err.message });
    }
    
    // Step 2: If we got a token, try to query transactions
    if (monetizzeToken) {
        try {
            console.log('Step 2: Querying transactions with token...');
            const txResponse = await fetch('https://api.monetizze.com.br/2.1/transactions', {
                method: 'GET',
                headers: {
                    'TOKEN': monetizzeToken,
                    'Accept': 'application/json'
                }
            });
            const txText = await txResponse.text();
            results.tests.push({
                name: 'Step 2: GET /transactions (TOKEN header)',
                status: txResponse.status,
                ok: txResponse.ok,
                body: txText.substring(0, 1000)
            });
        } catch (err) {
            results.tests.push({ name: 'Step 2: GET /transactions', error: err.message });
        }
    }
    
    return results;
}

// ==================== MONETIZZE SYNC CORE FUNCTION ====================
// Reusable sync logic - called by manual endpoint AND auto-sync
async function syncMonetizzeSalesCore(startDate, endDate) {
    const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
    if (!consumerKey) {
        throw new Error('MONETIZZE_CONSUMER_KEY not configured');
    }
    
    console.log('🔄 Starting Monetizze sync...');
    console.log('📅 Date range:', startDate || 'today', 'to', endDate || 'today');
    
    // Step 1: Authenticate
    let monetizzeToken;
    try {
        monetizzeToken = await getMonetizzeToken();
    } catch (authError) {
        throw new Error(`Monetizze authentication failed: ${authError.message}`);
    }
    
    // Step 2: Query transactions
    const params = new URLSearchParams();
    if (startDate) params.append('date_min', `${startDate} 00:00:00`);
    if (endDate) params.append('date_max', `${endDate} 23:59:59`);
    // Monetizze API only supports status 1-6. Chargebacks (8/9) come via postback only.
    // 1=Pending, 2=Approved, 3=Cancelled, 4=Refunded/Devolvida, 5=Blocked, 6=Complete
    ['1','2','3','4','5','6'].forEach(s => params.append('status[]', s));
    
    const validProductCodes = [
        '341972', '349241', '349242', '349243',
        '330254', '341443', '341444', '341448',
        '349260', '349261', '349266', '349267',
        '338375', '341452', '341453', '341454'
    ];
    validProductCodes.forEach(code => params.append('product[]', code));
    
    const txUrl = `https://api.monetizze.com.br/2.1/transactions?${params.toString()}`;
    console.log('🌐 Fetching transactions from Monetizze API 2.1');
    
    const response = await fetch(txUrl, {
        method: 'GET',
        headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
    });
    
    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Monetizze API error ${response.status}: ${errorText.substring(0, 200)}`);
    }
    
    const data = await response.json();
    
    let salesArray = [];
    if (Array.isArray(data)) salesArray = data;
    else if (Array.isArray(data.dados)) salesArray = data.dados;
    else if (Array.isArray(data.vendas)) salesArray = data.vendas;
    else if (Array.isArray(data.recordset)) salesArray = data.recordset;
    
    // Log pagination info for debugging
    console.log(`📄 API response - pagina: ${data.pagina}, paginas: ${data.paginas}, registros: ${data.registros}, first page items: ${salesArray.length}`);
    
    // Handle pagination - keep fetching until no more results
    const totalPages = data.paginas || 0;
    const totalRecords = data.registros || 0;
    
    if (totalPages > 1) {
        console.log(`📄 Pagination: ${totalPages} pages, ${totalRecords} total records`);
        for (let page = 2; page <= Math.min(totalPages, 15); page++) { // Max 15 pages (1500 items) safety limit
            try {
                const pageParams = new URLSearchParams(params.toString());
                pageParams.set('pagina', String(page));
                const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                    method: 'GET',
                    headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                });
                if (pageResponse.ok) {
                    const pageData = await pageResponse.json();
                    const pageItems = pageData.dados || pageData.vendas || [];
                    salesArray = salesArray.concat(pageItems);
                    console.log(`📄 Page ${page}/${totalPages}: ${pageItems.length} items (total: ${salesArray.length})`);
                    if (pageItems.length === 0) break; // No more data
                }
            } catch (pageError) {
                console.error(`❌ Error fetching page ${page}:`, pageError.message);
            }
        }
    } else if (salesArray.length >= 100 && totalPages === 0) {
        // API might not return paginas field - try fetching more pages manually
        console.log(`⚠️ Got ${salesArray.length} items but no pagination info. Trying to fetch more pages...`);
        for (let page = 2; page <= 15; page++) { // Max 15 pages (1500 items)
            try {
                const pageParams = new URLSearchParams(params.toString());
                pageParams.set('pagina', String(page));
                const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                    method: 'GET',
                    headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                });
                if (pageResponse.ok) {
                    const pageData = await pageResponse.json();
                    const pageItems = pageData.dados || pageData.vendas || [];
                    if (pageItems.length === 0) {
                        console.log(`📄 Page ${page}: empty - pagination complete`);
                        break;
                    }
                    salesArray = salesArray.concat(pageItems);
                    console.log(`📄 Page ${page}: ${pageItems.length} items (total: ${salesArray.length})`);
                } else {
                    console.log(`📄 Page ${page}: HTTP ${pageResponse.status} - stopping pagination`);
                    break;
                }
            } catch (pageError) {
                console.error(`❌ Page ${page} error:`, pageError.message);
                break;
            }
        }
    }
    
    console.log(`📦 Total transactions fetched: ${salesArray.length}`);
    
    if (salesArray.length === 0) {
        return { synced: 0, skipped: 0, total: 0 };
    }
    
    let synced = 0;
    let skipped = 0;
    let errors = [];
    
    const spanishCodes = ['349260', '349261', '349266', '349267', '338375', '341452', '341453', '341454'];
    const affiliateCodes = ['330254', '341443', '341444', '341448', '338375', '341452', '341453', '341454'];
    const statusMap = {
        '1': 'pending_payment', '2': 'approved', '3': 'cancelled',
        '4': 'refunded', '5': 'blocked', '6': 'approved',
        '7': 'abandoned_checkout', '8': 'chargeback', '9': 'chargeback'
    };
    
    for (const item of salesArray) {
        try {
            const vendaData = item.venda || item;
            const produtoData = item.produto || {};
            const compradorData = item.comprador || {};
            const tipoEvento = item.tipoEvento || {};
            
            const transactionId = vendaData.codigo || item.codigo_venda || item.chave_unica;
            const email = compradorData.email || vendaData.email;
            const phone = compradorData.telefone || vendaData.telefone;
            const name = compradorData.nome || vendaData.nome;
            const productName = produtoData.nome || vendaData.produto_nome;
            const productCode = produtoData.codigo || vendaData.produto_codigo;
            const value = vendaData.valor || vendaData.valorRecebido || vendaData.comissao;
            const statusCode = String(tipoEvento.codigo || item.codigo_status || '2');
            
            if (productCode && !validProductCodes.includes(String(productCode))) {
                skipped++;
                continue;
            }
            
            // Priority: dataInicio (Data Pedido in Monetizze UI) for consistent ordering
            // dataFinalizada only exists for completed sales, so use dataInicio for all
            const saleDateStr = vendaData.dataInicio || vendaData.dataFinalizada || vendaData.dataVenda || vendaData.data || null;
            const saleDate = parseMonetizzeDate(saleDateStr);
            
            // Debug log for date parsing (remove after testing)
            if (synced < 5) {
                console.log(`📅 DEBUG DATE - Using: "${saleDateStr}" | Parsed: ${saleDate ? saleDate.toISOString() : 'null'} | dataFinalizada: "${vendaData.dataFinalizada}" | dataInicio: "${vendaData.dataInicio}"`);
            }
            
            const funnelLanguage = spanishCodes.includes(String(productCode)) ? 'es' : 'en';
            const funnelSource = affiliateCodes.includes(String(productCode)) ? 'affiliate' : 'main';
            
            // Map status - IMPORTANT: check if sale is actually finalized
            // Monetizze can send status='2' (Finalizada) but without valid dataFinalizada
            let mappedStatus = statusMap[String(statusCode)] || 'approved';
            
            // Check dataFinalizada - if it's "0000-00-00" or empty, it's not finalized
            const dataFinalizada = vendaData.dataFinalizada || '';
            const isFinalized = dataFinalizada && 
                               dataFinalizada !== '0000-00-00 00:00:00' && 
                               dataFinalizada !== '0000-00-00' &&
                               !dataFinalizada.startsWith('0000-00-00');
            
            // Check venda.status text for the REAL status
            const vendaStatus = (vendaData.status || '').toLowerCase();
            // Also check tipoEvento.descricao for additional status info
            const eventoDesc = (tipoEvento.descricao || '').toLowerCase();
            const combinedStatus = `${vendaStatus} ${eventoDesc}`;
            
            // Debug: log refund/chargeback detection
            if (statusCode === '4' || statusCode === '8' || statusCode === '9' || 
                combinedStatus.includes('chargeback') || combinedStatus.includes('devolvida') || 
                combinedStatus.includes('reembolso') || combinedStatus.includes('disputa')) {
                console.log(`🔴 REFUND/CHARGEBACK DETECTED - ID: ${transactionId}, statusCode: ${statusCode}, vendaStatus: "${vendaData.status}", eventoDesc: "${tipoEvento.descricao}", mappedStatus (before text check): ${mappedStatus}`);
            }
            
            // Text-based status detection can UPGRADE to refund/chargeback but should NEVER
            // downgrade from refunded/chargeback back to approved/cancelled/etc.
            // The statusCode from tipoEvento.codigo is the most reliable source for refund/chargeback detection.
            const statusCodeIsRefund = (statusCode === '4' || statusCode === '8' || statusCode === '9');
            
            // Check text for refund/chargeback indicators (can upgrade status)
            if (combinedStatus.includes('chargeback') || combinedStatus.includes('disputa') || combinedStatus.includes('contestação') || combinedStatus.includes('contestacao')) {
                mappedStatus = 'chargeback';
            } else if (combinedStatus.includes('devolvida') || combinedStatus.includes('reembolso') || combinedStatus.includes('reembolsada') || combinedStatus.includes('refund')) {
                mappedStatus = 'refunded';
            } else if (statusCodeIsRefund) {
                // StatusCode says refund/chargeback - KEEP IT, don't let text override change it
                // This handles cases where vendaStatus says "Finalizada" but statusCode is 4/8/9
                console.log(`🔒 Preserving ${mappedStatus} from statusCode=${statusCode} (vendaStatus text was: "${vendaData.status}")`);
            } else if (vendaStatus.includes('cancelada') || vendaStatus.includes('cancel')) {
                mappedStatus = 'cancelled';
            } else if (vendaStatus.includes('aguardando') || vendaStatus.includes('pending')) {
                mappedStatus = 'pending_payment';
            } else if (vendaStatus.includes('finalizada') || vendaStatus.includes('aprovada')) {
                // Monetizze says "Finalizada"/"Aprovada" - trust it as approved
                mappedStatus = 'approved';
                if (!isFinalized) {
                    console.log(`⚠️ SYNC: vendaStatus says "${vendaData.status}" but dataFinalizada invalid - trusting as approved`);
                }
            } else if (statusCode === '2' || statusCode === '6') {
                // Status code 2/6 = Monetizze confirmed payment - always mark as approved
                mappedStatus = 'approved';
                if (!isFinalized) {
                    console.log(`⚠️ SYNC: statusCode=${statusCode} (approved) but dataFinalizada invalid - trusting statusCode`);
                }
            }
            
            // Final debug log for refund/chargeback after all status resolution
            if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
                console.log(`✅ FINAL STATUS: ${transactionId} → ${mappedStatus} (email: ${email}, product: ${productName}, value: ${value})`);
            }
            
            if (!email || !transactionId) {
                skipped++;
                continue;
            }
            
            // Try to get correct WhatsApp from leads table (more reliable than Monetizze phone)
            let finalPhone = phone;
            try {
                const leadResult = await pool.query(
                    `SELECT whatsapp FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                    [email]
                );
                if (leadResult.rows.length > 0 && leadResult.rows[0].whatsapp) {
                    finalPhone = leadResult.rows[0].whatsapp;
                }
            } catch (leadErr) {
                // Silently ignore - use Monetizze phone as fallback
            }
            
            // CHECK: Is this transaction becoming "approved" for the first time?
            // If so, we need to send a CAPI Purchase event in real-time
            let wasNewlyApproved = false;
            if (mappedStatus === 'approved') {
                try {
                    const existingTx = await pool.query(
                        `SELECT status FROM transactions WHERE transaction_id = $1`,
                        [transactionId]
                    );
                    // Newly approved if: didn't exist before, OR existed with different status
                    if (existingTx.rows.length === 0 || existingTx.rows[0].status !== 'approved') {
                        // Also check if CAPI was already sent (by postback handler)
                        const capiExists = await pool.query(
                            `SELECT id FROM capi_purchase_logs WHERE transaction_id = $1`,
                            [transactionId]
                        );
                        if (capiExists.rows.length === 0) {
                            wasNewlyApproved = true;
                        }
                    }
                } catch (checkErr) {
                    // Non-blocking
                }
            }
            
            await pool.query(`
                INSERT INTO transactions (
                    transaction_id, email, phone, name, product, value, 
                    monetizze_status, status, raw_data, funnel_language, funnel_source, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, COALESCE($12, NOW()))
                ON CONFLICT (transaction_id) 
                DO UPDATE SET 
                    monetizze_status = $7,
                    status = $8,
                    value = $6,
                    raw_data = $9,
                    funnel_language = $10,
                    funnel_source = $11,
                    phone = COALESCE($3, transactions.phone),
                    updated_at = NOW()
            `, [transactionId, email, finalPhone, name, productName, value, String(statusCode), mappedStatus, JSON.stringify(item), funnelLanguage, funnelSource, saleDate]);
            
            // SYNC CAPI: Don't send Purchase immediately - let the catch-up handle it
            // The catch-up (sendMissingCAPIPurchases) runs right after sync finishes
            // and has the LATEST fbc/fbp data (from enrichPurchase on upsell pages)
            if (wasNewlyApproved && email) {
                console.log(`🔥 SYNC: Transaction ${transactionId} (${email}) just became approved! CAPI Purchase will be sent via catch-up after sync completes.`);
            }
            
            // Also create refund_requests entry for refunds/chargebacks so they appear in admin panel
            if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
                try {
                    const refundProtocol = `MON-${String(transactionId).substring(0, 12).toUpperCase()}`;
                    const refundType = mappedStatus === 'chargeback' ? 'chargeback' : 'refund';
                    
                    // Check if already exists to avoid duplicates
                    const existing = await pool.query(
                        'SELECT id FROM refund_requests WHERE transaction_id = $1',
                        [String(transactionId)]
                    );
                    
                    if (existing.rows.length === 0) {
                        await pool.query(`
                            INSERT INTO refund_requests (
                                protocol, full_name, email, phone, product, reason, 
                                status, source, refund_type, transaction_id, value, funnel_language, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, COALESCE($13, NOW()))
                        `, [
                            refundProtocol,
                            name || 'N/A',
                            email,
                            finalPhone || null,
                            productName,
                            refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                            'approved', // Monetizze already processed it
                            'monetizze',
                            refundType,
                            String(transactionId),
                            parseFloat(value) || 0,
                            funnelLanguage,
                            saleDate
                        ]);
                        
                        console.log(`📥 SYNC: ${refundType.toUpperCase()} registered: ${refundProtocol} - ${email} - ${productName}`);
                    } else {
                        // Update existing entry if status changed (e.g., refund -> chargeback)
                        await pool.query(`
                            UPDATE refund_requests SET 
                                refund_type = $1, 
                                status = 'approved',
                                updated_at = NOW()
                            WHERE transaction_id = $2 AND refund_type != $1
                        `, [refundType, String(transactionId)]);
                    }
                } catch (refundError) {
                    console.error(`⚠️ Error registering refund_request during sync: ${refundError.message}`);
                }
            }
            
            synced++;
        } catch (saleError) {
            errors.push({ sale: item?.venda?.codigo || 'unknown', error: saleError.message });
            skipped++;
        }
    }
    
    console.log(`🎉 Sync complete: ${synced} synced, ${skipped} skipped`);
    return { synced, skipped, total: salesArray.length, errors: errors.length > 0 ? errors : undefined };
}

// ==================== AUTO-SYNC MONETIZZE (every 30 minutes) ====================
let autoSyncInterval = null;

async function runAutoSync() {
    try {
        const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
        if (!consumerKey) {
            console.log('⏭️ Auto-sync skipped: MONETIZZE_CONSUMER_KEY not configured');
            return;
        }
        
        // Sync today and yesterday (to catch late-arriving transactions)
        const today = new Date();
        const yesterday = new Date(today);
        yesterday.setDate(yesterday.getDate() - 1);
        
        const startDate = yesterday.toISOString().split('T')[0];
        const endDate = today.toISOString().split('T')[0];
        
        console.log(`🔄 Auto-sync starting (${startDate} to ${endDate})...`);
        const result = await syncMonetizzeSalesCore(startDate, endDate);
        console.log(`✅ Auto-sync complete: ${result.synced} new/updated, ${result.skipped} skipped, ${result.total} total from Monetizze`);
        
        // After sync, check for newly approved transactions that need CAPI Purchase events
        if (result.synced > 0) {
            console.log('🔄 Auto-sync found new/updated transactions - running CAPI catch-up...');
            await sendMissingCAPIPurchases();
        }
    } catch (error) {
        console.error('❌ Auto-sync error:', error.message);
    }
}

async function runRefundBackfill() {
    try {
        console.log('🔄 Running automatic refund backfill...');
        const result = await pool.query(`
            SELECT t.transaction_id, t.email, t.phone, t.name, t.product, t.value, 
                   t.status, t.funnel_language, t.created_at
            FROM transactions t
            LEFT JOIN refund_requests rr ON rr.transaction_id = t.transaction_id
            WHERE t.status IN ('refunded', 'chargeback')
              AND rr.id IS NULL
            ORDER BY t.created_at DESC
        `);
        
        let created = 0;
        for (const tx of result.rows) {
            try {
                const refundProtocol = `MON-${String(tx.transaction_id).substring(0, 12).toUpperCase()}`;
                const refundType = tx.status === 'chargeback' ? 'chargeback' : 'refund';
                
                await pool.query(`
                    INSERT INTO refund_requests (
                        protocol, full_name, email, phone, product, reason, 
                        status, source, refund_type, transaction_id, value, funnel_language, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (protocol) DO NOTHING
                `, [
                    refundProtocol, tx.name || 'N/A', tx.email, tx.phone || null,
                    tx.product,
                    refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                    'approved', 'monetizze', refundType, String(tx.transaction_id),
                    parseFloat(tx.value) || 0, tx.funnel_language || 'en', tx.created_at || new Date()
                ]);
                created++;
            } catch (err) { /* skip duplicates */ }
        }
        
        if (created > 0) {
            console.log(`✅ Backfill: ${created} refund_requests created from ${result.rows.length} orphaned transactions`);
        }
    } catch (error) {
        console.error('⚠️ Refund backfill error:', error.message);
    }
}

async function runDeepSync() {
    // Deep sync strategy:
    // 1. Sync last 7 days day-by-day (full sync - catches all new transactions + status changes)
    // 2. Then do a targeted refund-only sync for last 60 days (only status 3,4 = cancelled/refunded)
    try {
        const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
        if (!consumerKey) return;
        
        const today = new Date();
        let totalSynced = 0;
        let totalSkipped = 0;
        let totalFetched = 0;
        
        // PHASE 1: Full sync of last 7 days (day-by-day to get ALL transactions including status changes)
        console.log(`🔍 Deep sync PHASE 1: Full sync of last 7 days (day-by-day)...`);
        for (let daysBack = 0; daysBack <= 7; daysBack++) {
            const targetDate = new Date(today);
            targetDate.setDate(targetDate.getDate() - daysBack);
            const dateStr = targetDate.toISOString().split('T')[0];
            
            try {
                const result = await syncMonetizzeSalesCore(dateStr, dateStr);
                totalSynced += result.synced || 0;
                totalSkipped += result.skipped || 0;
                totalFetched += result.total || 0;
                
                if (result.synced > 0 || result.total > 0) {
                    console.log(`📅 Day ${dateStr}: ${result.total} fetched, ${result.synced} synced`);
                }
            } catch (dayError) {
                console.error(`❌ Error syncing ${dateStr}:`, dayError.message);
            }
            
            await new Promise(resolve => setTimeout(resolve, 300));
        }
        
        console.log(`✅ Phase 1 complete: ${totalSynced} synced, ${totalFetched} fetched across 7 days`);
        
        // PHASE 2: Targeted refund/chargeback sync for last 60 days
        // Fetch status 3 (cancelled), 4 (refunded), 8 (chargeback), 9 (chargeback alt)
        console.log(`🔍 Deep sync PHASE 2: Targeted refund/chargeback sync (last 60 days)...`);
        let refundSynced = 0;
        let refundFetched = 0;
        
        try {
            let monetizzeToken;
            try {
                monetizzeToken = await getMonetizzeToken();
            } catch (e) {
                console.error('❌ Phase 2: Monetizze auth failed');
                return;
            }
            
            const sixtyDaysAgo = new Date(today);
            sixtyDaysAgo.setDate(sixtyDaysAgo.getDate() - 60);
            const startDate = sixtyDaysAgo.toISOString().split('T')[0];
            const endDate = today.toISOString().split('T')[0];
            
            const params = new URLSearchParams();
            params.append('date_min', `${startDate} 00:00:00`);
            params.append('date_max', `${endDate} 23:59:59`);
            // 3=cancelled, 4=refunded, 8=chargeback, 9=chargeback alt
            ['3', '4', '8', '9'].forEach(s => params.append('status[]', s));
            
            const validProductCodes = [
                '341972', '349241', '349242', '349243',
                '330254', '341443', '341444', '341448',
                '349260', '349261', '349266', '349267',
                '338375', '341452', '341453', '341454'
            ];
            validProductCodes.forEach(code => params.append('product[]', code));
            
            const txUrl = `https://api.monetizze.com.br/2.1/transactions?${params.toString()}`;
            console.log(`🌐 Fetching refunded/cancelled/chargeback transactions (${startDate} to ${endDate})...`);
            
            const response = await fetch(txUrl, {
                method: 'GET',
                headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
            });
            
            if (response.ok) {
                const data = await response.json();
                let refundArray = [];
                if (Array.isArray(data)) refundArray = data;
                else if (Array.isArray(data.dados)) refundArray = data.dados;
                else if (Array.isArray(data.vendas)) refundArray = data.vendas;
                
                console.log(`📄 Refund query: first page ${refundArray.length} items (paginas: ${data.paginas || 'N/A'})`);
                
                // Handle pagination for refunds
                const totalPages = data.paginas || 0;
                if (totalPages > 1) {
                    for (let page = 2; page <= Math.min(totalPages, 20); page++) {
                        try {
                            const pageParams = new URLSearchParams(params.toString());
                            pageParams.set('pagina', String(page));
                            const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                                method: 'GET',
                                headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                            });
                            if (pageResponse.ok) {
                                const pageData = await pageResponse.json();
                                const pageItems = pageData.dados || pageData.vendas || [];
                                refundArray = refundArray.concat(pageItems);
                                if (pageItems.length === 0) break;
                            }
                        } catch (e) { break; }
                    }
                } else if (refundArray.length >= 100 && totalPages === 0) {
                    // Probe for more pages
                    for (let page = 2; page <= 20; page++) {
                        try {
                            const pageParams = new URLSearchParams(params.toString());
                            pageParams.set('pagina', String(page));
                            const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                                method: 'GET',
                                headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                            });
                            if (pageResponse.ok) {
                                const pageData = await pageResponse.json();
                                const pageItems = Array.isArray(pageData) ? pageData : (pageData.dados || pageData.vendas || []);
                                if (pageItems.length === 0) break;
                                refundArray = refundArray.concat(pageItems);
                            } else break;
                        } catch (e) { break; }
                    }
                }
                
                console.log(`📦 Total refund/cancelled transactions fetched: ${refundArray.length}`);
                refundFetched = refundArray.length;
                
                // Process each refund transaction
                for (const item of refundArray) {
                    try {
                        const vendaData = item.venda || item;
                        const tipoEvento = item.tipoEvento || {};
                        const compradorData = item.comprador || {};
                        
                        const transactionId = String(vendaData.codigo || item.codigo || '');
                        const email = compradorData.email || '';
                        if (!transactionId || !email) continue;
                        
                        const statusCode = String(tipoEvento.codigo || vendaData.statusCodigo || '');
                        const vendaStatus = (vendaData.status || '').toLowerCase();
                        const eventoDesc = (tipoEvento.descricao || '').toLowerCase();
                        
                        // Determine status
                        let mappedStatus = 'cancelled';
                        if (statusCode === '4' || vendaStatus.includes('devolvida') || vendaStatus.includes('reembolso') || eventoDesc.includes('reembolso')) {
                            mappedStatus = 'refunded';
                        }
                        if (vendaStatus.includes('chargeback') || eventoDesc.includes('chargeback') || eventoDesc.includes('disputa') || statusCode === '8' || statusCode === '9') {
                            mappedStatus = 'chargeback';
                        }
                        
                        const productName = (item.produto || {}).nome || 'Unknown';
                        const value = vendaData.valor || vendaData.valorRecebido || vendaData.comissao || '0';
                        const buyerName = compradorData.nome || '';
                        const buyerPhone = compradorData.telefone || '';
                        
                        // Update or insert transaction (PRESERVE fbc/fbp/visitor_id from postback - don't overwrite)
                        const upsert = await pool.query(`
                            INSERT INTO transactions (transaction_id, email, phone, name, product, value, monetizze_status, status, raw_data, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                            ON CONFLICT (transaction_id) DO UPDATE SET
                                status = CASE 
                                    WHEN $8 IN ('refunded', 'chargeback') THEN $8
                                    WHEN transactions.status IN ('refunded', 'chargeback') THEN transactions.status
                                    ELSE $8
                                END,
                                monetizze_status = $7,
                                raw_data = $9,
                                updated_at = NOW()
                            RETURNING (xmax = 0) as is_insert, transactions.fbc, transactions.fbp, transactions.visitor_id
                        `, [
                            transactionId, email, buyerPhone, buyerName, productName,
                            value, statusCode, mappedStatus, JSON.stringify(item),
                            vendaData.dataInicio ? new Date(vendaData.dataInicio.replace(' ', 'T') + '-03:00') : new Date()
                        ]);
                        
                        // Create refund_requests for refunded/chargebacked
                        if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
                            const refundProtocol = `MON-${String(transactionId).substring(0, 12).toUpperCase()}`;
                            const existingRefund = await pool.query('SELECT id FROM refund_requests WHERE transaction_id = $1', [transactionId]);
                            
                            // Detect funnel language from product name
                            const pLower = (productName || '').toLowerCase();
                            const refundLang = (pLower.includes('espanhol') || pLower.includes('spanish') || pLower.includes('español')) ? 'es' : 'en';
                            
                            if (existingRefund.rows.length === 0) {
                                await pool.query(`
                                    INSERT INTO refund_requests (
                                        protocol, full_name, email, phone, product, reason,
                                        status, source, refund_type, transaction_id, value, funnel_language, created_at
                                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
                                    ON CONFLICT (protocol) DO NOTHING
                                `, [
                                    refundProtocol, buyerName || 'N/A', email, buyerPhone || null,
                                    productName,
                                    mappedStatus === 'chargeback' ? 'Chargeback - Disputa' : 'Reembolso via Monetizze',
                                    'approved', 'monetizze',
                                    mappedStatus === 'chargeback' ? 'chargeback' : 'refund',
                                    transactionId, parseFloat(value) || 0, refundLang
                                ]);
                                refundSynced++;
                                console.log(`🔴 REFUND FOUND: ${transactionId} (${email}) → ${mappedStatus} - ${productName} - R$${value}`);
                            }
                        }
                    } catch (itemError) {
                        continue;
                    }
                }
            }
        } catch (phase2Error) {
            console.error('❌ Phase 2 error:', phase2Error.message);
        }
        
        console.log(`✅ Deep sync complete! Phase 1: ${totalSynced} synced/${totalFetched} fetched (7 days) | Phase 2: ${refundSynced} refunds found/${refundFetched} fetched (60 days)`);
    } catch (error) {
        console.error('❌ Deep sync error:', error.message);
    }
}

async function reprocessPostbackLogs() {
    try {
        console.log('🔄 Reprocessing postback logs for missed refunds/chargebacks...');
        
        // Fetch all postback logs
        const logs = await pool.query(`
            SELECT id, body, created_at FROM postback_logs 
            WHERE content_type != 'ERROR_NO_EMAIL' 
              AND content_type != 'CRITICAL_ERROR'
              AND content_type != 'DB_ERROR'
            ORDER BY created_at DESC
        `);
        
        if (logs.rows.length === 0) {
            console.log('No postback logs to reprocess');
            return;
        }
        
        console.log(`📦 Found ${logs.rows.length} postback logs to scan`);
        let fixed = 0;
        
        for (const log of logs.rows) {
            try {
                let body;
                if (typeof log.body === 'string') {
                    body = JSON.parse(log.body);
                } else {
                    body = log.body;
                }
                
                if (!body) continue;
                
                // Check for dot-notation keys
                const hasDotKeys = Object.keys(body).some(k => k.includes('.'));
                function unflattenObj(obj) {
                    const result = {};
                    for (const key of Object.keys(obj)) {
                        if (key.includes('.')) {
                            const parts = key.split('.');
                            let current = result;
                            for (let i = 0; i < parts.length - 1; i++) {
                                if (!current[parts[i]] || typeof current[parts[i]] !== 'object') current[parts[i]] = {};
                                current = current[parts[i]];
                            }
                            current[parts[parts.length - 1]] = obj[key];
                        } else if (result[key] === undefined) {
                            result[key] = obj[key];
                        }
                    }
                    return result;
                }
                const parsed = hasDotKeys ? { ...unflattenObj(body), ...body } : body;
                
                const tipoEvento = (parsed.tipoEvento && typeof parsed.tipoEvento === 'object') ? parsed.tipoEvento : {};
                const venda = (parsed.venda && typeof parsed.venda === 'object') ? parsed.venda : {};
                const comprador = (parsed.comprador && typeof parsed.comprador === 'object') ? parsed.comprador : {};
                
                const statusCode = String(tipoEvento.codigo || parsed['tipoEvento.codigo'] || parsed['tipoEvento[codigo]'] || '');
                const eventoDesc = (tipoEvento.descricao || '').toLowerCase();
                const vendaStatus = (venda.status || '').toLowerCase();
                const transactionId = parsed.chave_unica || venda.codigo || parsed['venda.codigo'] || '';
                const email = comprador.email || parsed.email || parsed['comprador.email'] || '';
                
                if (!transactionId || !email) continue;
                
                // Detect refund/chargeback
                let newStatus = null;
                
                if (statusCode === '4' || vendaStatus.includes('devolvida') || vendaStatus.includes('reembolso') || eventoDesc.includes('reembolso')) {
                    newStatus = 'refunded';
                }
                if (statusCode === '8' || statusCode === '9' || vendaStatus.includes('chargeback') || eventoDesc.includes('chargeback') || eventoDesc.includes('disputa')) {
                    newStatus = 'chargeback';
                }
                
                if (!newStatus) continue;
                
                // Check if this transaction exists in our DB with wrong status
                const existing = await pool.query(
                    `SELECT transaction_id, status FROM transactions WHERE transaction_id = $1`,
                    [transactionId]
                );
                
                if (existing.rows.length > 0 && existing.rows[0].status !== newStatus) {
                    console.log(`🔧 FIXING postback log ${log.id}: tx ${transactionId} (${email}) - was "${existing.rows[0].status}" → should be "${newStatus}" (code: ${statusCode})`);
                    
                    // Update transaction status
                    await pool.query(
                        `UPDATE transactions SET status = $1, monetizze_status = $2, updated_at = NOW() WHERE transaction_id = $3`,
                        [newStatus, statusCode, transactionId]
                    );
                    
                    // Create refund_requests entry
                    const refundProtocol = `MON-${String(transactionId).substring(0, 12).toUpperCase()}`;
                    const existingRefund = await pool.query('SELECT id FROM refund_requests WHERE transaction_id = $1', [transactionId]);
                    
                    if (existingRefund.rows.length === 0) {
                        const produto = (parsed.produto && typeof parsed.produto === 'object') ? parsed.produto : {};
                        const productName = produto.nome || parsed['produto.nome'] || 'Unknown Product';
                        const valor = venda.valor || parsed.valor || parsed['venda.valor'] || '0';
                        const buyerName = comprador.nome || parsed.nome || parsed['comprador.nome'] || 'N/A';
                        const buyerPhone = comprador.telefone || parsed.telefone || parsed['comprador.telefone'] || null;
                        
                        await pool.query(`
                            INSERT INTO refund_requests (
                                protocol, full_name, email, phone, product, reason, 
                                status, source, refund_type, transaction_id, value, funnel_language, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                            ON CONFLICT (protocol) DO NOTHING
                        `, [
                            refundProtocol, buyerName, email, buyerPhone,
                            productName,
                            newStatus === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                            'approved', 'monetizze', newStatus === 'chargeback' ? 'chargeback' : 'refund',
                            transactionId, parseFloat(valor) || 0, 'en', log.created_at || new Date()
                        ]);
                    }
                    
                    fixed++;
                } else if (existing.rows.length === 0) {
                    // Transaction doesn't exist - it may have been skipped
                    console.log(`⚠️ Postback log ${log.id}: tx ${transactionId} (${email}) is ${newStatus} but NOT in transactions table`);
                }
            } catch (logError) {
                // Skip individual log errors
                continue;
            }
        }
        
        console.log(`✅ Postback log reprocessing complete: ${fixed} transactions fixed`);
    } catch (error) {
        console.error('❌ Postback reprocessing error:', error.message);
    }
}

// Re-check approved transactions by querying Monetizze API individually
// The Monetizze /transactions list API doesn't reflect chargeback status changes,
// but individual transaction queries or re-fetching by date MIGHT show updated status.
// As a workaround, we re-fetch transactions by venda.codigo to check for status changes.
async function recheckApprovedTransactions() {
    try {
        const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
        if (!consumerKey) return;
        
        console.log('🔍 Re-checking approved transactions for status changes...');
        
        // Get all approved transactions from the last 60 days
        const approved = await pool.query(`
            SELECT transaction_id, email, name, phone, product, value, funnel_language, created_at
            FROM transactions 
            WHERE status = 'approved' 
              AND created_at >= NOW() - INTERVAL '60 days'
            ORDER BY created_at DESC
        `);
        
        if (approved.rows.length === 0) {
            console.log('No approved transactions to re-check');
            return;
        }
        
        console.log(`📦 Re-checking ${approved.rows.length} approved transactions...`);
        
        // Get a fresh Monetizze token
        let monetizzeToken;
        try {
            monetizzeToken = await getMonetizzeToken();
        } catch (e) {
            console.error('❌ Cannot re-check: Monetizze auth failed');
            return;
        }
        
        let updated = 0;
        let refundsFound = 0;
        
        // Query each transaction individually using Monetizze API
        // We'll batch by querying each transaction_id (venda.codigo)
        for (const tx of approved.rows) {
            try {
                // Query Monetizze for this specific transaction
                const txUrl = `https://api.monetizze.com.br/2.1/transactions?codigo=${tx.transaction_id}`;
                const response = await fetch(txUrl, {
                    method: 'GET',
                    headers: { 'TOKEN': monetizzeToken, 'Accept': 'application/json' }
                });
                
                if (!response.ok) continue;
                
                const data = await response.json();
                const items = Array.isArray(data) ? data : (data.dados || data.vendas || []);
                
                if (items.length === 0) continue;
                
                const item = items[0];
                const vendaData = item.venda || item;
                const tipoEvento = item.tipoEvento || {};
                const vendaStatus = (vendaData.status || '').toLowerCase();
                const eventoDesc = (tipoEvento.descricao || '').toLowerCase();
                const eventoCodigo = String(tipoEvento.codigo || '');
                
                // Check if this transaction is now refunded or chargebacked
                const isRefund = eventoCodigo === '4' || vendaStatus.includes('devolvida') || vendaStatus.includes('reembolso');
                const isChargeback = eventoCodigo === '8' || eventoCodigo === '9' || 
                    vendaStatus.includes('chargeback') || eventoDesc.includes('chargeback') || 
                    eventoDesc.includes('disputa');
                
                if (isRefund || isChargeback) {
                    const newStatus = isChargeback ? 'chargeback' : 'refunded';
                    const refundType = isChargeback ? 'chargeback' : 'refund';
                    
                    console.log(`🔴 STATUS CHANGE DETECTED: ${tx.transaction_id} (${tx.email}) was approved → now ${newStatus} (vendaStatus: "${vendaData.status}", eventoDesc: "${tipoEvento.descricao}")`);
                    
                    // Update transaction status
                    await pool.query(
                        `UPDATE transactions SET status = $1, monetizze_status = $2, updated_at = NOW() WHERE transaction_id = $3`,
                        [newStatus, eventoCodigo || '4', tx.transaction_id]
                    );
                    
                    // Create refund_requests entry
                    const refundProtocol = `MON-${String(tx.transaction_id).substring(0, 12).toUpperCase()}`;
                    const existing = await pool.query('SELECT id FROM refund_requests WHERE transaction_id = $1', [tx.transaction_id]);
                    
                    if (existing.rows.length === 0) {
                        await pool.query(`
                            INSERT INTO refund_requests (
                                protocol, full_name, email, phone, product, reason, 
                                status, source, refund_type, transaction_id, value, funnel_language, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                            ON CONFLICT (protocol) DO NOTHING
                        `, [
                            refundProtocol, tx.name || 'N/A', tx.email, tx.phone || null,
                            tx.product,
                            isChargeback ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                            'approved', 'monetizze', refundType, tx.transaction_id,
                            parseFloat(tx.value) || 0, tx.funnel_language || 'en', tx.created_at || new Date()
                        ]);
                        refundsFound++;
                    }
                    
                    updated++;
                }
                
                // Small delay to avoid rate limiting Monetizze API
                await new Promise(resolve => setTimeout(resolve, 200));
                
            } catch (txError) {
                // Skip errors for individual transactions
                continue;
            }
        }
        
        console.log(`✅ Re-check complete: ${updated} status changes found, ${refundsFound} refund_requests created (out of ${approved.rows.length} checked)`);
        
    } catch (error) {
        console.error('❌ Re-check error:', error.message);
    }
}

async function runDiagnosticLog() {
    try {
        console.log('\n========== 🔍 REFUND DIAGNOSTIC LOG ==========');
        
        const txStatus = await pool.query('SELECT status, COUNT(*) as count FROM transactions GROUP BY status ORDER BY count DESC');
        console.log('📊 Transactions by status:', JSON.stringify(txStatus.rows));
        
        const refReq = await pool.query('SELECT refund_type, source, status, COUNT(*) as count FROM refund_requests GROUP BY refund_type, source, status ORDER BY count DESC');
        console.log('📊 Refund requests:', JSON.stringify(refReq.rows));
        
        // Check all unique status combos from Monetizze raw data
        const distinctStatuses = await pool.query(`
            SELECT raw_data->'venda'->>'status' as venda_status, 
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao,
                   COUNT(*) as count
            FROM transactions WHERE raw_data IS NOT NULL
            GROUP BY raw_data->'venda'->>'status', raw_data->'tipoEvento'->>'codigo', raw_data->'tipoEvento'->>'descricao'
            ORDER BY count DESC
        `);
        console.log('📊 Distinct API statuses:', JSON.stringify(distinctStatuses.rows));
        
        // Broad search for refund keywords in ALL raw_data (cast to text)
        const broadSearch = await pool.query(`
            SELECT transaction_id, email, status, monetizze_status,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao
            FROM transactions 
            WHERE LOWER(CAST(raw_data AS text)) LIKE '%devolv%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%chargeback%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%reembolso%'
               OR monetizze_status IN ('4', '8', '9')
            LIMIT 20
        `);
        console.log('📊 Broad refund search results:', broadSearch.rows.length, 'found');
        if (broadSearch.rows.length > 0) {
            broadSearch.rows.forEach(r => console.log(`  🔴 ${r.transaction_id} | status: ${r.status} | monetizze: ${r.monetizze_status} | venda: ${r.venda_status_text} | evento: ${r.evento_codigo}/${r.evento_descricao}`));
        }
        
        // Check specific Monetizze IDs from the screenshot (5580, 5579, 5578, 5577, 5576)
        const specific = await pool.query(`
            SELECT transaction_id, email, status, monetizze_status
            FROM transactions
            WHERE transaction_id IN ('55803165', '55799093', '55785981', '55781116', '55781107', '55780560', '55779651', '55778218', '55777545', '55777542', '55777539', '55777537', '55764031')
        `);
        console.log('📊 Specific Monetizze IDs check:', specific.rows.length > 0 ? JSON.stringify(specific.rows) : 'NONE FOUND - these transactions are NOT in our DB');
        
        // Sample of cancelled transactions
        const cancelled = await pool.query(`
            SELECT transaction_id, email, monetizze_status,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao
            FROM transactions WHERE status = 'cancelled' ORDER BY created_at DESC LIMIT 5
        `);
        console.log('📊 Recent cancelled sample:', JSON.stringify(cancelled.rows));
        
        console.log('========== END DIAGNOSTIC ==========\n');
    } catch (error) {
        console.error('Diagnostic error:', error.message);
    }
}

function startAutoSync() {
    // Run backfill + CAPI catch-up after startup (10 seconds after start)
    setTimeout(async () => {
        // First, backfill fbc/fbp from raw_data into transactions columns
        await backfillTransactionFbcFbp();
        
        console.log('🚀 Running CAPI catch-up on startup...');
        await sendMissingCAPIPurchases();
        // Schedule recurring CAPI catch-up every 30 minutes (reduced from 10min to prevent excessive sends)
        setInterval(sendMissingCAPIPurchases, 30 * 60 * 1000);
    }, 10000);
    
    // Run heavy startup tasks 30 seconds after server start
    setTimeout(async () => {
        // Step 1: Deep sync last 7 days (most important - gets sales data into DB)
        await runDeepSync();
        
        // CRITICAL: Set up auto-sync IMMEDIATELY after deep sync, before heavy CAPI tasks
        // This ensures sales data stays fresh every 5 minutes even while CAPI catch-up runs
        autoSyncInterval = setInterval(runAutoSync, 5 * 60 * 1000);
        setInterval(recheckApprovedTransactions, 6 * 60 * 60 * 1000);
        console.log('🔄 Auto-sync scheduled: every 5 minutes | Re-check: every 6 hours | CAPI catch-up: every 30 min');
        
        // Step 2: Run one immediate auto-sync to catch any transactions that arrived during restart
        try { await runAutoSync(); } catch(e) { console.error('Initial auto-sync error:', e.message); }
        
        // Step 3+: Background tasks (non-blocking for data freshness)
        try { await reprocessPostbackLogs(); } catch(e) { console.error('Reprocess error:', e.message); }
        try { await recheckApprovedTransactions(); } catch(e) { console.error('Recheck error:', e.message); }
        try { await runRefundBackfill(); } catch(e) { console.error('Refund backfill error:', e.message); }
        try { await runDiagnosticLog(); } catch(e) { console.error('Diagnostic error:', e.message); }
        // CAPI catch-up runs last (heavy task, not blocking data display)
        try { await sendMissingCAPIPurchases(); } catch(e) { console.error('CAPI catch-up error:', e.message); }
    }, 30000);
}

module.exports = {
    getMonetizzeToken,
    testMonetizzeApi,
    syncMonetizzeSalesCore,
    runAutoSync,
    runRefundBackfill,
    runDeepSync,
    reprocessPostbackLogs,
    recheckApprovedTransactions,
    runDiagnosticLog,
    startAutoSync
};
