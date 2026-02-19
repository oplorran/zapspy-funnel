const express = require('express');
const router = express.Router();
const pool = require('../database');
const { authenticateToken, requireAdmin } = require('../middleware');
const { getMonetizzeToken } = require('../services/monetizze');
const { sendToFacebookCAPI, sendMissingCAPIPurchases, backfillTransactionFbcFbp } = require('../services/facebook-capi');
const { parseMonetizzeDate } = require('../helpers');

// Shared in-memory postback storage from postbacks module
const { recentPostbacks } = require('./postbacks');

// ==================== DEBUG ENDPOINTS ====================

router.get('/api/admin/debug/sales-count', authenticateToken, async (req, res) => {
    try {
        const total = await pool.query(`SELECT COUNT(*) as count FROM transactions`);
        const approved = await pool.query(`SELECT COUNT(*) as count FROM transactions WHERE status = 'approved'`);
        const byStatus = await pool.query(`SELECT status, COUNT(*) as count FROM transactions GROUP BY status ORDER BY count DESC`);
        const byDate = await pool.query(`
            SELECT (created_at AT TIME ZONE 'America/Sao_Paulo')::date as date, COUNT(*) as total, 
                   SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved
            FROM transactions 
            GROUP BY (created_at AT TIME ZONE 'America/Sao_Paulo')::date 
            ORDER BY date DESC 
            LIMIT 10
        `);
        
        const cancelledTotal = await pool.query(`
            SELECT COUNT(*) as total_rows, COUNT(DISTINCT transaction_id) as unique_tx
            FROM transactions 
            WHERE status IN ('cancelled', 'pending_payment', 'blocked', 'refused', 'rejected', 'waiting_payment')
        `);
        
        const cancelledToday = await pool.query(`
            SELECT transaction_id, email, product, value, status, created_at
            FROM transactions 
            WHERE status IN ('cancelled', 'pending_payment', 'blocked', 'refused', 'rejected', 'waiting_payment')
            AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
            ORDER BY created_at DESC
        `);
        
        res.json({
            totalTransactions: parseInt(total.rows[0].count),
            approvedTransactions: parseInt(approved.rows[0].count),
            byStatus: byStatus.rows,
            byDate: byDate.rows,
            cancelledDebug: {
                totalRows: parseInt(cancelledTotal.rows[0].total_rows),
                uniqueTransactions: parseInt(cancelledTotal.rows[0].unique_tx),
                todayTransactions: cancelledToday.rows
            }
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Debug endpoint to see recent postbacks (memory + DB)
router.get('/api/admin/debug/postbacks', authenticateToken, requireAdmin, async (req, res) => {
    const postbacksWithValueInfo = recentPostbacks.map(p => {
        const venda = p.body?.venda || {};
        return {
            timestamp: p.timestamp,
            chave_unica: p.body?.chave_unica || venda.codigo,
            status: p.body?.tipoEvento?.codigo,
            produto: p.body?.produto?.nome,
            valores: {
                'body.comissao': p.body?.comissao || 'N/A',
                'venda.comissao': venda.comissao || 'N/A',
                'venda.valorLiquido': venda.valorLiquido || 'N/A',
                'venda.valorRecebido': venda.valorRecebido || 'N/A',
                'venda.valor': venda.valor || 'N/A',
                'body.valor': p.body?.valor || 'N/A'
            },
            comprador: p.body?.comprador?.email,
            hasDotKeys: p.hasDotKeys
        };
    });
    
    let dbLogs = [];
    try {
        const dbResult = await pool.query(`
            SELECT id, content_type, body, created_at 
            FROM postback_logs 
            ORDER BY created_at DESC 
            LIMIT 30
        `);
        dbLogs = dbResult.rows;
    } catch (err) {
        dbLogs = [{ error: err.message }];
    }
    
    let recentTx = [];
    try {
        const txResult = await pool.query(`
            SELECT transaction_id, email, product, value, status, monetizze_status, created_at 
            FROM transactions 
            ORDER BY created_at DESC 
            LIMIT 10
        `);
        recentTx = txResult.rows;
    } catch (err) {
        recentTx = [{ error: err.message }];
    }
    
    res.json({
        memoryCount: recentPostbacks.length,
        dbLogCount: dbLogs.length,
        info: 'Memory postbacks are lost on server restart. DB logs persist. Check if Monetizze is sending postbacks to the correct URL.',
        expectedUrl: 'https://zapspy-funnel-production.up.railway.app/api/postback/monetizze',
        alternateUrl: 'https://painel.xaimonitor.com/api/postback/monetizze',
        postbacks: postbacksWithValueInfo,
        dbLogs,
        recentTransactions: recentTx,
        fullPostbacks: recentPostbacks
    });
});

// Debug endpoint to search for a specific transaction
router.get('/api/admin/debug/transaction/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        const result = await pool.query(`
            SELECT * FROM transactions 
            WHERE transaction_id ILIKE $1 
               OR transaction_id ILIKE $2
            ORDER BY created_at DESC
            LIMIT 10
        `, [`%${id}%`, id]);
        
        const logsResult = await pool.query(`
            SELECT id, content_type, body, created_at 
            FROM postback_logs 
            WHERE body::text ILIKE $1
            ORDER BY created_at DESC
            LIMIT 10
        `, [`%${id}%`]);
        
        res.json({
            searchId: id,
            foundTransactions: result.rows.length,
            transactions: result.rows,
            foundInPostbackLogs: logsResult.rows.length,
            postbackLogs: logsResult.rows
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Debug endpoint to search transactions by email
router.get('/api/admin/debug/transactions-by-email/:email', authenticateToken, async (req, res) => {
    try {
        const { email } = req.params;
        
        const transactions = await pool.query(`
            SELECT id, transaction_id, product, value, status, monetizze_status, 
                   funnel_language, funnel_source, email, phone, created_at, updated_at,
                   raw_data
            FROM transactions 
            WHERE LOWER(email) = LOWER($1)
            ORDER BY created_at DESC
        `, [email]);
        
        const lead = await pool.query(`
            SELECT id, email, name, status, total_spent, products_purchased, 
                   first_purchase_at, last_purchase_at, visitor_id
            FROM leads 
            WHERE LOWER(email) = LOWER($1)
            LIMIT 1
        `, [email]);
        
        const approvedTransactions = transactions.rows.filter(t => t.status === 'approved');
        const calculatedTotal = approvedTransactions.reduce((sum, t) => sum + parseFloat(t.value || 0), 0);
        
        res.json({
            email: email,
            foundTransactions: transactions.rows.length,
            approvedCount: approvedTransactions.length,
            transactions: transactions.rows,
            lead: lead.rows[0] || null,
            analysis: {
                leadTotalSpent: lead.rows[0]?.total_spent || 0,
                calculatedTotalFromTransactions: calculatedTotal,
                mismatch: lead.rows[0] ? (parseFloat(lead.rows[0].total_spent || 0) !== calculatedTotal) : false,
                suggestedFix: lead.rows[0] && (parseFloat(lead.rows[0].total_spent || 0) !== calculatedTotal) 
                    ? `UPDATE leads SET total_spent = ${calculatedTotal} WHERE LOWER(email) = LOWER('${email}')`
                    : null
            }
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Fix transaction status endpoint
router.post('/api/admin/fix-transaction/:transactionId', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const { transactionId } = req.params;
        const { newStatus, syncFromMonetizze } = req.body;
        
        if (!newStatus && !syncFromMonetizze) {
            return res.status(400).json({ error: 'Provide newStatus or set syncFromMonetizze: true' });
        }
        
        const txResult = await pool.query(`
            SELECT * FROM transactions WHERE transaction_id = $1
        `, [transactionId]);
        
        if (txResult.rows.length === 0) {
            return res.status(404).json({ error: 'Transaction not found' });
        }
        
        const tx = txResult.rows[0];
        const oldStatus = tx.status;
        
        const validStatuses = ['approved', 'pending_payment', 'cancelled', 'refunded', 'chargeback', 'abandoned_checkout'];
        if (!validStatuses.includes(newStatus)) {
            return res.status(400).json({ error: `Invalid status. Use: ${validStatuses.join(', ')}` });
        }
        
        await pool.query(`
            UPDATE transactions 
            SET status = $1, updated_at = NOW()
            WHERE transaction_id = $2
        `, [newStatus, transactionId]);
        
        if (tx.email) {
            const approvedSum = await pool.query(`
                SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total
                FROM transactions 
                WHERE LOWER(email) = LOWER($1) AND status = 'approved'
            `, [tx.email]);
            
            await pool.query(`
                UPDATE leads 
                SET total_spent = $1, updated_at = NOW()
                WHERE LOWER(email) = LOWER($2)
            `, [approvedSum.rows[0].total, tx.email]);
        }
        
        res.json({
            success: true,
            transactionId,
            oldStatus,
            newStatus,
            message: `Transaction ${transactionId} updated from ${oldStatus} to ${newStatus}`
        });
        
    } catch (error) {
        console.error('Error fixing transaction:', error);
        res.status(500).json({ error: error.message });
    }
});

// Test Monetizze API connectivity (debug endpoint)
router.get('/api/admin/test-monetizze-api', authenticateToken, requireAdmin, async (req, res) => {
    const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
    const results = { tests: [], consumerKeyPresent: !!consumerKey, keyLength: consumerKey?.length };
    
    if (!consumerKey) {
        return res.json({ error: 'MONETIZZE_CONSUMER_KEY not configured', results });
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
    
    res.json(results);
});

// ==================== SYNC MONETIZZE ====================

router.post('/api/admin/sync-monetizze', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const { startDate, endDate, productCodes } = req.body;
        
        const consumerKey = process.env.MONETIZZE_CONSUMER_KEY;
        
        if (!consumerKey) {
            return res.status(500).json({ 
                error: 'Monetizze API credentials not configured',
                message: 'Configure MONETIZZE_CONSUMER_KEY in environment variables'
            });
        }
        
        console.log('🔄 Starting manual Monetizze sync...');
        console.log('📅 Date range:', startDate || 'today', 'to', endDate || 'today');
        
        let monetizzeToken;
        try {
            monetizzeToken = await getMonetizzeToken();
        } catch (authError) {
            console.error('❌ Monetizze authentication failed:', authError.message);
            return res.status(200).json({ 
                success: false,
                error: 'Monetizze authentication failed',
                details: authError.message,
                hint: 'Verifique se a MONETIZZE_CONSUMER_KEY está correta no Railway.'
            });
        }
        
        const params = new URLSearchParams();
        if (startDate) params.append('date_min', `${startDate} 00:00:00`);
        if (endDate) params.append('date_max', `${endDate} 23:59:59`);
        ['1','2','3','4','5','6','8','9'].forEach(s => params.append('status[]', s));
        
        const validProductCodes = [
            '341972', '349241', '349242', '349243',
            '330254', '341443', '341444', '341448',
            '349260', '349261', '349266', '349267',
            '338375', '341452', '341453', '341454'
        ];
        validProductCodes.forEach(code => params.append('product[]', code));
        
        const txUrl = `https://api.monetizze.com.br/2.1/transactions?${params.toString()}`;
        console.log('🌐 Fetching transactions from Monetizze API 2.1:', txUrl);
        
        const response = await fetch(txUrl, {
            method: 'GET',
            headers: {
                'TOKEN': monetizzeToken,
                'Accept': 'application/json'
            }
        });
        
        if (!response.ok) {
            const errorText = await response.text();
            console.error('❌ Monetizze API error:', response.status, errorText);
            
            let errorDetails = errorText;
            try {
                const errorJson = JSON.parse(errorText);
                errorDetails = errorJson.message || errorJson.error || errorText;
            } catch (e) {}
            
            return res.status(200).json({ 
                success: false,
                error: 'Monetizze API request failed',
                status: response.status,
                details: errorDetails,
                hint: response.status === 403 ? 'Token expirado. Tente novamente.' : 'Erro na API da Monetizze.'
            });
        }
        
        const data = await response.json();
        
        console.log('📦 Response type:', typeof data);
        console.log('📦 Is array:', Array.isArray(data));
        if (!Array.isArray(data)) {
            console.log('📦 Response keys:', Object.keys(data));
            console.log('📦 Status field:', data.status);
            console.log('📦 Pagination:', data.pagina, '/', data.paginas, 'registros:', data.registros);
        }
        
        let salesArray = [];
        if (Array.isArray(data)) {
            salesArray = data;
        } else if (Array.isArray(data.dados)) {
            salesArray = data.dados;
        } else if (Array.isArray(data.vendas)) {
            salesArray = data.vendas;
        } else if (Array.isArray(data.recordset)) {
            salesArray = data.recordset;
        }
        
        console.log(`📦 First page: ${salesArray.length} transactions`);
        console.log(`📄 API pagination info - pagina: ${data.pagina}, paginas: ${data.paginas}, registros: ${data.registros}`);
        
        const totalPages = data.paginas || 0;
        const totalRecords = data.registros || salesArray.length;
        
        if (totalPages > 1) {
            console.log(`📄 Pagination detected: ${totalPages} pages, ${totalRecords} total records`);
            for (let page = 2; page <= Math.min(totalPages, 50); page++) {
                try {
                    console.log(`📄 Fetching page ${page}/${totalPages}...`);
                    const pageParams = new URLSearchParams(params.toString());
                    pageParams.set('pagina', String(page));
                    
                    const pageResponse = await fetch(`https://api.monetizze.com.br/2.1/transactions?${pageParams.toString()}`, {
                        method: 'GET',
                        headers: {
                            'TOKEN': monetizzeToken,
                            'Accept': 'application/json'
                        }
                    });
                    
                    if (pageResponse.ok) {
                        const pageData = await pageResponse.json();
                        const pageItems = pageData.dados || pageData.vendas || [];
                        salesArray = salesArray.concat(pageItems);
                        console.log(`📄 Page ${page}: ${pageItems.length} transactions (total: ${salesArray.length})`);
                        if (pageItems.length === 0) break;
                    }
                } catch (pageError) {
                    console.error(`❌ Error fetching page ${page}:`, pageError.message);
                }
            }
        } else if (salesArray.length >= 100 && totalPages === 0) {
            console.log(`⚠️ Got ${salesArray.length} items but no pagination info. Probing for more pages...`);
            for (let page = 2; page <= 15; page++) {
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
            return res.json({
                success: true,
                message: 'No sales found for the specified period',
                synced: 0,
                skipped: 0,
                total: 0,
                responseKeys: Object.keys(data),
                rawPreview: JSON.stringify(data).substring(0, 500)
            });
        }
        
        let synced = 0;
        let skipped = 0;
        let errors = [];
        
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
                const status = vendaData.status || tipoEvento.descricao;
                const statusCode = String(tipoEvento.codigo || item.codigo_status || '2');
                
                console.log(`📋 Processing sale: ID=${transactionId}, Product=${productName} (${productCode}), Value=${value}, Status=${status} (${statusCode})`);
                
                const validProductCodesInner = [
                    '341972', '349241', '349242', '349243',
                    '330254', '341443', '341444', '341448',
                    '349260', '349261', '349266', '349267',
                    '338375', '341452', '341453', '341454'
                ];
                if (productCode && !validProductCodesInner.includes(String(productCode))) {
                    console.log(`⏭️ Skipping product not in our funnel: ${productCode} - ${productName}`);
                    skipped++;
                    continue;
                }
                
                const saleDateStr = vendaData.dataInicio || vendaData.dataFinalizada || vendaData.dataVenda || vendaData.data || null;
                const saleDate = parseMonetizzeDate(saleDateStr);
                
                const spanishCodes = ['349260', '349261', '349266', '349267', '338375', '341452', '341453', '341454'];
                const affiliateCodes = ['330254', '341443', '341444', '341448', '338375', '341452', '341453', '341454'];
                
                let funnelLanguage = spanishCodes.includes(String(productCode)) ? 'es' : 'en';
                const funnelSource = affiliateCodes.includes(String(productCode)) ? 'affiliate' : 'main';
                
                const statusMap = {
                    '1': 'pending_payment',
                    '2': 'approved',
                    '3': 'cancelled',
                    '4': 'refunded',
                    '5': 'blocked',
                    '6': 'approved',
                    '7': 'abandoned_checkout',
                    '8': 'chargeback',
                    '9': 'chargeback'
                };
                let mappedStatus = statusMap[String(statusCode)] || 'approved';
                
                const dataFinalizada = vendaData.dataFinalizada || '';
                const isFinalized = dataFinalizada && 
                                   dataFinalizada !== '0000-00-00 00:00:00' && 
                                   dataFinalizada !== '0000-00-00' &&
                                   !dataFinalizada.startsWith('0000-00-00');
                
                const vendaStatus = (vendaData.status || '').toLowerCase();
                const eventoDesc = (tipoEvento.descricao || '').toLowerCase();
                const combinedStatus = `${vendaStatus} ${eventoDesc}`;
                
                const statusCodeIsRefund = (statusCode === '4' || statusCode === '8' || statusCode === '9');
                
                if (combinedStatus.includes('chargeback') || combinedStatus.includes('disputa') || combinedStatus.includes('contestação') || combinedStatus.includes('contestacao')) {
                    mappedStatus = 'chargeback';
                } else if (combinedStatus.includes('devolvida') || combinedStatus.includes('reembolso') || combinedStatus.includes('reembolsada') || combinedStatus.includes('refund')) {
                    mappedStatus = 'refunded';
                } else if (statusCodeIsRefund) {
                    console.log(`🔒 Preserving ${mappedStatus} from statusCode=${statusCode} (vendaStatus: "${vendaData.status}")`);
                } else if (vendaStatus.includes('cancelada') || vendaStatus.includes('cancel')) {
                    mappedStatus = 'cancelled';
                } else if (vendaStatus.includes('aguardando') || vendaStatus.includes('pending')) {
                    mappedStatus = 'pending_payment';
                } else if (vendaStatus.includes('finalizada') || vendaStatus.includes('aprovada')) {
                    mappedStatus = 'approved';
                    if (!isFinalized) {
                        console.log(`⚠️ REPROCESS: vendaStatus says "${vendaData.status}" but dataFinalizada invalid - trusting as approved`);
                    }
                } else if (statusCode === '2' || statusCode === '6') {
                    mappedStatus = 'approved';
                    if (!isFinalized) {
                        console.log(`⚠️ REPROCESS: statusCode=${statusCode} (approved) but dataFinalizada invalid - trusting statusCode`);
                    }
                }
                
                if (!email || !transactionId) {
                    console.log(`⚠️ Skipping sale without email or ID:`, transactionId);
                    skipped++;
                    continue;
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
                        raw_data = $9,
                        funnel_language = $10,
                        funnel_source = $11,
                        updated_at = NOW()
                `, [
                    transactionId, email, phone, name, productName, value,
                    String(statusCode), mappedStatus, JSON.stringify(item),
                    funnelLanguage, funnelSource, saleDate
                ]);
                
                if (mappedStatus === 'refunded' || mappedStatus === 'chargeback') {
                    try {
                        const refundProtocol = `MON-${String(transactionId).substring(0, 12).toUpperCase()}`;
                        const refundType = mappedStatus === 'chargeback' ? 'chargeback' : 'refund';
                        
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
                                refundProtocol, name || 'N/A', email, phone || null, productName,
                                refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                                'approved', 'monetizze', refundType, String(transactionId),
                                parseFloat(value) || 0, funnelLanguage, saleDate
                            ]);
                            console.log(`📥 MANUAL SYNC: ${refundType.toUpperCase()} registered: ${refundProtocol} - ${email}`);
                        } else {
                            await pool.query(`
                                UPDATE refund_requests SET refund_type = $1, status = 'approved', updated_at = NOW()
                                WHERE transaction_id = $2 AND refund_type != $1
                            `, [refundType, String(transactionId)]);
                        }
                    } catch (refundError) {
                        console.error(`⚠️ Error registering refund_request during manual sync: ${refundError.message}`);
                    }
                }
                
                console.log(`✅ Synced: ${transactionId} - ${email} - ${productName}`);
                synced++;
                
            } catch (saleError) {
                console.error(`❌ Error processing sale:`, saleError.message);
                errors.push({ sale: item?.venda?.codigo || 'unknown', error: saleError.message });
                skipped++;
            }
        }
        
        console.log(`🎉 Sync complete: ${synced} synced, ${skipped} skipped`);
        
        res.json({
            success: true,
            message: 'Monetizze sync completed',
            synced,
            skipped,
            total: salesArray.length,
            errors: errors.length > 0 ? errors : undefined
        });
        
    } catch (error) {
        console.error('❌ Monetizze sync error:', error);
        res.status(500).json({ 
            error: 'Sync failed',
            message: error.message
        });
    }
});

// ==================== TEST POSTBACK ====================

router.post('/api/admin/test-postback', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const { statusCode, email, language = 'en' } = req.body;
        const code = String(statusCode || '7');
        const validCodes = ['1', '2', '6', '7'];
        if (!validCodes.includes(code)) {
            return res.status(400).json({
                success: false,
                error: 'statusCode must be 1, 2, 6 or 7',
                mapping: { '1': 'InitiateCheckout (aguardando)', '2': 'Purchase (aprovada)', '6': 'Purchase (completa)', '7': 'InitiateCheckout (abandono)' }
            });
        }
        const testEmail = email || `test-postback-${Date.now()}@test.local`;
        const chave_unica = `test_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
        const funnelLanguage = (language === 'es' ? 'es' : 'en');

        let leadData = null;
        try {
            const leadResult = await pool.query(
                `SELECT ip_address, user_agent, fbc, fbp, country, country_code, city, state, name, target_gender, whatsapp, visitor_id 
                 FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                [testEmail]
            );
            if (leadResult.rows.length > 0) leadData = leadResult.rows[0];
        } catch (e) { /* ignore */ }

        const fbUserData = {
            email: testEmail,
            phone: leadData?.whatsapp || null,
            firstName: leadData?.name || 'Test',
            ip: leadData?.ip_address || null,
            userAgent: leadData?.user_agent || null,
            fbc: leadData?.fbc || null,
            fbp: leadData?.fbp || null,
            country: leadData?.country_code || null,
            city: leadData?.city || null,
            state: leadData?.state || null,
            gender: leadData?.target_gender || null,
            externalId: leadData?.visitor_id || null
        };
        const fbCustomData = {
            content_name: 'Test Product',
            content_ids: ['TEST'],
            content_type: 'product',
            value: funnelLanguage === 'es' ? 27 : 37,
            currency: 'USD',
            order_id: chave_unica,
            num_items: 1,
            customer_segmentation: 'new_customer_to_business'
        };
        const eventSourceUrl = funnelLanguage === 'es' ? 'https://espanhol.zappdetect.com/' : 'https://ingles.zappdetect.com/';
        const eventId = `test_${chave_unica}`;
        const testCode = funnelLanguage === 'es' ? (process.env.FB_TEST_CODE_ES || 'TEST96875') : (process.env.FB_TEST_CODE_EN || 'TEST23104');
        const capiOptions = { language: funnelLanguage, testEventCode: testCode };

        let eventSent = 'none';
        if (code === '7' || code === '1') {
            await sendToFacebookCAPI('InitiateCheckout', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_checkout`, capiOptions);
            eventSent = 'InitiateCheckout';
        } else if (code === '2' || code === '6') {
            await sendToFacebookCAPI('Purchase', fbUserData, fbCustomData, eventSourceUrl, `${eventId}_purchase`, capiOptions);
            eventSent = 'Purchase';
        }

        return res.json({
            success: true,
            message: `Postback de teste processado. Evento enviado: ${eventSent}`,
            statusCode: code,
            eventSent,
            eventName: eventSent,
            testEventCode: capiOptions.testEventCode,
            hint: 'Verifique no Gerenciador de Eventos do Meta (Test Events) se o evento apareceu.'
        });
    } catch (error) {
        console.error('Test postback error:', error);
        return res.status(500).json({ success: false, error: error.message });
    }
});

// ==================== DEBUG TRANSACTIONS & DIAGNOSTICS ====================

router.get('/api/admin/debug-transactions', authenticateToken, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;
        
        let params = [];
        if (startDate && endDate) {
            params = [startDate, endDate];
        }
        
        const approved = await pool.query(`
            SELECT transaction_id, email, product, value, status, created_at,
                   (created_at AT TIME ZONE 'America/Sao_Paulo') as created_at_brazil
            FROM transactions 
            WHERE status = 'approved' ${startDate ? 'AND (created_at AT TIME ZONE \'America/Sao_Paulo\')::date >= $1::date AND (created_at AT TIME ZONE \'America/Sao_Paulo\')::date <= $2::date' : ''}
            ORDER BY created_at DESC
            LIMIT 50
        `, params);
        
        const sumResult = await pool.query(`
            SELECT 
                COUNT(*) as count,
                COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total_value,
                COALESCE(SUM(CAST(value AS DECIMAL)), 0) as revenue
            FROM transactions 
            WHERE status = 'approved' ${startDate ? 'AND (created_at AT TIME ZONE \'America/Sao_Paulo\')::date >= $1::date AND (created_at AT TIME ZONE \'America/Sao_Paulo\')::date <= $2::date' : ''}
        `, params);
        
        const statuses = await pool.query(`
            SELECT status, COUNT(*) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total
            FROM transactions
            GROUP BY status
        `);
        
        res.json({
            dateFilter: { startDate, endDate },
            summary: sumResult.rows[0],
            statusBreakdown: statuses.rows,
            recentApproved: approved.rows
        });
    } catch (error) {
        console.error('Debug transactions error:', error);
        res.status(500).json({ error: error.message });
    }
});

// DIAGNOSTIC: Complete panel health check
router.get('/api/admin/diagnostic', authenticateToken, async (req, res) => {
    try {
        const today = new Date().toISOString().split('T')[0];
        const brazilNow = `(NOW() AT TIME ZONE 'America/Sao_Paulo')`;
        const brazilToday = `(${brazilNow})::date`;
        
        const leadsTotal = await pool.query(`SELECT COUNT(*) as count FROM leads`);
        const leadsTodayBrazil = await pool.query(`
            SELECT COUNT(*) as count FROM leads 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        const leadsThisWeek = await pool.query(`
            SELECT COUNT(*) as count FROM leads 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= (${brazilNow} - INTERVAL '7 days')::date
        `);
        
        const txTotal = await pool.query(`SELECT COUNT(*) as count FROM transactions`);
        const txApproved = await pool.query(`SELECT COUNT(*) as count FROM transactions WHERE status = 'approved'`);
        const txTodayBrazil = await pool.query(`
            SELECT COUNT(*) as count FROM transactions 
            WHERE status = 'approved' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        const txRevenue = await pool.query(`
            SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved'
        `);
        const txRevenueTodayBrazil = await pool.query(`
            SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions 
            WHERE status = 'approved' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        
        const txByStatus = await pool.query(`
            SELECT status, COUNT(*) as count, COALESCE(SUM(CAST(value AS DECIMAL)), 0) as value
            FROM transactions GROUP BY status ORDER BY count DESC
        `);
        
        const funnelTotal = await pool.query(`SELECT COUNT(*) as count FROM funnel_events`);
        const funnelTodayBrazil = await pool.query(`
            SELECT COUNT(*) as count FROM funnel_events 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        const funnelByEvent = await pool.query(`
            SELECT event, COUNT(*) as count FROM funnel_events 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
            GROUP BY event ORDER BY count DESC
        `);
        const funnelLandingToday = await pool.query(`
            SELECT COUNT(DISTINCT visitor_id) as count FROM funnel_events 
            WHERE event = 'page_view_landing' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        
        const refundsTotal = await pool.query(`SELECT COUNT(*) as count FROM refund_requests`);
        const refundsTodayBrazil = await pool.query(`
            SELECT COUNT(*) as count FROM refund_requests 
            WHERE (created_at AT TIME ZONE 'America/Sao_Paulo')::date = ${brazilToday}
        `);
        
        const serverTime = await pool.query(`SELECT NOW() as utc, NOW() AT TIME ZONE 'America/Sao_Paulo' as brazil`);
        
        const recentTx = await pool.query(`
            SELECT transaction_id, email, status, value, 
                   created_at as utc_time,
                   (created_at AT TIME ZONE 'America/Sao_Paulo') as brazil_time,
                   (created_at AT TIME ZONE 'America/Sao_Paulo')::date as brazil_date
            FROM transactions ORDER BY created_at DESC LIMIT 5
        `);
        
        const recentLeads = await pool.query(`
            SELECT id, email, 
                   created_at as utc_time,
                   (created_at AT TIME ZONE 'America/Sao_Paulo') as brazil_time,
                   (created_at AT TIME ZONE 'America/Sao_Paulo')::date as brazil_date
            FROM leads ORDER BY created_at DESC LIMIT 5
        `);
        
        res.json({
            serverInfo: {
                utcTime: serverTime.rows[0].utc,
                brazilTime: serverTime.rows[0].brazil,
                todayBrazil: today
            },
            leads: {
                total: parseInt(leadsTotal.rows[0].count),
                todayBrazil: parseInt(leadsTodayBrazil.rows[0].count),
                thisWeek: parseInt(leadsThisWeek.rows[0].count),
                recent: recentLeads.rows
            },
            transactions: {
                total: parseInt(txTotal.rows[0].count),
                approved: parseInt(txApproved.rows[0].count),
                todayApproved: parseInt(txTodayBrazil.rows[0].count),
                totalRevenue: parseFloat(txRevenue.rows[0].total),
                todayRevenue: parseFloat(txRevenueTodayBrazil.rows[0].total),
                byStatus: txByStatus.rows,
                recent: recentTx.rows
            },
            funnel: {
                totalEvents: parseInt(funnelTotal.rows[0].count),
                todayEvents: parseInt(funnelTodayBrazil.rows[0].count),
                todayLandingVisitors: parseInt(funnelLandingToday.rows[0].count),
                todayByEvent: funnelByEvent.rows
            },
            refunds: {
                total: parseInt(refundsTotal.rows[0].count),
                today: parseInt(refundsTodayBrazil.rows[0].count)
            },
            consistency: {
                message: 'Compare these numbers with what the panel shows. They should match.',
                tips: [
                    'If leads.todayBrazil != panel leads today, timezone issue remains',
                    'If transactions.todayApproved != panel sales today, timezone issue remains',
                    'If funnel.todayLandingVisitors is way higher than leads, check for old/test data'
                ]
            }
        });
    } catch (error) {
        console.error('Diagnostic error:', error);
        res.status(500).json({ error: error.message });
    }
});

// DATA CLEANUP: Find corrupted data
router.get('/api/admin/diagnostic/corrupted', authenticateToken, async (req, res) => {
    try {
        const issues = [];
        
        const futureTx = await pool.query(`
            SELECT transaction_id, email, status, value, created_at,
                   (created_at AT TIME ZONE 'America/Sao_Paulo') as brazil_time
            FROM transactions 
            WHERE created_at > NOW() + INTERVAL '1 day'
            ORDER BY created_at DESC
        `);
        
        if (futureTx.rows.length > 0) {
            issues.push({
                type: 'transactions_future_dates',
                severity: 'high',
                count: futureTx.rows.length,
                description: 'Transações com datas no futuro (corrompidas)',
                data: futureTx.rows,
                fix_action: 'delete_or_fix_date'
            });
        }
        
        const invalidIdTx = await pool.query(`
            SELECT transaction_id, email, status, value, created_at
            FROM transactions 
            WHERE transaction_id !~ '^[0-9]+$' 
            OR LENGTH(transaction_id) > 20
            OR value IS NULL 
            OR value = '0'
        `);
        
        if (invalidIdTx.rows.length > 0) {
            issues.push({
                type: 'transactions_invalid_id_or_value',
                severity: 'medium',
                count: invalidIdTx.rows.length,
                description: 'Transações com ID inválido ou valor nulo/zero',
                data: invalidIdTx.rows,
                fix_action: 'review_and_delete'
            });
        }
        
        const duplicateTx = await pool.query(`
            SELECT transaction_id, COUNT(*) as count
            FROM transactions 
            GROUP BY transaction_id 
            HAVING COUNT(*) > 1
        `);
        
        if (duplicateTx.rows.length > 0) {
            issues.push({
                type: 'transactions_duplicates',
                severity: 'medium',
                count: duplicateTx.rows.length,
                description: 'Transaction IDs duplicados',
                data: duplicateTx.rows,
                fix_action: 'remove_duplicates'
            });
        }
        
        const invalidLeads = await pool.query(`
            SELECT id, email, created_at
            FROM leads 
            WHERE email IS NULL 
            OR email = ''
            OR email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
            LIMIT 20
        `);
        
        if (invalidLeads.rows.length > 0) {
            issues.push({
                type: 'leads_invalid_email',
                severity: 'low',
                count: invalidLeads.rows.length,
                description: 'Leads com email inválido ou vazio',
                data: invalidLeads.rows,
                fix_action: 'review'
            });
        }
        
        const oldEvents = await pool.query(`
            SELECT COUNT(*) as count, MIN(created_at) as oldest
            FROM funnel_events 
            WHERE created_at < '2025-01-01'
        `);
        
        if (parseInt(oldEvents.rows[0].count) > 0) {
            issues.push({
                type: 'funnel_very_old_events',
                severity: 'low',
                count: parseInt(oldEvents.rows[0].count),
                description: 'Eventos do funil muito antigos (antes de 2025)',
                data: { oldest: oldEvents.rows[0].oldest },
                fix_action: 'review_and_delete'
            });
        }
        
        res.json({
            summary: {
                totalIssues: issues.length,
                highSeverity: issues.filter(i => i.severity === 'high').length,
                mediumSeverity: issues.filter(i => i.severity === 'medium').length,
                lowSeverity: issues.filter(i => i.severity === 'low').length
            },
            issues,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('Corrupted data check error:', error);
        res.status(500).json({ error: error.message });
    }
});

// DATA CLEANUP: Delete corrupted transactions
router.post('/api/admin/diagnostic/fix-corrupted', authenticateToken, async (req, res) => {
    try {
        const { action, transactionIds } = req.body;
        let result = { deleted: 0, fixed: 0, details: [] };
        
        if (action === 'delete_future_dates') {
            const deleted = await pool.query(`
                DELETE FROM transactions 
                WHERE created_at > NOW() + INTERVAL '1 day'
                RETURNING transaction_id, email
            `);
            result.deleted = deleted.rowCount;
            result.details = deleted.rows;
        }
        
        if (action === 'delete_invalid') {
            const deleted = await pool.query(`
                DELETE FROM transactions 
                WHERE (transaction_id !~ '^[0-9]+$' OR LENGTH(transaction_id) > 20)
                AND status != 'approved'
                RETURNING transaction_id, email
            `);
            result.deleted = deleted.rowCount;
            result.details = deleted.rows;
        }
        
        if (action === 'delete_specific' && transactionIds && transactionIds.length > 0) {
            const deleted = await pool.query(`
                DELETE FROM transactions 
                WHERE transaction_id = ANY($1)
                RETURNING transaction_id, email
            `, [transactionIds]);
            result.deleted = deleted.rowCount;
            result.details = deleted.rows;
        }
        
        if (action === 'remove_duplicates') {
            const deleted = await pool.query(`
                DELETE FROM transactions a
                USING transactions b
                WHERE a.transaction_id = b.transaction_id 
                AND a.created_at < b.created_at
                RETURNING a.transaction_id
            `);
            result.deleted = deleted.rowCount;
        }
        
        console.log(`🧹 Cleanup action "${action}": ${result.deleted} deleted, ${result.fixed} fixed`);
        
        res.json({
            success: true,
            action,
            result,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('Fix corrupted error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Fix leads status
router.post('/api/admin/fix-leads-status', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query(`
            UPDATE leads 
            SET status = 'converted',
                updated_at = NOW(),
                notes = COALESCE(notes, '') || E'\n[Auto-fix] Status corrigido para converted - ' || NOW()::text
            WHERE status != 'converted'
            AND LOWER(email) IN (
                SELECT DISTINCT LOWER(email) 
                FROM transactions 
                WHERE status = 'approved'
            )
            RETURNING id, email, status
        `);
        
        console.log(`✅ Fixed ${result.rowCount} leads with incorrect status`);
        
        const stats = await pool.query(`
            SELECT status, COUNT(*) as count 
            FROM leads 
            GROUP BY status
            ORDER BY count DESC
        `);
        
        res.json({ 
            success: true, 
            message: `${result.rowCount} leads corrigidos para 'converted'`,
            fixedLeads: result.rows.map(l => ({ id: l.id, email: l.email })),
            currentStats: stats.rows
        });
    } catch (error) {
        console.error('Error fixing leads status:', error);
        res.status(500).json({ error: 'Failed to fix leads status', details: error.message });
    }
});

// Debug endpoint: Customer journey
router.get('/api/admin/debug/customer-journey', authenticateToken, async (req, res) => {
    try {
        const { email, visitor_id } = req.query;
        if (!email && !visitor_id) {
            return res.status(400).json({ error: 'Provide email or visitor_id query parameter' });
        }
        
        let lead = null;
        let vid = visitor_id;
        if (email) {
            const leadResult = await pool.query(
                `SELECT * FROM leads WHERE LOWER(email) = LOWER($1) ORDER BY created_at DESC LIMIT 1`,
                [email]
            );
            if (leadResult.rows.length > 0) {
                lead = leadResult.rows[0];
                vid = lead.visitor_id;
            }
        }
        
        const events = vid ? await pool.query(
            `SELECT event, page, created_at, metadata FROM funnel_events WHERE visitor_id = $1 ORDER BY created_at ASC`,
            [vid]
        ) : { rows: [] };
        
        const transactions = email ? await pool.query(
            `SELECT transaction_id, product, value, status, monetizze_status, funnel_language, funnel_source, created_at FROM transactions WHERE LOWER(email) = LOWER($1) ORDER BY created_at ASC`,
            [email]
        ) : (lead ? await pool.query(
            `SELECT transaction_id, product, value, status, monetizze_status, funnel_language, funnel_source, created_at FROM transactions WHERE LOWER(email) = LOWER($1) ORDER BY created_at ASC`,
            [lead.email]
        ) : { rows: [] });
        
        const capiLogs = email ? await pool.query(
            `SELECT * FROM capi_purchase_logs WHERE LOWER(email) = LOWER($1) ORDER BY created_at ASC`,
            [email]
        ) : { rows: [] };
        
        res.json({
            lead: lead ? { id: lead.id, email: lead.email, name: lead.name, visitor_id: lead.visitor_id, status: lead.status, funnel_language: lead.funnel_language, products_purchased: lead.products_purchased, total_spent: lead.total_spent, created_at: lead.created_at } : null,
            visitor_id: vid,
            funnel_events: events.rows,
            transactions: transactions.rows,
            capi_purchase_logs: capiLogs.rows,
            summary: {
                total_events: events.rows.length,
                total_transactions: transactions.rows.length,
                upsell_events: events.rows.filter(e => e.event.includes('upsell')).map(e => ({ event: e.event, page: e.page, time: e.created_at }))
            }
        });
    } catch (error) {
        console.error('Customer journey error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Debug endpoint: Upsell tracking analysis
router.get('/api/admin/debug/upsell-tracking', authenticateToken, async (req, res) => {
    try {
        const upsellEvents = await pool.query(`
            SELECT event, COUNT(*) as count
            FROM funnel_events
            WHERE event LIKE '%upsell%'
            GROUP BY event
            ORDER BY count DESC
        `);
        
        const orphanedEvents = await pool.query(`
            SELECT fe.visitor_id, COUNT(*) as event_count, 
                   array_agg(DISTINCT fe.event) as events,
                   MIN(fe.created_at) as first_event,
                   MAX(fe.created_at) as last_event
            FROM funnel_events fe
            LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
            WHERE fe.event LIKE '%upsell%' AND l.id IS NULL
            GROUP BY fe.visitor_id
            ORDER BY last_event DESC
            LIMIT 50
        `);
        
        const linkedEvents = await pool.query(`
            SELECT l.id as lead_id, l.email, l.name, fe.visitor_id,
                   COUNT(*) as event_count,
                   array_agg(DISTINCT fe.event) as events
            FROM funnel_events fe
            INNER JOIN leads l ON fe.visitor_id = l.visitor_id
            WHERE fe.event LIKE '%upsell%'
            GROUP BY l.id, l.email, l.name, fe.visitor_id
            ORDER BY MAX(fe.created_at) DESC
            LIMIT 50
        `);
        
        const recentUpsellEvents = await pool.query(`
            SELECT fe.*, 
                   l.email as lead_email, 
                   l.name as lead_name
            FROM funnel_events fe
            LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
            WHERE fe.event LIKE '%upsell%'
            ORDER BY fe.created_at DESC
            LIMIT 100
        `);
        
        const transactionsWithoutLeadLink = await pool.query(`
            SELECT t.email, t.product, t.status, t.value, t.created_at,
                   l.visitor_id as lead_visitor_id,
                   (SELECT COUNT(*) FROM funnel_events fe WHERE fe.visitor_id = l.visitor_id AND fe.event LIKE '%upsell%') as upsell_events_count
            FROM transactions t
            LEFT JOIN leads l ON LOWER(t.email) = LOWER(l.email)
            WHERE t.status = 'approved'
            AND (t.product ILIKE '%recovery%' OR t.product ILIKE '%vault%' OR t.product ILIKE '%vision%' OR t.product ILIKE '%vip%')
            ORDER BY t.created_at DESC
            LIMIT 50
        `);
        
        res.json({
            summary: {
                totalUpsellEventTypes: upsellEvents.rows.length,
                orphanedVisitors: orphanedEvents.rows.length,
                linkedVisitors: linkedEvents.rows.length
            },
            eventBreakdown: upsellEvents.rows,
            orphanedEvents: orphanedEvents.rows,
            linkedEvents: linkedEvents.rows,
            recentEvents: recentUpsellEvents.rows,
            upsellTransactionsLinkStatus: transactionsWithoutLeadLink.rows
        });
        
    } catch (error) {
        console.error('Error in upsell tracking debug:', error);
        res.status(500).json({ error: error.message });
    }
});

// Debug endpoint: Find customer journey by email
router.get('/api/admin/debug/journey-by-email/:email', authenticateToken, async (req, res) => {
    try {
        const { email } = req.params;
        
        const leads = await pool.query(`
            SELECT * FROM leads WHERE LOWER(email) = LOWER($1)
        `, [email]);
        
        const transactions = await pool.query(`
            SELECT * FROM transactions WHERE LOWER(email) = LOWER($1) ORDER BY created_at
        `, [email]);
        
        const allEvents = [];
        for (const lead of leads.rows) {
            if (lead.visitor_id) {
                const events = await pool.query(`
                    SELECT * FROM funnel_events WHERE visitor_id = $1 ORDER BY created_at
                `, [lead.visitor_id]);
                allEvents.push({
                    lead_id: lead.id,
                    visitor_id: lead.visitor_id,
                    events: events.rows
                });
            }
        }
        
        if (transactions.rows.length > 0) {
            const firstTxTime = transactions.rows[0].created_at;
            const potentialEvents = await pool.query(`
                SELECT fe.*, l.email as potential_lead_email
                FROM funnel_events fe
                LEFT JOIN leads l ON fe.visitor_id = l.visitor_id
                WHERE fe.event LIKE '%upsell%'
                AND fe.created_at BETWEEN $1::timestamp - INTERVAL '30 minutes' AND $1::timestamp + INTERVAL '30 minutes'
                ORDER BY fe.created_at
            `, [firstTxTime]);
            
            allEvents.push({
                context: 'potential_matches_by_time',
                events: potentialEvents.rows
            });
        }
        
        res.json({
            email: email,
            leads: leads.rows,
            transactions: transactions.rows,
            eventsByLead: allEvents
        });
        
    } catch (error) {
        console.error('Error in journey by email debug:', error);
        res.status(500).json({ error: error.message });
    }
});

module.exports = router;
