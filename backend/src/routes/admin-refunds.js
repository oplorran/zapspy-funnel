const express = require('express');
const router = express.Router();
const pool = require('../database');
const { authenticateToken, requireAdmin } = require('../middleware');
const { ZAPI_BASE_URL, ZAPI_CLIENT_TOKEN } = require('../config');
const { buildDateFilter, parseMonetizzeDate } = require('../helpers');
const { sendToFacebookCAPI, sendMissingCAPIPurchases } = require('../services/facebook-capi');

// In-memory postback storage (shared within this module)
const recentPostbacks = [];

// ==================== REFUND DIAGNOSTIC ====================

router.get('/api/admin/refund-diagnostic', authenticateToken, async (req, res) => {
    try {
        // 1. Count transactions by status
        const txStatusCounts = await pool.query(`
            SELECT status, COUNT(*) as count FROM transactions GROUP BY status ORDER BY count DESC
        `);
        
        // 2. Count refund_requests by type and source
        const refundCounts = await pool.query(`
            SELECT refund_type, source, status, COUNT(*) as count 
            FROM refund_requests GROUP BY refund_type, source, status ORDER BY count DESC
        `);
        
        // 3. Get sample of refunded/chargeback transactions (raw_data included)
        const refundedTx = await pool.query(`
            SELECT transaction_id, email, name, product, value, status, monetizze_status, 
                   funnel_language, created_at,
                   raw_data->>'venda' as venda_json,
                   raw_data->'tipoEvento' as tipo_evento,
                   raw_data->'venda'->>'status' as venda_status_text
            FROM transactions 
            WHERE status IN ('refunded', 'chargeback')
            ORDER BY created_at DESC LIMIT 20
        `);
        
        // 4. Find orphaned refunds (in transactions but not in refund_requests)
        const orphans = await pool.query(`
            SELECT t.transaction_id, t.email, t.status, t.product, t.value, t.created_at
            FROM transactions t
            LEFT JOIN refund_requests rr ON rr.transaction_id = t.transaction_id
            WHERE t.status IN ('refunded', 'chargeback') AND rr.id IS NULL
        `);
        
        // 5. Check if there are transactions with "devolvida" in raw_data that are NOT marked as refunded
        const missedRefunds = await pool.query(`
            SELECT transaction_id, email, status, product, value,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao
            FROM transactions 
            WHERE status NOT IN ('refunded', 'chargeback')
              AND (
                  LOWER(raw_data->'venda'->>'status') LIKE '%devolv%'
                  OR LOWER(raw_data->'venda'->>'status') LIKE '%chargeback%'
                  OR LOWER(raw_data->'venda'->>'status') LIKE '%reembolso%'
                  OR LOWER(raw_data->'tipoEvento'->>'descricao') LIKE '%devolv%'
                  OR LOWER(raw_data->'tipoEvento'->>'descricao') LIKE '%chargeback%'
                  OR raw_data->'tipoEvento'->>'codigo' IN ('4', '8', '9')
              )
            LIMIT 20
        `);
        
        // 6. Get total refund_requests
        const totalRefundRequests = await pool.query('SELECT COUNT(*) FROM refund_requests');
        
        // 7. Sample of CANCELLED transactions to see if refunds are hiding as cancellations
        const cancelledSample = await pool.query(`
            SELECT transaction_id, email, status, monetizze_status, product, value,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao,
                   raw_data->'venda'->>'dataFinalizada' as data_finalizada,
                   created_at
            FROM transactions 
            WHERE status = 'cancelled'
            ORDER BY created_at DESC LIMIT 10
        `);
        
        // 8. Search ALL raw_data for any mention of refund keywords (broader search)
        const broadSearch = await pool.query(`
            SELECT transaction_id, email, status, product, value,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao,
                   monetizze_status
            FROM transactions 
            WHERE LOWER(CAST(raw_data AS text)) LIKE '%devolv%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%chargeback%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%reembolso%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%disputa%'
               OR LOWER(CAST(raw_data AS text)) LIKE '%refund%'
               OR monetizze_status IN ('4', '8', '9')
            LIMIT 20
        `);
        
        // 9. Get distinct vendaStatus values to see all possible status texts
        const distinctStatuses = await pool.query(`
            SELECT raw_data->'venda'->>'status' as venda_status, 
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo,
                   raw_data->'tipoEvento'->>'descricao' as evento_descricao,
                   COUNT(*) as count
            FROM transactions
            WHERE raw_data IS NOT NULL
            GROUP BY raw_data->'venda'->>'status', raw_data->'tipoEvento'->>'codigo', raw_data->'tipoEvento'->>'descricao'
            ORDER BY count DESC
        `);
        
        // 10. Check specific transaction IDs from Monetizze (the ones shown in the screenshot)
        const specificCheck = await pool.query(`
            SELECT transaction_id, email, status, monetizze_status, product, value,
                   raw_data->'venda'->>'status' as venda_status_text,
                   raw_data->'tipoEvento'->>'codigo' as evento_codigo
            FROM transactions
            WHERE transaction_id LIKE '5580%' OR transaction_id LIKE '5579%' 
               OR transaction_id LIKE '5578%' OR transaction_id LIKE '5577%' OR transaction_id LIKE '5576%'
            ORDER BY created_at DESC LIMIT 20
        `);
        
        res.json({
            transactions_by_status: txStatusCounts.rows,
            refund_requests_summary: refundCounts.rows,
            total_refund_requests: parseInt(totalRefundRequests.rows[0].count),
            refunded_transactions: refundedTx.rows,
            orphaned_refunds: orphans.rows,
            missed_refunds_in_transactions: missedRefunds.rows,
            cancelled_sample: cancelledSample.rows,
            broad_refund_search: broadSearch.rows,
            distinct_api_statuses: distinctStatuses.rows,
            specific_monetizze_ids: specificCheck.rows,
            diagnostic_note: 'cancelled_sample shows recent cancelled transactions raw data. broad_refund_search finds any refund keywords in raw_data. distinct_api_statuses shows all unique status combinations from Monetizze API. specific_monetizze_ids checks for the transaction IDs visible in Monetizze dashboard.'
        });
    } catch (error) {
        console.error('Diagnostic error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Backfill: sync existing transactions with refunded/chargeback status to refund_requests table
router.post('/api/admin/backfill-refunds', authenticateToken, requireAdmin, async (req, res) => {
    try {
        console.log('ðŸ”„ Starting refund backfill...');
        
        // Find all transactions with refunded/chargeback status that DON'T have a refund_requests entry
        const result = await pool.query(`
            SELECT t.transaction_id, t.email, t.phone, t.name, t.product, t.value, 
                   t.status, t.funnel_language, t.created_at
            FROM transactions t
            LEFT JOIN refund_requests rr ON rr.transaction_id = t.transaction_id
            WHERE t.status IN ('refunded', 'chargeback')
              AND rr.id IS NULL
            ORDER BY t.created_at DESC
        `);
        
        const missing = result.rows;
        console.log(`ðŸ“¦ Found ${missing.length} transactions without refund_requests entries`);
        
        let created = 0;
        let errors = 0;
        
        for (const tx of missing) {
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
                    refundProtocol,
                    tx.name || 'N/A',
                    tx.email,
                    tx.phone || null,
                    tx.product,
                    refundType === 'chargeback' ? 'Chargeback - Disputa de cartÃ£o' : 'Reembolso via Monetizze',
                    'approved',
                    'monetizze',
                    refundType,
                    String(tx.transaction_id),
                    parseFloat(tx.value) || 0,
                    tx.funnel_language || 'en',
                    tx.created_at || new Date()
                ]);
                
                created++;
                console.log(`ðŸ“¥ Backfill: ${refundType.toUpperCase()} - ${refundProtocol} - ${tx.email}`);
            } catch (err) {
                errors++;
                console.error(`âš ï¸ Backfill error for ${tx.transaction_id}: ${err.message}`);
            }
        }
        
        console.log(`âœ… Backfill complete: ${created} created, ${errors} errors, ${missing.length} total found`);
        
        res.json({
            success: true,
            message: `Backfill complete: ${created} refund_requests created`,
            found: missing.length,
            created,
            errors
        });
    } catch (error) {
        console.error('âŒ Backfill error:', error);
        res.status(500).json({ error: 'Backfill failed', message: error.message });
    }
});


// ==================== REFUND REQUESTS API ====================

// Get all refund requests (protected)
router.get('/api/admin/refunds', authenticateToken, async (req, res) => {
    try {
        // Step 1: Fix transaction statuses that were overwritten by auto-sync
        try {
            const fixResult = await pool.query(`
                UPDATE transactions 
                SET status = CASE 
                    WHEN monetizze_status IN ('8', '9') THEN 'chargeback'
                    WHEN monetizze_status = '4' THEN 'refunded'
                END,
                updated_at = NOW()
                WHERE monetizze_status IN ('4', '8', '9')
                  AND status NOT IN ('refunded', 'chargeback')
                RETURNING transaction_id, email, status
            `);
            if (fixResult.rows.length > 0) {
                console.log(`Status fix: ${fixResult.rows.length} transactions corrected from monetizze_status`);
            }
            
            // Also check raw_data for chargeback/refund keywords
            const fixRawResult = await pool.query(`
                UPDATE transactions 
                SET status = CASE 
                    WHEN raw_data::text ILIKE '%chargeback%' OR raw_data::text ILIKE '%disputa%' THEN 'chargeback'
                    ELSE 'refunded'
                END,
                updated_at = NOW()
                WHERE status NOT IN ('refunded', 'chargeback')
                  AND (
                    raw_data::text ILIKE '%chargeback%' 
                    OR raw_data::text ILIKE '%disputa%'
                    OR (raw_data::text ILIKE '%devolvida%' AND raw_data::text ILIKE '%reembolso%')
                  )
                RETURNING transaction_id, email, status
            `);
            if (fixRawResult.rows.length > 0) {
                console.log(`Raw data fix: ${fixRawResult.rows.length} transactions corrected from raw_data`);
            }
        } catch (fixErr) {
            console.error('Status fix error (non-blocking):', fixErr.message);
        }
        
        // Step 2: Create refund_requests for transactions with refunded/chargeback status
        try {
            const backfillResult = await pool.query(`
                SELECT t.transaction_id, t.email, t.phone, t.name, t.product, t.value, 
                       t.status, t.funnel_language, t.created_at
                FROM transactions t
                LEFT JOIN refund_requests rr ON rr.transaction_id = t.transaction_id
                WHERE t.status IN ('refunded', 'chargeback')
                  AND rr.id IS NULL
            `);
            
            for (const tx of backfillResult.rows) {
                const refundProtocol = `MON-${String(tx.transaction_id).substring(0, 12).toUpperCase()}`;
                const refundType = tx.status === 'chargeback' ? 'chargeback' : 'refund';
                const pLower = (tx.product || '').toLowerCase();
                const lang = tx.funnel_language || (pLower.includes('espanhol') || pLower.includes('recuperaci') || pLower.includes('infidelidad') ? 'es' : 'en');
                await pool.query(`
                    INSERT INTO refund_requests (
                        protocol, full_name, email, phone, product, reason, 
                        status, source, refund_type, transaction_id, value, funnel_language, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (protocol) DO NOTHING
                `, [
                    refundProtocol,
                    tx.name || 'N/A',
                    tx.email,
                    tx.phone || null,
                    tx.product,
                    refundType === 'chargeback' ? 'Chargeback - Disputa de cartão' : 'Reembolso via Monetizze',
                    'approved',
                    'monetizze',
                    refundType,
                    String(tx.transaction_id),
                    parseFloat(tx.value) || 0,
                    lang,
                    tx.created_at || new Date()
                ]);
            }
            if (backfillResult.rows.length > 0) {
                console.log(`Auto-backfill: ${backfillResult.rows.length} refund_requests created`);
            }
        } catch (bfErr) {
            console.error('Auto-backfill error (non-blocking):', bfErr.message);
        }
        
        const { status, source, type, language, startDate, endDate } = req.query;
        
        let conditions = [];
        let params = [];
        let paramIndex = 1;
        
        if (status) {
            conditions.push(`status = $${paramIndex++}`);
            params.push(status);
        }
        if (source) {
            conditions.push(`source = $${paramIndex++}`);
            params.push(source);
        }
        if (type) {
            conditions.push(`refund_type = $${paramIndex++}`);
            params.push(type);
        }
        if (language) {
            conditions.push(`funnel_language = $${paramIndex++}`);
            params.push(language);
        }
        if (startDate && endDate) {
            conditions.push(`(created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${paramIndex++}::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${paramIndex++}::date`);
            params.push(startDate, endDate);
        }
        
        const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
        
        const result = await pool.query(`
            SELECT *
            FROM refund_requests 
            ${whereClause}
            ORDER BY created_at DESC
        `, params);
        
        let statsConditions = [];
        let statsParams = [];
        let statsParamIndex = 1;
        
        if (startDate && endDate) {
            statsConditions.push(`(created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${statsParamIndex++}::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${statsParamIndex++}::date`);
            statsParams.push(startDate, endDate);
        }
        
        const statsWhereClause = statsConditions.length > 0 ? `WHERE ${statsConditions.join(' AND ')}` : '';
        
        const statsResult = await pool.query(`
            SELECT 
                COALESCE(source, 'form') as source,
                COALESCE(refund_type, 'refund') as refund_type,
                COALESCE(funnel_language, 'unknown') as funnel_language,
                COUNT(*) as count,
                status
            FROM refund_requests
            ${statsWhereClause}
            GROUP BY source, refund_type, funnel_language, status
        `, statsParams);
        
        const stats = {
            form: { pending: 0, handling: 0, processing: 0, convinced: 0, refunded: 0, approved: 0, rejected: 0, total: 0, en: 0, es: 0 },
            monetizze_refund: { pending: 0, handling: 0, processing: 0, convinced: 0, refunded: 0, approved: 0, rejected: 0, total: 0, en: 0, es: 0 },
            monetizze_chargeback: { pending: 0, handling: 0, processing: 0, convinced: 0, refunded: 0, approved: 0, rejected: 0, total: 0, en: 0, es: 0 }
        };
        
        statsResult.rows.forEach(row => {
            const key = row.source === 'monetizze' 
                ? `monetizze_${row.refund_type || 'refund'}`
                : 'form';
            
            if (stats[key]) {
                stats[key][row.status] = (stats[key][row.status] || 0) + parseInt(row.count);
                stats[key].total += parseInt(row.count);
                
                if (row.funnel_language === 'en' || row.funnel_language === 'es') {
                    stats[key][row.funnel_language] = (stats[key][row.funnel_language] || 0) + parseInt(row.count);
                }
            }
        });

        res.json({ 
            refunds: result.rows, 
            total: result.rows.length,
            stats
        });

    } catch (error) {
        console.error('Error fetching refunds:', error);
        res.status(500).json({ error: 'Failed to fetch refund requests' });
    }
});

// Get enriched refund details
router.get('/api/admin/refunds/:id/details', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        const refundResult = await pool.query('SELECT * FROM refund_requests WHERE id = $1', [id]);
        if (refundResult.rows.length === 0) {
            return res.status(404).json({ error: 'Refund not found' });
        }
        
        const refund = refundResult.rows[0];
        const email = refund.email;
        
        let leadData = null;
        if (email) {
            const leadResult = await pool.query(`
                SELECT id, email, name, whatsapp as phone, country, status, source,
                    products_purchased, total_spent, first_purchase_at, last_purchase_at,
                    funnel_language, created_at
                FROM leads 
                WHERE LOWER(email) = LOWER($1)
                ORDER BY created_at DESC LIMIT 1
            `, [email]);
            
            if (leadResult.rows.length > 0) {
                leadData = leadResult.rows[0];
            }
        }
        
        let transactions = [];
        if (email) {
            const txResult = await pool.query(`
                SELECT transaction_id, product, value, status, monetizze_status, 
                    funnel_language, created_at
                FROM transactions 
                WHERE LOWER(email) = LOWER($1)
                ORDER BY created_at DESC
            `, [email]);
            transactions = txResult.rows;
        }
        
        let funnelEvents = [];
        if (email) {
            try {
                const eventsResult = await pool.query(`
                    SELECT event, page, metadata, created_at
                    FROM funnel_events 
                    WHERE LOWER(metadata->>'email') = LOWER($1)
                    ORDER BY created_at ASC
                    LIMIT 50
                `, [email]);
                funnelEvents = eventsResult.rows;
            } catch (evErr) {
                console.error('Funnel events query error (non-blocking):', evErr.message);
            }
        }
        
        res.json({
            success: true,
            refund,
            crossReference: {
                lead: leadData,
                transactions,
                funnelEvents,
                summary: {
                    totalTransactions: transactions.length,
                    approvedTransactions: transactions.filter(t => t.status === 'approved').length,
                    totalSpent: transactions.filter(t => t.status === 'approved').reduce((sum, t) => sum + parseFloat(t.value || 0), 0),
                    productsBought: [...new Set(transactions.filter(t => t.status === 'approved').map(t => t.product))],
                    funnelSteps: funnelEvents.length,
                    detectedLanguage: transactions[0]?.funnel_language || leadData?.funnel_language || null
                }
            }
        });
        
    } catch (error) {
        console.error('Error fetching refund details:', error);
        res.status(500).json({ error: 'Failed to fetch refund details' });
    }
});

// Update refund status
router.put('/api/admin/refunds/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { status, notes } = req.body;
        
        const validStatuses = ['pending', 'handling', 'processing', 'convinced', 'refunded', 'approved', 'rejected'];
        if (status && !validStatuses.includes(status)) {
            return res.status(400).json({ error: 'Invalid status' });
        }

        const result = await pool.query(`
            UPDATE refund_requests 
            SET status = $1, admin_notes = $2, updated_at = NOW()
            WHERE id = $3
            RETURNING *
        `, [status, notes, id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Refund request not found' });
        }
        
        console.log(`ðŸ“ Refund ${id} status updated to: ${status}`);

        res.json({ success: true, refund: result.rows[0] });

    } catch (error) {
        console.error('Error updating refund:', error);
        res.status(500).json({ error: 'Failed to update refund request' });
    }
});

// Send refund communication via Z-API
router.post('/api/admin/refunds/:id/send-message', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { channel, template_key, message, phone, email } = req.body;
        
        if (!message) return res.status(400).json({ error: 'Mensagem Ã© obrigatÃ³ria' });
        
        const refundResult = await pool.query('SELECT * FROM refund_requests WHERE id = $1', [id]);
        if (refundResult.rows.length === 0) return res.status(404).json({ error: 'Reembolso nÃ£o encontrado' });
        const refund = refundResult.rows[0];
        
        let sent = false;
        let messageId = null;
        let sendError = null;
        
        if (channel === 'whatsapp') {
            const cleanPhone = (phone || refund.phone || '').replace(/\D/g, '');
            if (!cleanPhone || cleanPhone.length < 10) {
                return res.status(400).json({ error: 'NÃºmero de telefone invÃ¡lido' });
            }
            
            const zapiHeaders = { 'Content-Type': 'application/json' };
            if (ZAPI_CLIENT_TOKEN) zapiHeaders['Client-Token'] = ZAPI_CLIENT_TOKEN;
            
            try {
                const zapiResponse = await fetch(`${ZAPI_BASE_URL}/send-text`, {
                    method: 'POST',
                    headers: zapiHeaders,
                    body: JSON.stringify({ phone: cleanPhone, message, delayMessage: 3 })
                });
                const zapiData = await zapiResponse.json();
                sent = zapiResponse.ok && !!zapiData.messageId;
                messageId = zapiData.messageId;
                if (!sent) sendError = zapiData.error || zapiData.message || 'Falha Z-API';
                
                if (sent) {
                    try {
                        await pool.query(`
                            INSERT INTO whatsapp_messages (phone, message, message_id, zaap_id, status, sent_by, created_at)
                            VALUES ($1, $2, $3, $4, 'sent', 'refund_comm', NOW())
                        `, [cleanPhone, message, zapiData.messageId, zapiData.zaapId]);
                    } catch (dbErr) { /* ignore */ }
                }
            } catch (fetchErr) {
                sendError = fetchErr.message;
            }
        } else {
            sent = true;
        }
        
        let notes = [];
        if (refund.notes && typeof refund.notes === 'object') {
            notes = Array.isArray(refund.notes) ? refund.notes : [];
        }
        notes.push({
            id: Date.now(),
            date: new Date().toISOString(),
            action: channel === 'whatsapp' ? 'whatsapp' : 'email',
            note: `[${template_key || 'custom'}] ${message.substring(0, 100)}...`,
            user: 'Admin',
            sent: sent,
            messageId
        });
        
        await pool.query(`UPDATE refund_requests SET notes = $1, updated_at = NOW() WHERE id = $2`, [JSON.stringify(notes), id]);
        
        if (refund.status === 'pending') {
            await pool.query(`UPDATE refund_requests SET status = 'handling' WHERE id = $1`, [id]);
        }
        
        if (sent) {
            console.log(`ðŸ“¨ Refund ${id} - ${channel} sent (${template_key})`);
            res.json({ success: true, messageId, channel });
        } else {
            console.error(`âŒ Refund ${id} - ${channel} failed:`, sendError);
            res.status(500).json({ error: sendError || 'Falha no envio' });
        }
    } catch (error) {
        console.error('Error sending refund message:', error);
        res.status(500).json({ error: 'Falha ao enviar: ' + error.message });
    }
});

// Get refund communication history
router.get('/api/admin/refunds/:id/history', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const result = await pool.query('SELECT notes, full_name, email, phone, protocol, status FROM refund_requests WHERE id = $1', [id]);
        if (result.rows.length === 0) return res.status(404).json({ error: 'Reembolso nÃ£o encontrado' });
        
        const refund = result.rows[0];
        let notes = [];
        if (refund.notes && typeof refund.notes === 'object') {
            notes = Array.isArray(refund.notes) ? refund.notes : [];
        }
        
        const communications = notes.filter(n => ['whatsapp', 'email', 'call'].includes(n.action));
        res.json({ success: true, communications, total: communications.length, refund: { name: refund.full_name, email: refund.email, phone: refund.phone, protocol: refund.protocol, status: refund.status } });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Add note to refund request
router.post('/api/admin/refunds/:id/notes', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        const { action, note } = req.body;
        
        if (!action || !note) {
            return res.status(400).json({ error: 'Action and note are required' });
        }
        
        const refundResult = await pool.query('SELECT * FROM refund_requests WHERE id = $1', [id]);
        if (refundResult.rows.length === 0) {
            return res.status(404).json({ error: 'Refund not found' });
        }
        
        const refund = refundResult.rows[0];
        
        let notes = [];
        if (refund.notes && typeof refund.notes === 'object') {
            notes = Array.isArray(refund.notes) ? refund.notes : [];
        }
        
        const newNote = {
            id: Date.now(),
            date: new Date().toISOString(),
            action: action,
            note: note,
            user: 'Admin'
        };
        notes.push(newNote);
        
        await pool.query(`
            UPDATE refund_requests 
            SET notes = $1, updated_at = NOW()
            WHERE id = $2
        `, [JSON.stringify(notes), id]);
        
        console.log(`ðŸ“ Note added to refund ${id}: ${action} - ${note.substring(0, 50)}...`);
        
        res.json({ success: true, note: newNote, allNotes: notes });
        
    } catch (error) {
        console.error('Error adding note:', error);
        res.status(500).json({ error: 'Failed to add note' });
    }
});

// Get notes for a refund
router.get('/api/admin/refunds/:id/notes', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        const result = await pool.query('SELECT notes FROM refund_requests WHERE id = $1', [id]);
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Refund not found' });
        }
        
        let notes = [];
        if (result.rows[0].notes && typeof result.rows[0].notes === 'object') {
            notes = Array.isArray(result.rows[0].notes) ? result.rows[0].notes : [];
        }
        
        res.json({ success: true, notes });
        
    } catch (error) {
        console.error('Error fetching notes:', error);
        res.status(500).json({ error: 'Failed to fetch notes' });
    }
});

// Get transactions with pagination
router.get('/api/admin/transactions', authenticateToken, async (req, res) => {
    try {
        const { language, startDate, endDate, source, search, page = 1, limit = 10 } = req.query;
        
        const pageNum = parseInt(page) || 1;
        const limitNum = parseInt(limit) || 10;
        const offset = (pageNum - 1) * limitNum;
        
        let baseQuery = `FROM transactions WHERE 1=1`;
        let params = [];
        let paramIndex = 1;
        
        baseQuery += ` AND transaction_id NOT LIKE 'TEST%' AND transaction_id NOT LIKE '%TEST%'`;
        baseQuery += ` AND email NOT LIKE '%test%@%' AND email NOT LIKE '%@test.%'`;
        
        if (search) {
            baseQuery += ` AND (email ILIKE $${paramIndex} OR name ILIKE $${paramIndex} OR transaction_id ILIKE $${paramIndex})`;
            params.push(`%${search}%`);
            paramIndex++;
        }
        
        if (language === 'en' || language === 'es') {
            baseQuery += ` AND (funnel_language = $${paramIndex} OR (funnel_language IS NULL AND $${paramIndex} = 'en'))`;
            params.push(language);
            paramIndex++;
        }
        
        if (source === 'main' || source === 'affiliate' || source === 'perfectpay') {
            baseQuery += ` AND (funnel_source = $${paramIndex} OR (funnel_source IS NULL AND $${paramIndex} = 'main'))`;
            params.push(source);
            paramIndex++;
        }
        
        if (startDate && endDate) {
            baseQuery += ` AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${paramIndex}::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${paramIndex + 1}::date`;
            params.push(startDate, endDate);
            paramIndex += 2;
        }
        
        const countResult = await pool.query(`SELECT COUNT(*) ${baseQuery}`, params);
        const total = parseInt(countResult.rows[0].count);
        const totalPages = Math.ceil(total / limitNum);
        
        const dataQuery = `SELECT * ${baseQuery} ORDER BY created_at DESC LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
        params.push(limitNum, offset);
        
        const result = await pool.query(dataQuery, params);
        
        res.json({ 
            transactions: result.rows, 
            language: language || 'all', 
            source: source || 'all',
            pagination: {
                page: pageNum,
                limit: limitNum,
                total,
                totalPages
            }
        });
        
    } catch (error) {
        console.error('Error fetching transactions:', error);
        res.status(500).json({ error: 'Failed to fetch transactions' });
    }
});

// Delete transaction
router.delete('/api/admin/transactions/:id', authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;
        
        const result = await pool.query(
            'DELETE FROM transactions WHERE id = $1 RETURNING *',
            [id]
        );
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Transaction not found' });
        }
        
        console.log(`Transaction deleted: ${id}`);
        res.json({ success: true, message: 'Transaction deleted' });
        
    } catch (error) {
        console.error('Error deleting transaction:', error);
        res.status(500).json({ error: 'Failed to delete transaction' });
    }
});

// Get sales stats
router.get('/api/admin/sales', authenticateToken, async (req, res) => {
    try {
        const { language, startDate, endDate, source } = req.query;
        
        console.log('[Sales API] Params:', { language, startDate, endDate, source });
        
        let langCondition = '';
        let langParams = [];
        if (language === 'en' || language === 'es') {
            langCondition = `AND (funnel_language = $1 OR (funnel_language IS NULL AND $1 = 'en'))`;
            langParams = [language];
        }
        
        let sourceCondition = '';
        if (source === 'main' || source === 'affiliate' || source === 'perfectpay') {
            const sourceIdx = langParams.length + 1;
            sourceCondition = ` AND (funnel_source = $${sourceIdx} OR (funnel_source IS NULL AND $${sourceIdx} = 'main'))`;
            langParams.push(source);
        }
        
        let dateCondition = '';
        if (startDate && endDate) {
            const startIdx = langParams.length + 1;
            const endIdx = langParams.length + 2;
            dateCondition = ` AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${startIdx}::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${endIdx}::date`;
            langParams.push(startDate, endDate);
        }
        
        const [totalResult, approvedResult, refundedResult, revenueResult, cancelledResult, lostRevenueResult, upsellRevenueResult, totalAttemptsResult, approvedAttemptsResult] = await Promise.all([
            pool.query(`SELECT COUNT(*) FROM transactions WHERE 1=1 ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status IN ('refunded', 'chargeback') ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`
                SELECT COUNT(DISTINCT email) 
                FROM transactions t 
                WHERE t.status = 'cancelled'
                ${langCondition}${sourceCondition}${dateCondition}
                AND NOT EXISTS (
                    SELECT 1 FROM transactions t2 
                    WHERE t2.email = t.email 
                    AND t2.status = 'approved'
                )
            `, langParams),
            pool.query(`
                SELECT COALESCE(SUM(max_value), 0) as total
                FROM (
                    SELECT email, MAX(CAST(value AS DECIMAL)) as max_value
                    FROM transactions t
                    WHERE t.status = 'cancelled'
                    ${langCondition}${sourceCondition}${dateCondition}
                    AND NOT EXISTS (
                        SELECT 1 FROM transactions t2 
                        WHERE t2.email = t.email 
                        AND t2.status = 'approved'
                    )
                    GROUP BY email
                ) unique_customers
            `, langParams),
            pool.query(`SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved' AND (product ILIKE '%Message Vault%' OR product ILIKE '%Vault%' OR product ILIKE '%360%' OR product ILIKE '%Tracker%' OR product ILIKE '%Instant%' OR product ILIKE '%RecuperaciÃ³n%' OR product ILIKE '%VisiÃ³n%' OR product ILIKE '%VIP%') ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(DISTINCT email) FROM transactions WHERE 1=1 ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(DISTINCT email) FROM transactions WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}`, langParams)
        ]);
        
        const [todayResult, weekResult] = await Promise.all([
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(*) FROM transactions WHERE status = 'approved' AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= ((NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '7 days')::date ${langCondition}${sourceCondition}${dateCondition}`, langParams)
        ]);
        
        let funnelLangCondition = '';
        if (language === 'en' || language === 'es') {
            funnelLangCondition = ` AND (metadata->>'funnelLanguage' = '${language}' OR (metadata->>'funnelLanguage' IS NULL AND '${language}' = 'en'))`;
        }
        
        let funnelSourceCondition = '';
        if (source === 'main' || source === 'affiliate' || source === 'perfectpay') {
            funnelSourceCondition = ` AND (metadata->>'funnelSource' = '${source}' OR (metadata->>'funnelSource' IS NULL AND '${source}' = 'main'))`;
        }
        
        let funnelDateCondition = '';
        if (startDate && endDate) {
            funnelDateCondition = ` AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= '${startDate}'::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= '${endDate}'::date`;
        }
        
        const checkoutClickedResult = await pool.query(`
            SELECT COUNT(DISTINCT visitor_id) as count 
            FROM funnel_events 
            WHERE event = 'checkout_clicked'${funnelLangCondition}${funnelSourceCondition}${funnelDateCondition}
        `);
        
        const paymentAttemptsResult = await pool.query(`
            SELECT COUNT(DISTINCT LOWER(email)) as count 
            FROM transactions 
            WHERE 1=1 ${langCondition}${sourceCondition}${dateCondition}
        `, langParams);
        
        const paymentAttempts = parseInt(paymentAttemptsResult.rows[0].count) || 0;
        
        const approvedEmailsResult = await pool.query(`
            SELECT COUNT(DISTINCT LOWER(email)) as count 
            FROM transactions 
            WHERE status = 'approved' ${langCondition}${sourceCondition}${dateCondition}
        `, langParams);
        
        const checkoutClicked = parseInt(checkoutClickedResult.rows[0].count) || 0;
        const checkoutAbandoned = Math.max(0, checkoutClicked - paymentAttempts);
        const approvedEmails = parseInt(approvedEmailsResult.rows[0].count) || 0;
        
        let leadsLangCondition = '';
        let leadsSourceCondition = '';
        let leadsDateCondition = '';
        let leadsParams = [];
        
        if (language) {
            leadsLangCondition = ` AND (funnel_language = $${leadsParams.length + 1} OR (funnel_language IS NULL AND $${leadsParams.length + 1} = 'en'))`;
            leadsParams.push(language);
        }
        if (source === 'main' || source === 'affiliate' || source === 'perfectpay') {
            leadsSourceCondition = ` AND (funnel_source = $${leadsParams.length + 1} OR (funnel_source IS NULL AND $${leadsParams.length + 1} = 'main'))`;
            leadsParams.push(source);
        }
        if (startDate && endDate) {
            const startIdx = leadsParams.length + 1;
            const endIdx = leadsParams.length + 2;
            leadsDateCondition = ` AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${startIdx}::date AND (created_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${endIdx}::date`;
            leadsParams.push(startDate, endDate);
        }
        
        const leadsCount = await pool.query(`SELECT COUNT(*) FROM leads WHERE 1=1 ${leadsLangCondition}${leadsSourceCondition}${leadsDateCondition}`, leadsParams);
        const conversionRate = parseInt(leadsCount.rows[0].count) > 0 
            ? ((parseInt(approvedResult.rows[0].count) / parseInt(leadsCount.rows[0].count)) * 100).toFixed(2)
            : 0;
        
        const productStats = await pool.query(`
            SELECT 
                product,
                COUNT(*) FILTER (WHERE status = 'approved') as approved,
                COUNT(*) FILTER (WHERE status IN ('refunded', 'chargeback')) as refunded,
                COALESCE(SUM(CAST(value AS DECIMAL)) FILTER (WHERE status = 'approved'), 0) as revenue,
                COUNT(*) as total
            FROM transactions
            WHERE product IS NOT NULL ${langCondition}${sourceCondition}${dateCondition}
            GROUP BY product
            ORDER BY approved DESC
        `, langParams);
        
        const enFrontKeywords = "product ILIKE '%Monitor%' OR product ILIKE '%ZappDetect%' OR product ILIKE '%341972%' OR product ILIKE '%330254%'";
        const enUp1Keywords = "product ILIKE '%Message Vault%' OR product ILIKE '%349241%' OR product ILIKE '%341443%'";
        const enUp2Keywords = "product ILIKE '%360%' OR product ILIKE '%Tracker%' OR product ILIKE '%349242%' OR product ILIKE '%341444%'";
        const enUp3Keywords = "product ILIKE '%Instant Access%' OR product ILIKE '%349243%' OR product ILIKE '%341448%'";
        
        const esFrontKeywords = "product ILIKE '%Infidelidad%' OR product ILIKE '%349260%' OR product ILIKE '%338375%'";
        const esUp1Keywords = "product ILIKE '%RecuperaciÃ³n%' OR product ILIKE '%349261%' OR product ILIKE '%341452%'";
        const esUp2Keywords = "product ILIKE '%VisiÃ³n Total%' OR product ILIKE '%349266%' OR product ILIKE '%341453%'";
        const esUp3Keywords = "product ILIKE '%VIP Sin Esperas%' OR product ILIKE '%349267%' OR product ILIKE '%341454%'";
        
        let frontKeywords, up1Keywords, up2Keywords, up3Keywords;
        if (language === 'es') {
            frontKeywords = esFrontKeywords;
            up1Keywords = esUp1Keywords;
            up2Keywords = esUp2Keywords;
            up3Keywords = esUp3Keywords;
        } else if (language === 'en') {
            frontKeywords = enFrontKeywords;
            up1Keywords = enUp1Keywords;
            up2Keywords = enUp2Keywords;
            up3Keywords = enUp3Keywords;
        } else {
            frontKeywords = `(${enFrontKeywords}) OR (${esFrontKeywords})`;
            up1Keywords = `(${enUp1Keywords}) OR (${esUp1Keywords})`;
            up2Keywords = `(${enUp2Keywords}) OR (${esUp2Keywords})`;
            up3Keywords = `(${enUp3Keywords}) OR (${esUp3Keywords})`;
        }
        
        const [frontSales, upsell1Sales, upsell2Sales, upsell3Sales, frontRevenueResult, up1RevenueResult, up2RevenueResult, up3RevenueResult] = await Promise.all([
            pool.query(`SELECT COUNT(DISTINCT email) as count FROM transactions WHERE status = 'approved' AND (${frontKeywords}) ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(DISTINCT email) as count FROM transactions WHERE status = 'approved' AND (${up1Keywords}) ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(DISTINCT email) as count FROM transactions WHERE status = 'approved' AND (${up2Keywords}) ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COUNT(DISTINCT email) as count FROM transactions WHERE status = 'approved' AND (${up3Keywords}) ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved' AND (${frontKeywords}) ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved' AND (${up1Keywords}) ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved' AND (${up2Keywords}) ${langCondition}${sourceCondition}${dateCondition}`, langParams),
            pool.query(`SELECT COALESCE(SUM(CAST(value AS DECIMAL)), 0) as total FROM transactions WHERE status = 'approved' AND (${up3Keywords}) ${langCondition}${sourceCondition}${dateCondition}`, langParams)
        ]);
        
        const frontCount = parseInt(frontSales.rows[0].count) || 0;
        const up1Count = parseInt(upsell1Sales.rows[0].count) || 0;
        const up2Count = parseInt(upsell2Sales.rows[0].count) || 0;
        const up3Count = parseInt(upsell3Sales.rows[0].count) || 0;
        
        const totalUpsellCount = up1Count + up2Count + up3Count;
        const frontRevenue = parseFloat(frontRevenueResult.rows[0].total) || 0;
        const up1Revenue = parseFloat(up1RevenueResult.rows[0].total) || 0;
        const up2Revenue = parseFloat(up2RevenueResult.rows[0].total) || 0;
        const up3Revenue = parseFloat(up3RevenueResult.rows[0].total) || 0;
        const upsellRevenue = up1Revenue + up2Revenue + up3Revenue;
        const avgUpsellTicket = totalUpsellCount > 0 ? upsellRevenue / totalUpsellCount : 0;
        
        res.json({
            total: parseInt(totalResult.rows[0].count),
            approved: parseInt(approvedResult.rows[0].count),
            refunded: parseInt(refundedResult.rows[0].count),
            cancelled: parseInt(cancelledResult.rows[0].count),
            lostRevenue: parseFloat(lostRevenueResult.rows[0].total) || 0,
            revenue: parseFloat(revenueResult.rows[0].total) || 0,
            frontRevenue: frontRevenue,
            upsellRevenue: upsellRevenue,
            avgUpsellTicket: avgUpsellTicket,
            checkoutAbandoned: checkoutAbandoned,
            checkoutClicked: checkoutClicked,
            today: parseInt(todayResult.rows[0].count),
            thisWeek: parseInt(weekResult.rows[0].count),
            totalAttempts: parseInt(totalAttemptsResult.rows[0].count),
            approvedAttempts: parseInt(approvedAttemptsResult.rows[0].count),
            conversionRate: parseFloat(conversionRate),
            byProduct: productStats.rows,
            language: language || 'all',
            source: source || 'all',
            upsellStats: {
                front: frontCount,
                frontRevenue: frontRevenue,
                upsell1: up1Count,
                upsell1Revenue: up1Revenue,
                upsell2: up2Count,
                upsell2Revenue: up2Revenue,
                upsell3: up3Count,
                upsell3Revenue: up3Revenue,
                total: totalUpsellCount,
                takeRate1: frontCount > 0 ? ((up1Count / frontCount) * 100).toFixed(1) : 0,
                takeRate2: frontCount > 0 ? ((up2Count / frontCount) * 100).toFixed(1) : 0,
                takeRate3: frontCount > 0 ? ((up3Count / frontCount) * 100).toFixed(1) : 0
            }
        });
        
    } catch (error) {
        console.error('Error fetching sales stats:', error);
        res.status(500).json({ error: 'Failed to fetch sales stats' });
    }
});

// Migrate existing transactions to set funnel_source based on product codes
router.post('/api/admin/migrate-funnel-source', authenticateToken, async (req, res) => {
    try {
        const affiliateCodes = ['330254', '341443', '341444', '341448', '338375', '341452', '341453', '341454'];
        
        let updated = 0;
        
        for (const code of affiliateCodes) {
            const result = await pool.query(`
                UPDATE transactions 
                SET funnel_source = 'affiliate' 
                WHERE (funnel_source IS NULL OR funnel_source = 'main')
                AND (
                    raw_data::text LIKE $1
                    OR product ILIKE $2
                )
            `, [`%"codigo":${code}%`, `%${code}%`]);
            updated += result.rowCount;
        }
        
        const fixLangAff = await pool.query(`
            UPDATE transactions 
            SET funnel_language = 'en', funnel_source = 'affiliate'
            WHERE funnel_language = 'en-aff'
        `);
        updated += fixLangAff.rowCount;
        
        const fixLangAffEs = await pool.query(`
            UPDATE transactions 
            SET funnel_language = 'es', funnel_source = 'affiliate'
            WHERE funnel_language = 'es-aff'
        `);
        updated += fixLangAffEs.rowCount;
        
        const fixNull = await pool.query(`
            UPDATE transactions SET funnel_source = 'main' WHERE funnel_source IS NULL
        `);
        
        res.json({ 
            success: true, 
            message: `Migration complete`, 
            updated,
            nullFixed: fixNull.rowCount
        });
    } catch (error) {
        console.error('Error migrating funnel_source:', error);
        res.status(500).json({ error: 'Migration failed', details: error.message });
    }
});

// Migrate existing leads to set funnel_source based on related transactions
router.post('/api/admin/migrate-leads-funnel-source', authenticateToken, async (req, res) => {
    try {
        const affiliateCodes = ['330254', '341443', '341444', '341448', '338375', '341452', '341453', '341454'];
        
        let updated = 0;
        
        for (const code of affiliateCodes) {
            const result = await pool.query(`
                UPDATE leads 
                SET funnel_source = 'affiliate' 
                WHERE (funnel_source IS NULL OR funnel_source = 'main')
                AND LOWER(email) IN (
                    SELECT DISTINCT LOWER(email) FROM transactions 
                    WHERE product ILIKE $1
                )
            `, [`%${code}%`]);
            updated += result.rowCount;
        }
        
        const fromTransactions = await pool.query(`
            UPDATE leads 
            SET funnel_source = 'affiliate' 
            WHERE (funnel_source IS NULL OR funnel_source = 'main')
            AND LOWER(email) IN (
                SELECT DISTINCT LOWER(email) FROM transactions 
                WHERE funnel_source = 'affiliate'
            )
        `);
        updated += fromTransactions.rowCount;
        
        const fixNull = await pool.query(`
            UPDATE leads SET funnel_source = 'main' WHERE funnel_source IS NULL
        `);
        
        const stats = await pool.query(`
            SELECT funnel_source, COUNT(*) as count 
            FROM leads 
            GROUP BY funnel_source
        `);
        
        res.json({ 
            success: true, 
            message: `Leads migration complete`, 
            affiliateUpdated: updated,
            nullFixed: fixNull.rowCount,
            stats: stats.rows
        });
    } catch (error) {
        console.error('Error migrating leads funnel_source:', error);
        res.status(500).json({ error: 'Leads migration failed', details: error.message });
    }
});

module.exports = router;
