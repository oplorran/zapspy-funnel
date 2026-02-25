# Recovery Sales Data Analysis

## Key Findings

1. **Transactions table** stores PerfectPay data with:
   - `transaction_id` (PP_xxx format)
   - `email`, `name`, `value`, `status`
   - `raw_data` JSONB with full webhook payload
   - `funnel_source` = 'perfectpay'
   - `funnel_language` = 'en' or 'es'
   - No dedicated UTM columns in transactions table

2. **Recovery sales identification**: 
   - The raw_data JSONB contains `metadata.utm_medium` and `metadata.utm_campaign` for PerfectPay
   - For Monetizze postbacks, UTMs are in `venda.utm_medium`, `venda.utm_campaign`, `venda.utm_source`
   - Recovery emails use utm_source=ActiveCampaign, utm_medium=email, utm_campaign="email1 rmkt" etc.

3. **Approach for recovery sales query**:
   - Query transactions where raw_data contains utm_medium='email' OR utm_source='ActiveCampaign'
   - Also cross-reference: emails in email_dispatch_log that later appear in transactions with approved status
   - Both approaches should be used for maximum coverage

4. **Current totals**: 1,838 approved transactions, $244,192.30 total revenue
