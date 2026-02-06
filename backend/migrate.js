/**
 * Database migration script
 * Run this once to create the leads table
 */

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

async function migrate() {
    console.log('🔄 Running database migrations...');
    
    try {
        // Create leads table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                whatsapp VARCHAR(50) NOT NULL,
                target_phone VARCHAR(50),
                target_gender VARCHAR(20),
                status VARCHAR(50) DEFAULT 'new',
                notes TEXT,
                ip_address VARCHAR(45),
                referrer TEXT,
                user_agent TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log('✅ Table "leads" created/verified');
        
        // Create indexes for better performance
        await pool.query(`
            CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email);
        `);
        await pool.query(`
            CREATE INDEX IF NOT EXISTS idx_leads_created_at ON leads(created_at DESC);
        `);
        await pool.query(`
            CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
        `);
        console.log('✅ Indexes created/verified');
        
        console.log('');
        console.log('🎉 Migration completed successfully!');
        console.log('');
        console.log('Your database is ready. You can now start the server:');
        console.log('  npm start');
        
    } catch (error) {
        console.error('❌ Migration failed:', error.message);
        process.exit(1);
    } finally {
        await pool.end();
    }
}

migrate();
