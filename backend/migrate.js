/**
 * Database migration script
 * Run this to create/update tables
 * 
 * v1: Initial leads table
 * v2: Added name and funnel_language columns
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
        // Create leads table (v1)
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
        
        // Migration v2: Add name column if not exists
        try {
            await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS name VARCHAR(255);`);
            console.log('✅ Column "name" added/verified');
        } catch (e) {
            console.log('ℹ️ Column "name" already exists or cannot be added');
        }
        
        // Migration v2: Add funnel_language column if not exists
        try {
            await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS funnel_language VARCHAR(10) DEFAULT 'en';`);
            console.log('✅ Column "funnel_language" added/verified');
        } catch (e) {
            console.log('ℹ️ Column "funnel_language" already exists or cannot be added');
        }
        
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
        await pool.query(`
            CREATE INDEX IF NOT EXISTS idx_leads_funnel_language ON leads(funnel_language);
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
