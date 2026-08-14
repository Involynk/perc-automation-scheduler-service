import { PrismaClient } from '@prisma/client';
import * as fs from 'fs';
import * as path from 'path';

async function main() {
  const prisma = new PrismaClient();
  try {
    console.log('Connecting to Supabase PostgreSQL...');
    await prisma.$connect();
    console.log('Connected.');

    const sqlPath = path.join(__dirname, '../prisma/migrations/20260814000000_init_scheduler_schema/migration.sql');
    const fullSql = fs.readFileSync(sqlPath, 'utf8');

    // Split SQL by statements
    const statements = fullSql
      .split(';')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);

    console.log(`Executing ${statements.length} SQL DDL statements on Supabase...`);
    for (const stmt of statements) {
      try {
        await prisma.$executeRawUnsafe(stmt);
      } catch (err: any) {
        // Ignore "type already exists" warnings for idempotency
        if (err.message.includes('already exists') || err.meta?.code === '42710') {
          console.log(`Notice: Enum or object already exists. Skipping.`);
        } else {
          console.error(`Statement error on: ${stmt.substring(0, 50)}...`, err);
          throw err;
        }
      }
    }

    console.log('All Scheduler DDL statements executed successfully.');

    // Verify tables
    const tables: any = await prisma.$queryRaw`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' AND table_name IN ('timers', 'processed_events', 'outbox_events');
    `;
    console.log('Verified Scheduler tables in Supabase:', tables);
  } catch (error) {
    console.error('Migration error:', error);
    process.exit(1);
  } finally {
    await prisma.$disconnect();
  }
}

main();
