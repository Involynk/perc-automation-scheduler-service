/**
 * Prisma connection test using the current DATABASE_URL from .env.
 * Does not print credentials.
 */
const fs = require('fs');
const path = require('path');

const envContent = fs.readFileSync(path.resolve(__dirname, '../.env'), 'utf8');
const line = envContent.split('\n').find(l => l.startsWith('DATABASE_URL='));
const rawUrl = line.replace(/^DATABASE_URL="?/, '').replace(/"?\r?$/, '');
const u = new URL(rawUrl);

console.log('=== Prisma Connection Test ===');
console.log('Host    :', u.hostname);
console.log('Port    :', u.port);
console.log('User    :', u.username);
console.log('DB      :', u.pathname.replace('/', ''));
console.log('Params  :', Object.fromEntries(u.searchParams));
console.log('');

const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient({ log: ['error'] });

async function run() {
  try {
    await prisma.$connect();
    console.log('✅ Prisma connected successfully.');
    const tables = await prisma.$queryRawUnsafe(
      `SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('timers','processed_events','outbox_events') ORDER BY table_name`
    );
    console.log('✅ Tables visible:', tables.map(t => t.table_name));
    const timerCount = await prisma.timer.count();
    console.log('✅ timers row count:', timerCount);
  } catch (err) {
    console.error('❌ Connection failed:', err.message);
  } finally {
    await prisma.$disconnect();
  }
}
run();
