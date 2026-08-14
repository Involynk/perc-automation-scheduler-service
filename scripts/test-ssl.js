const fs = require('fs');
const path = require('path');
const { PrismaClient } = require('@prisma/client');

const envContent = fs.readFileSync(path.resolve(__dirname, '../.env'), 'utf8');
const line = envContent.split('\n').find(l => l.startsWith('DATABASE_URL='));
const rawUrl = line.replace(/^DATABASE_URL="?/, '').replace(/"?\r?$/, '');

async function testWithPrisma(url, label) {
  console.log(`\n--- Testing PrismaClient with ${label} ---`);
  const prisma = new PrismaClient({
    datasources: { db: { url } },
    log: ['error', 'warn']
  });
  try {
    await prisma.$connect();
    console.log(`[Prisma] Success! Connected securely.`);
    const tables = await prisma.$queryRawUnsafe(
      `SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('timers','processed_events','outbox_events') ORDER BY table_name`
    );
    console.log(`[Prisma] Scheduler tables visible:`, tables.map(t => t.table_name));
    const timerCount = await prisma.timer.count();
    console.log(`[Prisma] Timer records count:`, timerCount);
    return true;
  } catch (err) {
    console.error(`[Prisma] Failed: ${err.message}`);
    return false;
  } finally {
    await prisma.$disconnect();
  }
}

async function run() {
  const base = rawUrl.split('?')[0];

  console.log('Testing connection options on Supabase Session Pooler (port 5432)...');

  // Option 1: sslmode=require (Standard secure SSL)
  await testWithPrisma(`${base}?schema=public&sslmode=require`, 'sslmode=require');

  // Option 2: sslmode=prefer (Standard fallback)
  await testWithPrisma(`${base}?schema=public&sslmode=prefer`, 'sslmode=prefer');

  // Option 3: default without sslmode query param
  await testWithPrisma(`${base}?schema=public`, 'no sslmode parameter');
}

run();
