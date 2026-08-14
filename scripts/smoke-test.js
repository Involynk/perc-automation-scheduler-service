const { Kafka } = require('kafkajs');
const { PrismaClient } = require('@prisma/client');
const { v4: uuidv4 } = require('uuid');

const prisma = new PrismaClient();

const kafka = new Kafka({
  clientId: 'smoke-test-runner',
  brokers: ['localhost:9092'],
});

function isDeepEqual(obj1, obj2) {
  if (obj1 === obj2) return true;
  if (typeof obj1 !== 'object' || obj1 === null || typeof obj2 !== 'object' || obj2 === null) {
    return false;
  }
  const keys1 = Object.keys(obj1);
  const keys2 = Object.keys(obj2);
  if (keys1.length !== keys2.length) return false;
  for (const key of keys1) {
    if (!keys2.includes(key) || !isDeepEqual(obj1[key], obj2[key])) {
      return false;
    }
  }
  return true;
}

async function runSmokeTest() {
  console.log('========================================================');
  console.log('  PERC SCHEDULER CONTAINERIZED SMOKE TEST');
  console.log('========================================================\n');

  const admin = kafka.admin();
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `smoke-group-${Date.now()}` });

  const testEventId = uuidv4();
  const testTimerKey = `smoke:container:${Date.now()}`;
  const testCorrelationId = `corr-${Date.now()}`;
  const delayMs = 3000;
  const targetExecutionTime = new Date(Date.now() + delayMs);
  const opaquePayload = {
    testName: 'Containerized Production Smoke Test',
    leadId: 'LEAD-PROD-2026',
    metadata: { source: 'workflow-engine', priority: 'critical', nestedArray: [1, 2, 3] },
  };

  try {
    // 0. Ensure topics exist
    console.log('[0/7] Ensuring Kafka topics exist...');
    await admin.connect();
    try {
      await admin.createTopics({
        topics: [
          { topic: 'perc.scheduler.timer-schedule-requested', numPartitions: 1, replicationFactor: 1 },
          { topic: 'perc.scheduler.timer-cancel-requested', numPartitions: 1, replicationFactor: 1 },
          { topic: 'perc.scheduler.timer-reschedule-requested', numPartitions: 1, replicationFactor: 1 },
          { topic: 'perc.scheduler.timer-triggered', numPartitions: 1, replicationFactor: 1 },
        ],
        waitForLeaders: true,
      });
    } catch {}
    await admin.disconnect();

    // 1. Connect Kafka
    console.log('[1/7] Connecting Kafka Producer and Consumer...');
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({
      topic: 'perc.scheduler.timer-triggered',
      fromBeginning: false,
    });

    let receivedTriggerEvent = null;
    await consumer.run({
      eachMessage: async ({ message }) => {
        const val = JSON.parse(message.value.toString());
        if (val.timerKey === testTimerKey) {
          receivedTriggerEvent = val;
          console.log(`\n🎉 [Kafka] Received timer-triggered event for ${testTimerKey}!`);
        }
      },
    });
    console.log('      Kafka producer/consumer ready.');

    // 2. Publish schedule command to Kafka
    console.log(`\n[2/7] Publishing Schedule Command to perc.scheduler.timer-schedule-requested...`);
    const schedulePayload = {
      eventId: testEventId,
      timerKey: testTimerKey,
      targetExecutionTime: targetExecutionTime.toISOString(),
      requestingService: 'workflow-engine',
      correlationId: testCorrelationId,
      opaquePayload,
    };

    await producer.send({
      topic: 'perc.scheduler.timer-schedule-requested',
      messages: [
        {
          key: testCorrelationId,
          value: JSON.stringify(schedulePayload),
        },
      ],
    });
    console.log(`      Command sent. Target execution time: ${targetExecutionTime.toISOString()} (Delay: ${delayMs}ms)`);

    // 3. Verify Timer in PostgreSQL (Supabase)
    console.log('\n[3/7] Verifying Timer record creation in Supabase PostgreSQL...');
    let timerRecord = null;
    const pollStart = Date.now();
    while (Date.now() - pollStart < 10000) {
      timerRecord = await prisma.timer.findUnique({ where: { timerKey: testTimerKey } });
      if (timerRecord) break;
      await new Promise(r => setTimeout(r, 400));
    }

    if (!timerRecord) {
      throw new Error(`Timer record was not created in PostgreSQL within 10 seconds.`);
    }
    console.log(`      ✅ Timer record created in Supabase! Status: ${timerRecord.status}, ID: ${timerRecord.id}`);

    // 4. Wait for BullMQ worker execution
    console.log(`\n[4/7] Waiting for containerized BullMQ worker to execute timer (delay: ${delayMs}ms)...`);
    const execStart = Date.now();
    while (Date.now() - execStart < 15000) {
      timerRecord = await prisma.timer.findUnique({ where: { timerKey: testTimerKey } });
      if (timerRecord && timerRecord.status === 'EXECUTED') break;
      await new Promise(r => setTimeout(r, 400));
    }

    if (timerRecord.status !== 'EXECUTED') {
      throw new Error(`Timer did not transition to EXECUTED. Current status: ${timerRecord.status}`);
    }
    console.log(`      ✅ Timer executed successfully! ExecutedAt: ${timerRecord.executedAt.toISOString()}`);

    // 5. Verify Transactional Outbox in Supabase
    console.log('\n[5/7] Verifying Transactional Outbox record in Supabase...');
    let outboxRecord = null;
    const outboxStart = Date.now();
    while (Date.now() - outboxStart < 8000) {
      outboxRecord = await prisma.outboxEvent.findFirst({
        where: { kafkaKey: testCorrelationId },
      });
      if (outboxRecord) break;
      await new Promise(r => setTimeout(r, 400));
    }

    if (!outboxRecord) {
      throw new Error(`Outbox record was not created for correlationId: ${testCorrelationId}`);
    }
    console.log(`      ✅ Outbox record exists in Supabase! ID: ${outboxRecord.id}, Status: ${outboxRecord.status}`);

    // 6. Wait for Kafka timer-triggered event receipt
    console.log('\n[6/7] Verifying Kafka timer-triggered publication...');
    const triggerStart = Date.now();
    while (Date.now() - triggerStart < 10000) {
      if (receivedTriggerEvent) break;
      await new Promise(r => setTimeout(r, 400));
    }

    if (!receivedTriggerEvent) {
      throw new Error(`Kafka timer-triggered event was not received by consumer within 10 seconds.`);
    }

    // Verify deep semantic JSON equality
    const receivedPayload = receivedTriggerEvent.opaquePayload;
    if (!isDeepEqual(receivedPayload, opaquePayload)) {
      throw new Error(`Semantic payload mismatch! Expected: ${JSON.stringify(opaquePayload)}, Received: ${JSON.stringify(receivedPayload)}`);
    }
    console.log(`      ✅ Kafka timer-triggered received with 100% deep semantic JSON payload equality!`);

    // 7. Verify Outbox record updated to PUBLISHED
    console.log('\n[7/7] Verifying Outbox transition to PUBLISHED in Supabase...');
    const publishStart = Date.now();
    while (Date.now() - publishStart < 8000) {
      outboxRecord = await prisma.outboxEvent.findUnique({ where: { id: outboxRecord.id } });
      if (outboxRecord && outboxRecord.status === 'PUBLISHED') break;
      await new Promise(r => setTimeout(r, 400));
    }

    if (outboxRecord.status !== 'PUBLISHED') {
      throw new Error(`Outbox record did not transition to PUBLISHED. Status: ${outboxRecord.status}`);
    }
    console.log(`      ✅ Outbox status is PUBLISHED! PublishedAt: ${outboxRecord.publishedAt.toISOString()}`);

    console.log('\n========================================================');
    console.log('  🎯 ALL SMOKE TEST STEPS PASSED WITH 100% SUCCESS!');
    console.log('========================================================\n');
  } catch (error) {
    console.error('\n❌ SMOKE TEST FAILED:', error.message);
    process.exit(1);
  } finally {
    try { await producer.disconnect(); } catch {}
    try { await consumer.disconnect(); } catch {}
    try { await prisma.$disconnect(); } catch {}
  }
}

runSmokeTest();
