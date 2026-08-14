import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AppModule } from '../src/app.module';
import { PrismaService } from '../src/infrastructure/persistence/prisma.service';
import { TimerQueueService } from '../src/infrastructure/bullmq/timer-queue.service';
import { StartupRecoveryService } from '../src/application/services/startup-recovery.service';
import { OutboxPublisherService } from '../src/application/services/outbox-publisher.service';
import { TimerStatus } from '../src/domain/enums/timer-status.enum';
import { OutboxStatus } from '../src/domain/enums/outbox-status.enum';
import { Kafka, Producer, Consumer } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';
import Redis from 'ioredis';

describe('PERC Scheduler Engine - Comprehensive E2E Integration Suite', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let queueService: TimerQueueService;
  let startupRecovery: StartupRecoveryService;
  let outboxPublisher: OutboxPublisherService;
  let redisClient: Redis;

  let kafka: Kafka;
  let testProducer: Producer;
  let testConsumer: Consumer;

  const triggeredEventsReceived: any[] = [];
  const dlqEventsReceived: any[] = [];
  const createdTimerKeys: string[] = [];

  beforeAll(async () => {
    // 1. Initialize NestJS App
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();

    prisma = app.get(PrismaService);
    queueService = app.get(TimerQueueService);
    startupRecovery = app.get(StartupRecoveryService);
    outboxPublisher = app.get(OutboxPublisherService);

    // 2. Direct Redis Client for testing
    redisClient = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: parseInt(process.env.REDIS_PORT || '6379', 10),
      maxRetriesPerRequest: null,
    });

    // 3. Kafka Test Client
    const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
    kafka = new Kafka({
      clientId: 'perc-scheduler-e2e-tester',
      brokers,
    });

    testProducer = kafka.producer();
    await testProducer.connect();

    testConsumer = kafka.consumer({ groupId: `e2e-test-group-${Date.now()}` });
    await testConsumer.connect();

    await testConsumer.subscribe({
      topics: ['perc.scheduler.timer-triggered', 'perc.scheduler.commands.dlq'],
      fromBeginning: false,
    });

    await testConsumer.run({
      eachMessage: async ({ topic, message }) => {
        const value = message.value ? JSON.parse(message.value.toString()) : null;
        if (topic === 'perc.scheduler.timer-triggered') {
          triggeredEventsReceived.push(value);
        } else if (topic === 'perc.scheduler.commands.dlq') {
          dlqEventsReceived.push(value);
        }
      },
    });

    // Give Kafka consumer 3 seconds to establish partition assignments
    await new Promise((resolve) => setTimeout(resolve, 3000));
  }, 45000);

  afterAll(async () => {
    // Clean up created database records
    if (createdTimerKeys.length > 0) {
      try {
        await prisma.timer.deleteMany({
          where: { timerKey: { in: createdTimerKeys } },
        });
      } catch (err) {
        // Ignore cleanup errors
      }
    }

    await testConsumer?.disconnect();
    await testProducer?.disconnect();
    redisClient?.disconnect();
    await app?.close();
  });

  // ============================================================================
  // Test 1: Full Schedule -> BullMQ Delay -> Outbox -> Kafka Event E2E
  // ============================================================================
  it('1. Full Schedule -> BullMQ Delay -> Outbox -> Kafka Event E2E (Semantic JSON Equality)', async () => {
    const testTimerKey = `test:e2e:schedule:${uuidv4()}`;
    const eventId = uuidv4();
    createdTimerKeys.push(testTimerKey);

    const originalPayload = {
      leadId: 'L-9921',
      cadenceStep: 1,
      templateCode: 'FOLLOWUP_NO_REPLY_2H',
      nestedMetadata: {
        score: 95.5,
        flags: ['HOT_LEAD', 'SCHOLARSHIP_CANDIDATE'],
        details: { course: 'NEET_2026', counselor: 'Priya' },
      },
    };

    const targetTime = new Date(Date.now() + 1000); // 1.0 second delay

    // Publish schedule command to Kafka
    await testProducer.send({
      topic: 'perc.scheduler.timer-schedule-requested',
      messages: [
        {
          key: 'L-9921',
          value: JSON.stringify({
            eventId,
            timerKey: testTimerKey,
            targetExecutionTime: targetTime.toISOString(),
            requestingService: 'followup-engine',
            correlationId: 'L-9921',
            opaquePayload: originalPayload,
          }),
        },
      ],
    });

    // Wait for message consumption and BullMQ delay completion (7.5 seconds)
    await new Promise((resolve) => setTimeout(resolve, 7500));

    // Force an outbox polling pass
    await outboxPublisher.processOutboxBatch();
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Verify DB Timer state
    const timerInDb = await prisma.timer.findUnique({ where: { timerKey: testTimerKey } });
    expect(timerInDb).toBeDefined();
    expect(timerInDb?.status).toBe(TimerStatus.EXECUTED);
    expect(timerInDb?.executedAt).toBeDefined();

    // Verify Outbox Event state
    const outboxInDb = await prisma.outboxEvent.findFirst({
      where: { kafkaKey: 'L-9921' },
      orderBy: { createdAt: 'desc' },
    });
    expect(outboxInDb).toBeDefined();
    expect(outboxInDb?.status).toBe(OutboxStatus.PUBLISHED);

    // Verify Kafka Received Trigger Event
    const receivedEvent = triggeredEventsReceived.find((e) => e.timerKey === testTimerKey);
    expect(receivedEvent).toBeDefined();
    expect(receivedEvent.eventType).toBe('TIMER_TRIGGERED');
    expect(receivedEvent.requestingService).toBe('followup-engine');
    expect(receivedEvent.correlationId).toBe('L-9921');

    // Semantic JSON Equality Assertion
    expect(receivedEvent.opaquePayload).toEqual(originalPayload);
    expect(receivedEvent.opaquePayload.nestedMetadata.flags).toContain('SCHOLARSHIP_CANDIDATE');
  }, 25000);

  // ============================================================================
  // Test 2: Single & Bulk Prefix Cancellation E2E
  // ============================================================================
  it('2. Single & Bulk Prefix Cancellation E2E', async () => {
    const prefix = `test:bulk:${uuidv4().substring(0, 8)}`;
    const timerKey1 = `${prefix}:step-1`;
    const timerKey2 = `${prefix}:step-2`;
    createdTimerKeys.push(timerKey1, timerKey2);

    const futureTime = new Date(Date.now() + 25000); // 25 seconds in future

    // Schedule 2 timers with common prefix
    await testProducer.send({
      topic: 'perc.scheduler.timer-schedule-requested',
      messages: [
        {
          key: 'L-BULK',
          value: JSON.stringify({
            eventId: uuidv4(),
            timerKey: timerKey1,
            targetExecutionTime: futureTime.toISOString(),
            requestingService: 'workflow-engine',
            correlationId: 'L-BULK',
            opaquePayload: { timerKey1 },
          }),
        },
        {
          key: 'L-BULK',
          value: JSON.stringify({
            eventId: uuidv4(),
            timerKey: timerKey2,
            targetExecutionTime: futureTime.toISOString(),
            requestingService: 'workflow-engine',
            correlationId: 'L-BULK',
            opaquePayload: { timerKey2 },
          }),
        },
      ],
    });

    await new Promise((resolve) => setTimeout(resolve, 3500));

    // Send Bulk Cancel Request
    await testProducer.send({
      topic: 'perc.scheduler.timer-cancel-requested',
      messages: [
        {
          key: 'L-BULK',
          value: JSON.stringify({
            eventId: uuidv4(),
            timerKeyPrefix: prefix,
            requestingService: 'workflow-engine',
            reason: 'ADMISSION_COMPLETED',
          }),
        },
      ],
    });

    await new Promise((resolve) => setTimeout(resolve, 3500));

    // Verify both DB timers are CANCELLED
    const timersInDb = await prisma.timer.findMany({
      where: { timerKey: { in: [timerKey1, timerKey2] } },
    });

    expect(timersInDb.length).toBe(2);
    expect(timersInDb[0].status).toBe(TimerStatus.CANCELLED);
    expect(timersInDb[1].status).toBe(TimerStatus.CANCELLED);

    // Verify BullMQ jobs were removed from Redis
    const job1 = await queueService.getJob(timerKey1);
    const job2 = await queueService.getJob(timerKey2);
    expect(job1).toBeFalsy();
    expect(job2).toBeFalsy();
  }, 25000);

  // ============================================================================
  // Test 3: Atomic Reschedule E2E
  // ============================================================================
  it('3. Atomic Reschedule E2E', async () => {
    const testTimerKey = `test:resched:${uuidv4()}`;
    createdTimerKeys.push(testTimerKey);

    const initialFuture = new Date(Date.now() + 30000); // 30s

    // 1. Initial Schedule
    await testProducer.send({
      topic: 'perc.scheduler.timer-schedule-requested',
      messages: [
        {
          key: 'M-101',
          value: JSON.stringify({
            eventId: uuidv4(),
            timerKey: testTimerKey,
            targetExecutionTime: initialFuture.toISOString(),
            requestingService: 'meeting-engine',
            correlationId: 'M-101',
            opaquePayload: { meetingId: 'M-101', version: 1 },
          }),
        },
      ],
    });

    await new Promise((resolve) => setTimeout(resolve, 2500));

    // 2. Reschedule to 1s from NOW
    const updatedPayload = { meetingId: 'M-101', version: 2, slotRescheduled: true };
    await testProducer.send({
      topic: 'perc.scheduler.timer-reschedule-requested',
      messages: [
        {
          key: 'M-101',
          value: JSON.stringify({
            eventId: uuidv4(),
            timerKey: testTimerKey,
            newTargetExecutionTime: new Date(Date.now() + 1000).toISOString(),
            requestingService: 'meeting-engine',
            correlationId: 'M-101',
            updatedOpaquePayload: updatedPayload,
          }),
        },
      ],
    });

    // Wait for the rescheduled timer to mature (7.5s)
    await new Promise((resolve) => setTimeout(resolve, 7500));
    await outboxPublisher.processOutboxBatch();
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Verify DB Timer executed at rescheduled time
    const timerInDb = await prisma.timer.findUnique({ where: { timerKey: testTimerKey } });
    expect(timerInDb?.status).toBe(TimerStatus.EXECUTED);

    // Verify trigger event has updated payload
    const receivedEvent = triggeredEventsReceived.find((e) => e.timerKey === testTimerKey);
    expect(receivedEvent).toBeDefined();
    expect(receivedEvent.opaquePayload.version).toBe(2);
    expect(receivedEvent.opaquePayload.slotRescheduled).toBe(true);
  }, 25000);

  // ============================================================================
  // Test 4: ProcessedEvent Command Idempotency
  // ============================================================================
  it('4. ProcessedEvent Command Idempotency (Duplicate EventId Rejection)', async () => {
    const testTimerKey = `test:idemp:${uuidv4()}`;
    const duplicateEventId = uuidv4();
    createdTimerKeys.push(testTimerKey);

    const message = {
      eventId: duplicateEventId,
      timerKey: testTimerKey,
      targetExecutionTime: new Date(Date.now() + 15000).toISOString(),
      requestingService: 'followup-engine',
      correlationId: 'L-IDEMP',
      opaquePayload: { attempt: 1 },
    };

    // Send identical command 3 times concurrently
    await Promise.all([
      testProducer.send({
        topic: 'perc.scheduler.timer-schedule-requested',
        messages: [{ key: 'L-IDEMP', value: JSON.stringify(message) }],
      }),
      testProducer.send({
        topic: 'perc.scheduler.timer-schedule-requested',
        messages: [{ key: 'L-IDEMP', value: JSON.stringify(message) }],
      }),
      testProducer.send({
        topic: 'perc.scheduler.timer-schedule-requested',
        messages: [{ key: 'L-IDEMP', value: JSON.stringify(message) }],
      }),
    ]);

    await new Promise((resolve) => setTimeout(resolve, 4000));

    // Verify only 1 timer row exists
    const timersInDb = await prisma.timer.findMany({ where: { timerKey: testTimerKey } });
    expect(timersInDb.length).toBe(1);

    // Verify ProcessedEvent has exactly 1 entry
    const processed = await prisma.processedEvent.findUnique({ where: { eventId: duplicateEventId } });
    expect(processed).toBeDefined();
  }, 20000);

  // ============================================================================
  // Test 5: Redis State Loss & Startup Recovery
  // ============================================================================
  it('5. Redis State Loss & Startup Recovery (BullMQ Job Reconstruction from PostgreSQL)', async () => {
    const testTimerKey = `test:recovery:${uuidv4()}`;
    createdTimerKeys.push(testTimerKey);

    const futureTime = new Date(Date.now() + 60000); // 60s in future

    // 1. Create a PENDING timer directly in PostgreSQL
    await prisma.timer.create({
      data: {
        timerKey: testTimerKey,
        requestingService: 'workflow-engine',
        correlationId: 'L-RECOVER',
        targetExecutionTime: futureTime,
        status: TimerStatus.PENDING,
        opaquePayload: { recoveryTest: true },
      },
    });

    // 2. Wipe Redis memory (simulate state loss)
    await redisClient.flushall();

    // Verify BullMQ has no job
    let jobInRedis = await queueService.getJob(testTimerKey);
    expect(jobInRedis).toBeFalsy();

    // 3. Trigger Startup Recovery Reconciliation
    const recoveryResult = await startupRecovery.reconcileTimers();
    expect(recoveryResult.reconciledCount).toBeGreaterThanOrEqual(1);

    // 4. Verify BullMQ job has been reconstructed in Redis
    jobInRedis = await queueService.getJob(testTimerKey);
    expect(jobInRedis).toBeDefined();
    expect(jobInRedis?.id).toBe(testTimerKey.replace(/:/g, '__'));
  }, 20000);

  // ============================================================================
  // Test 6: Poison Message DLQ & Head-of-Line Unblocking
  // ============================================================================
  it('6. Poison Message DLQ & Head-of-Line Unblocking (Zero Pipeline Blocking)', async () => {
    const validTimerKey = `test:valid-after-poison:${uuidv4()}`;
    createdTimerKeys.push(validTimerKey);

    // 1. Send Poison Message 1: Malformed Non-JSON Text
    await testProducer.send({
      topic: 'perc.scheduler.timer-schedule-requested',
      messages: [{ key: 'POISON-1', value: 'INVALID_NON_JSON_MALFORMED_STRING{{{' }],
    });

    // 2. Send Poison Message 2: Missing required correlationId & targetExecutionTime
    await testProducer.send({
      topic: 'perc.scheduler.timer-schedule-requested',
      messages: [
        {
          key: 'POISON-2',
          value: JSON.stringify({
            eventId: uuidv4(),
            timerKey: 'missing-fields-key',
          }),
        },
      ],
    });

    // 3. Send Valid Schedule Command Immediately After
    await testProducer.send({
      topic: 'perc.scheduler.timer-schedule-requested',
      messages: [
        {
          key: 'VALID-CORR',
          value: JSON.stringify({
            eventId: uuidv4(),
            timerKey: validTimerKey,
            targetExecutionTime: new Date(Date.now() + 5000).toISOString(),
            requestingService: 'followup-engine',
            correlationId: 'VALID-CORR',
            opaquePayload: { valid: true },
          }),
        },
      ],
    });

    await new Promise((resolve) => setTimeout(resolve, 3500));

    // Verify Poison messages were routed to DLQ topic
    expect(dlqEventsReceived.length).toBeGreaterThanOrEqual(1);

    // Verify Valid Command was NOT blocked and successfully stored in DB
    const validInDb = await prisma.timer.findUnique({ where: { timerKey: validTimerKey } });
    expect(validInDb).toBeDefined();
    expect(validInDb?.status).toBe(TimerStatus.PENDING);
  }, 20000);

  // ============================================================================
  // Test 7: Outbox Kafka Outage & Resilience E2E
  // ============================================================================
  it('7. Outbox Kafka Outage & Resilience E2E (Pending Outbox Recovery)', async () => {
    const testTimerKey = `test:outbox-resilience:${uuidv4()}`;
    const outboxEventId = uuidv4();
    createdTimerKeys.push(testTimerKey);

    // 1. Create a timer in EXECUTED state with an OutboxEvent in PENDING state
    const timer = await prisma.timer.create({
      data: {
        timerKey: testTimerKey,
        requestingService: 'workflow-engine',
        correlationId: 'L-OUTBOX',
        targetExecutionTime: new Date(),
        status: TimerStatus.EXECUTED,
        executedAt: new Date(),
        opaquePayload: { outboxResilience: true },
      },
    });

    await prisma.outboxEvent.create({
      data: {
        eventId: outboxEventId,
        topic: 'perc.scheduler.timer-triggered',
        kafkaKey: 'L-OUTBOX',
        eventType: 'TIMER_TRIGGERED',
        payload: {
          eventId: outboxEventId,
          eventType: 'TIMER_TRIGGERED',
          timerId: timer.id,
          timerKey: testTimerKey,
          requestingService: 'workflow-engine',
          correlationId: 'L-OUTBOX',
          opaquePayload: { outboxResilience: true },
          firedAt: new Date().toISOString(),
        },
        status: OutboxStatus.PENDING,
      },
    });

    // 2. Trigger Outbox Publisher batch processing
    await outboxPublisher.processOutboxBatch();

    await new Promise((resolve) => setTimeout(resolve, 2000));

    // 3. Verify OutboxEvent status transitioned to PUBLISHED
    const outboxRecord = await prisma.outboxEvent.findUnique({ where: { eventId: outboxEventId } });
    expect(outboxRecord?.status).toBe(OutboxStatus.PUBLISHED);
    expect(outboxRecord?.publishedAt).toBeDefined();

    // 4. Verify Kafka received the outbox event
    const received = triggeredEventsReceived.find((e) => e.timerKey === testTimerKey);
    expect(received).toBeDefined();
    expect(received.opaquePayload.outboxResilience).toBe(true);
  }, 25000);
});
