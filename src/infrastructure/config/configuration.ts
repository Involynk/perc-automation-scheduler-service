export default () => ({
  port: parseInt(process.env.PORT, 10) || 3000,
  nodeEnv: process.env.NODE_ENV || 'development',
  database: {
    url: process.env.DATABASE_URL,
  },
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT, 10) || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
    db: parseInt(process.env.REDIS_DB, 10) || 0,
  },
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    clientId: process.env.KAFKA_CLIENT_ID || 'perc-scheduler-service',
    groupId: process.env.KAFKA_GROUP_ID || 'perc-scheduler-group',
    ssl: process.env.KAFKA_USE_SSL === 'true',
    saslMechanism: process.env.KAFKA_SASL_MECHANISM || undefined,
    saslUsername: process.env.KAFKA_SASL_USERNAME || undefined,
    saslPassword: process.env.KAFKA_SASL_PASSWORD || undefined,
  },
  topics: {
    scheduleRequested: 'perc.scheduler.timer-schedule-requested',
    cancelRequested: 'perc.scheduler.timer-cancel-requested',
    rescheduleRequested: 'perc.scheduler.timer-reschedule-requested',
    timerTriggered: 'perc.scheduler.timer-triggered',
    commandsDlq: 'perc.scheduler.commands.dlq',
    timerTriggeredDlq: 'perc.scheduler.timer-triggered.dlq',
  },
  outbox: {
    pollIntervalMs: parseInt(process.env.OUTBOX_POLL_INTERVAL_MS, 10) || 500,
    batchSize: parseInt(process.env.OUTBOX_BATCH_SIZE, 10) || 50,
    maxRetries: parseInt(process.env.OUTBOX_MAX_RETRIES, 10) || 5,
  },
  bullmq: {
    queueName: 'perc-scheduler-delay-queue',
    concurrency: parseInt(process.env.BULLMQ_CONCURRENCY, 10) || 20,
    lockDurationMs: parseInt(process.env.BULLMQ_LOCK_DURATION_MS, 10) || 30000,
  },
});
