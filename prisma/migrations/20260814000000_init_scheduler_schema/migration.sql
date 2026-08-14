-- CreateEnum
CREATE TYPE "TimerStatus" AS ENUM ('PENDING', 'EXECUTED', 'CANCELLED', 'FAILED');

-- CreateEnum
CREATE TYPE "OutboxStatus" AS ENUM ('PENDING', 'PUBLISHED', 'FAILED');

-- CreateTable
CREATE TABLE "timers" (
    "id" UUID NOT NULL,
    "timer_key" VARCHAR(255) NOT NULL,
    "requesting_service" VARCHAR(64) NOT NULL,
    "correlation_id" VARCHAR(128) NOT NULL,
    "target_execution_time" TIMESTAMPTZ(6) NOT NULL,
    "status" "TimerStatus" NOT NULL DEFAULT 'PENDING',
    "opaque_payload" JSONB NOT NULL,
    "executed_at" TIMESTAMPTZ(6),
    "cancelled_at" TIMESTAMPTZ(6),
    "cancel_reason" VARCHAR(255),
    "retry_count" INTEGER NOT NULL DEFAULT 0,
    "last_error" TEXT,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "timers_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "processed_events" (
    "event_id" UUID NOT NULL,
    "consumer_group" VARCHAR(64) NOT NULL,
    "processed_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_events_pkey" PRIMARY KEY ("event_id")
);

-- CreateTable
CREATE TABLE "outbox_events" (
    "id" UUID NOT NULL,
    "event_id" UUID NOT NULL,
    "topic" VARCHAR(128) NOT NULL,
    "kafka_key" VARCHAR(128) NOT NULL,
    "event_type" VARCHAR(64) NOT NULL,
    "payload" JSONB NOT NULL,
    "status" "OutboxStatus" NOT NULL DEFAULT 'PENDING',
    "retry_count" INTEGER NOT NULL DEFAULT 0,
    "last_error" TEXT,
    "published_at" TIMESTAMPTZ(6),
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "outbox_events_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "timers_timer_key_key" ON "timers"("timer_key");

-- CreateIndex
CREATE INDEX "idx_timers_pending_recovery" ON "timers"("status", "target_execution_time");

-- CreateIndex
CREATE INDEX "idx_timers_timer_key" ON "timers"("timer_key");

-- CreateIndex
CREATE INDEX "idx_timers_correlation_id" ON "timers"("correlation_id");

-- CreateIndex
CREATE INDEX "idx_processed_events_cleanup" ON "processed_events"("processed_at");

-- CreateIndex
CREATE UNIQUE INDEX "outbox_events_event_id_key" ON "outbox_events"("event_id");

-- CreateIndex
CREATE INDEX "idx_outbox_pending_events" ON "outbox_events"("status", "created_at");
