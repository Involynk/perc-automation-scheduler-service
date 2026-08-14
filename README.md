# PERC Scheduler Engine (`perc-scheduler-service`)

> **Generic Domain-Agnostic Time Management and Delayed Execution Engine for the PERC Platform.**

---

## Overview

The **PERC Scheduler Engine** provides high-precision delayed execution and timer management for distributed operations across the PERC platform (Follow-up Engine, Meeting Engine, Workflow Engine, etc.).

It acts strictly as an infrastructure time management primitive:
- **Zero domain/business logic**
- **Opaque payload preservation** (stored in PostgreSQL `JSONB` and returned with 100% semantic fidelity)
- **Transactional Outbox Pattern** guaranteeing at-least-once delivery
- **ProcessedEvent Idempotency** preventing duplicate execution
- **Startup Crash Recovery** ensuring zero missed timers across service or Redis restarts

---

## Architecture Flow

```
Workflow / Follow-up / Meeting Engines
               │
               │ (Kafka Command)
               ▼
┌────────────────────────────────────────────────────────┐
│               PERC SCHEDULER ENGINE                    │
│                                                        │
│  Kafka Consumer (Group: perc-scheduler-group)          │
│         │                                              │
│         ▼                                              │
│  PostgreSQL (Supabase) ──[Atomic Tx]──► ProcessedEvent │
│         │ (Timer: PENDING)                             │
│         ▼                                              │
│  Redis 7 / BullMQ (Sanitized Deterministic Job ID)     │
│         │ (Delay countdown)                            │
│         ▼                                              │
│  Timer Worker (Matures)                                │
│         │                                              │
│         ▼                                              │
│  PostgreSQL (Atomic Transaction):                      │
│     ├─ Timer: EXECUTED                                 │
│     └─ OutboxEvent: PENDING                            │
│         │                                              │
│         ▼                                              │
│  Transactional Outbox Publisher (500ms batch loop)     │
│         │                                              │
│         ▼                                              │
│     Kafka Publish                                      │
│     (Topic: perc.scheduler.timer-triggered)           │
│         │                                              │
│         ▼                                              │
│  PostgreSQL: OutboxEvent = PUBLISHED                   │
└────────────────────────────────────────────────────────┘
               │
               │ (Kafka Event)
               ▼
        Domain Consumers
```

---

## Canonical Kafka Contract

### Input Topics
- `perc.scheduler.timer-schedule-requested` — Schedule new timer or reactivate cancelled/executed timer
- `perc.scheduler.timer-cancel-requested` — Cancel timer by exact `timerKey` or prefix `timerKeyPrefix`
- `perc.scheduler.timer-reschedule-requested` — Atomically update execution target timestamp & payload

### Output Topic
- `perc.scheduler.timer-triggered` — Published upon timer maturity and execution

### Dead Letter Queues (DLQ)
- `perc.scheduler.commands.dlq` — Malformed JSON or invalid schema command messages
- `perc.scheduler.timer-triggered.dlq` — Outbox events exceeding max publication retries

---

## Tech Stack & Dependencies

- **Runtime**: Node.js 22 LTS / TypeScript 5.8
- **Framework**: NestJS 11
- **Database & ORM**: PostgreSQL (Supabase) + Prisma ORM 6
- **Queue / Delay Engine**: BullMQ 5 + Redis 7.2
- **Event Streaming**: Apache Kafka (KRaft mode) + KafkaJS 2

---

## Getting Started

### 1. Environment Setup

Copy `.env.example` to `.env` and fill in your connection variables:

```bash
cp .env.example .env
```

### 2. Run Locally via Docker Compose

```bash
# Start Redis 7.2, Kafka KRaft, and Scheduler Service
docker compose up -d --build

# View container logs
docker compose logs -f scheduler-service
```

### 3. Verify Health Probes

```bash
# Liveness Probe
curl http://localhost:3000/health/liveness

# Readiness Probe (Checks PostgreSQL + Kafka connectivity)
curl http://localhost:3000/health/readiness

# Swagger Documentation
open http://localhost:3000/api/docs
```

---

## Testing Matrix

```bash
# Run unit tests
npm test

# Run full E2E integration test suite
npm run test:e2e

# Run containerized smoke test
node scripts/smoke-test.js
```

---

## Production Migrations

Database schema changes must be applied strictly via Prisma Migrate:

```bash
npx prisma migrate deploy
```
