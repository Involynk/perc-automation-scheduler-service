# PERC Scheduler Engine — Technical Specification & Low-Level Design (LLD)
**Document Version**: 1.0.0  
**Target Service**: `perc-scheduler-service`  
**Architectural Model**: Choreographed Microservices (Domain-Agnostic Timing Engine)  
**Status**: Production-Ready Design  

---

## Document Structure
```
SCHEDULER ENGINE
│
├── 1. Purpose
├── 2. Responsibilities
├── 3. Non-responsibilities
├── 4. Architecture
├── 5. Database model
├── 6. Kafka input contracts
│    ├── Schedule
│    ├── Cancel
│    └── Reschedule
├── 7. Kafka output contract
│    └── Timer Triggered
├── 8. BullMQ/Redis flow
├── 9. Retry & DLQ
├── 10. Failure scenarios
├── 11. Recovery
├── 12. REST APIs
├── 13. Idempotency
├── 14. Security
├── 15. Testing strategy
└── 16. Deployment architecture
```

---

## 1. Purpose

The Scheduler Engine is a dedicated, domain-agnostic platform microservice responsible for **generic time management and delayed execution**. 

In the PERC ecosystem, multiple upstream services require temporal operations (e.g., waiting 2 hours before a follow-up evaluation, triggering a meeting reminder at $T - 2\text{ hours}$, or alerting a counselor after 24 hours). Instead of each engine implementing decentralized timer loops, cron jobs, or database polling, the Scheduler Engine provides a single, highly reliable, fault-tolerant timing primitive.

The Scheduler determines **WHEN** an action should execute. Upstream domain services determine **WHY** it is needed, **WHAT** data is exchanged, and **WHETHER** the action is still valid upon execution.

---

## 2. Responsibilities

* **Execute Delayed Operations**: Schedule arbitrary tasks to run at a specific future timestamp (or after a relative delay).
* **Schedule Timers**: Register time-based triggers for follow-up cadences, reminders, and escalations.
* **Cancel Active Timers**: Cancel individual timers by `timerKey` or groups of timers by `timerKeyPrefix` (e.g., when a lead replies or an admission is completed).
* **Reschedule Timers**: Update target execution timestamps and replace payload data in-place without race conditions.
* **Trigger Scheduled Events**: Publish domain-agnostic `perc.scheduler.timer-triggered` events containing the exact opaque payload originally provided.
* **Retry Failed Execution Dispatches**: Re-attempt Kafka publishing or worker processing upon transient network failures.
* **Recover Pending Timers**: Automatically reconcile and re-enqueue active timers from PostgreSQL into Redis/BullMQ upon service reboot or Redis crash.

---

## 3. Non-Responsibilities

The Scheduler Engine follows the **Single Responsibility Principle** and contains **ZERO PERC business logic**:

* **MUST NOT evaluate lead state machines or statuses** (owned by Workflow Engine).
* **MUST NOT evaluate follow-up cadences or inactivity rules** (owned by Follow-up Engine).
* **MUST NOT decide if a lead should or should not be contacted** (owned by Follow-up / Workflow Engines).
* **MUST NOT generate, format, or send WhatsApp, SMS, or Email messages** (owned by Notification Engine).
* **MUST NOT apply calendar availability, working hours, or counselor booking rules** (owned by Call & Meeting Engine).
* **MUST NOT evaluate admission fees or course criteria** (owned by Workflow / Response Template Engines).
* **MUST NOT parse, mutate, or validate the inner contents of `opaquePayload`**.

---

## 4. Architecture

The Scheduler Engine operates in a fully decentralized event-driven choreography without a central orchestrator.

```
                           KAFKA MESSAGE BUS
 ┌─────────────────────────────────────────────────────────────────┐
 │                                                                 │
 │  perc.scheduler.timer-schedule-requested                        │
 │  perc.scheduler.timer-cancel-requested                          │
 │  perc.scheduler.timer-reschedule-requested                      │
 └────────────────────────────────┬────────────────────────────────┘
                                  │ (Consumes Commands)
                                  ▼
                 ┌────────────────────────────────┐
                 │       Scheduler Service        │
                 │ ┌────────────────────────────┐ │
                 │ │     NestJS Application     │ │
                 │ │ ┌────────────┐ ┌─────────┐ │ │
                 │ │ │ PostgreSQL │ │  Redis  │ │ │
                 │ │ │  (Prisma)  │ │ (BullMQ)│ │ │
                 │ │ └────────────┘ └─────────┘ │ │
                 │ └────────────────────────────┘ │
                 └────────────────┬───────────────┘
                                  │ (Produces Triggers)
                                  ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │  perc.scheduler.timer-triggered                                 │
 └────────────────────────────────┬────────────────────────────────┘
                                  │ (Consumes Triggers)
                                  ▼
          Domain Engines (Workflow, Follow-up, Call & Meeting)
```

### 4.1 Internal Architecture Components
1. **Presentation Layer**: Kafka Command Listeners + REST Controllers.
2. **Application Layer**: Use Cases / Command Handlers (`ScheduleTimerUseCase`, `CancelTimerUseCase`, `RescheduleTimerUseCase`, `TriggerTimerUseCase`).
3. **Domain Layer**: `Timer` Entity, `TimerStatus` Enum, Repository Interfaces.
4. **Infrastructure Layer**:
   - **PostgreSQL (Prisma ORM)**: Authoritative, ACID-compliant source of truth for timer persistence and audit trails.
   - **Redis + BullMQ**: Ultra-low-latency in-memory delayed queue engine with distributed worker coordination.
   - **Kafka Client**: Producer and Consumer abstraction with idempotency handling and DLQ routing.

---

## 5. Database Model (PostgreSQL / Prisma)

```prisma
datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

generator client {
  provider = "prisma-client-js"
}

enum TimerStatus {
  PENDING
  EXECUTED
  CANCELLED
  FAILED
}

model Timer {
  id                  String       @id @default(uuid()) @db.Uuid
  timerKey            String       @map("timer_key") @db.VarChar(255)
  requestingService   String       @map("requesting_service") @db.VarChar(64)
  correlationId       String       @map("correlation_id") @db.VarChar(128)
  targetExecutionTime DateTime     @map("target_execution_time") @db.Timestamptz(6)
  status              TimerStatus  @default(PENDING)
  opaquePayload       Json         @map("opaque_payload") @db.JsonB
  
  // Execution metadata
  executedAt          DateTime?    @map("executed_at") @db.Timestamptz(6)
  cancelledAt         DateTime?    @map("cancelled_at") @db.Timestamptz(6)
  cancelReason        String?      @map("cancel_reason") @db.VarChar(255)
  retryCount          Int          @default(0) @map("retry_count")
  lastError           String?      @map("last_error") @db.Text
  
  createdAt           DateTime     @default(now()) @map("created_at") @db.Timestamptz(6)
  updatedAt           DateTime     @updatedAt @map("updated_at") @db.Timestamptz(6)

  @@index([status, targetExecutionTime], name: "idx_timers_pending_recovery")
  @@index([timerKey], name: "idx_timers_timer_key")
  @@index([correlationId], name: "idx_timers_correlation_id")
  @@index([timerKey, status], name: "idx_timers_key_status")
  @@map("timers")
}

model ProcessedEvent {
  eventId       String   @id @map("event_id") @db.Uuid
  consumerGroup String   @map("consumer_group") @db.VarChar(64)
  processedAt   DateTime @default(now()) @map("processed_at") @db.Timestamptz(6)

  @@index([processedAt], name: "idx_processed_events_cleanup")
  @@map("processed_events")
}
```

### 5.1 Index Rationale & Query Patterns
* **`idx_timers_pending_recovery` (`status, targetExecutionTime`)**: Powers crash-recovery scans during boot (`WHERE status = 'PENDING' AND target_execution_time <= NOW() + INTERVAL '24 hours'`).
* **`idx_timers_key_status` (`timerKey, status`)**: High-speed lookup for cancel and reschedule commands.
* **`idx_timers_correlation_id` (`correlationId`)**: Fast filtering for lead/meeting entity lifecycle tracking.
* **`ProcessedEvent` (`eventId`)**: Primary key uniqueness ensures message idempotency.

---

## 6. Kafka Input Contracts

```
+---------------------------------------------------------------------------------------------------------+
| Topic Name                           | Producer Engines            | Consumer Group    | Partition Key  |
+--------------------------------------+-----------------------------+-------------------+----------------+
| perc.scheduler.timer-schedule-req    | workflow, followup, meeting | perc-sched-group  | correlationId  |
| perc.scheduler.timer-cancel-req      | workflow, followup, meeting | perc-sched-group  | correlationId  |
| perc.scheduler.timer-reschedule-req  | workflow, followup, meeting | perc-sched-group  | correlationId  |
+--------------------------------------+-----------------------------+-------------------+----------------+
```

### 6.1 Schedule Request (`perc.scheduler.timer-schedule-requested`)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "TimerScheduleRequestedEvent",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "eventId",
    "timerKey",
    "targetExecutionTime",
    "requestingService",
    "correlationId",
    "opaquePayload"
  ],
  "properties": {
    "eventId": { "type": "string", "format": "uuid" },
    "timerKey": { "type": "string", "minLength": 3, "maxLength": 255 },
    "targetExecutionTime": { "type": "string", "format": "date-time" },
    "delaySeconds": { "type": "integer", "minimum": 0 },
    "requestingService": { "type": "string", "enum": ["workflow-engine", "followup-engine", "meeting-engine"] },
    "correlationId": { "type": "string", "minLength": 1, "maxLength": 128 },
    "opaquePayload": { "type": "object" }
  }
}
```

* **Validation Rules**: `targetExecutionTime` must be a valid future ISO timestamp. `timerKey` must match regex `^[a-zA-Z0-9_-]+(:[a-zA-Z0-9_-]+)+$`.
* **Flow**:
  1. Validate incoming DTO.
  2. Check `processed_events` table for duplicate `eventId`.
  3. Insert record in PostgreSQL `timers` table with `status = 'PENDING'`.
  4. Compute `delayMs = max(0, targetExecutionTime - now)`.
  5. Add job to BullMQ with `jobId = timerKey` and `delay = delayMs`.
  6. Commit Kafka offset.

---

### 6.2 Cancel Request (`perc.scheduler.timer-cancel-requested`)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "TimerCancelRequestedEvent",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "eventId",
    "requestingService"
  ],
  "properties": {
    "eventId": { "type": "string", "format": "uuid" },
    "timerKey": { "type": "string" },
    "timerKeyPrefix": { "type": "string" },
    "requestingService": { "type": "string", "enum": ["workflow-engine", "followup-engine", "meeting-engine"] },
    "reason": { "type": "string", "maxLength": 255 }
  }
}
```

* **Validation Rules**: Must contain at least one of `timerKey` or `timerKeyPrefix`.
* **Flow**:
  1. If `timerKey` is provided: Update PostgreSQL `status = 'CANCELLED'` for that key; remove BullMQ job `timerKey`.
  2. If `timerKeyPrefix` is provided: Query matching keys `WHERE timer_key LIKE :prefix || '%' AND status = 'PENDING'`, update DB to `CANCELLED`, and call `bullMQ.remove(key)` for each matching key.

---

### 6.3 Reschedule Request (`perc.scheduler.timer-reschedule-requested`)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "TimerRescheduleRequestedEvent",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "eventId",
    "timerKey",
    "newTargetExecutionTime",
    "requestingService",
    "correlationId"
  ],
  "properties": {
    "eventId": { "type": "string", "format": "uuid" },
    "timerKey": { "type": "string" },
    "newTargetExecutionTime": { "type": "string", "format": "date-time" },
    "requestingService": { "type": "string", "enum": ["workflow-engine", "followup-engine", "meeting-engine"] },
    "correlationId": { "type": "string" },
    "updatedOpaquePayload": { "type": "object" }
  }
}
```

* **Flow**:
  1. Transactionally lock row: `SELECT * FROM timers WHERE timer_key = :timerKey FOR UPDATE`.
  2. If status is `EXECUTED`, reject command (cannot reschedule executed timer).
  3. Update DB with `target_execution_time = newTargetExecutionTime` and optional `updatedOpaquePayload`.
  4. Remove old job from BullMQ: `await queue.remove(timerKey)`.
  5. Enqueue new BullMQ job with new delay and `jobId = timerKey`.

---

## 7. Kafka Output Contract

```
+---------------------------------------------------------------------------------------------------------+
| Topic Name                     | Producer Service       | Target Consumer Engines       | Key Strategy  |
+--------------------------------+------------------------+-------------------------------+---------------+
| perc.scheduler.timer-triggered | perc-scheduler-service | workflow, followup, meeting   | correlationId |
+--------------------------------+------------------------+-------------------------------+---------------+
```

### 7.1 Timer Triggered Event (`perc.scheduler.timer-triggered`)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "TimerTriggeredEvent",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "eventId",
    "eventType",
    "timerId",
    "timerKey",
    "requestingService",
    "correlationId",
    "opaquePayload",
    "firedAt"
  ],
  "properties": {
    "eventId": { "type": "string", "format": "uuid" },
    "eventType": { "type": "string", "enum": ["TIMER_TRIGGERED"] },
    "timerId": { "type": "string", "format": "uuid" },
    "timerKey": { "type": "string" },
    "requestingService": { "type": "string" },
    "correlationId": { "type": "string" },
    "opaquePayload": { "type": "object" },
    "firedAt": { "type": "string", "format": "date-time" }
  }
}
```

* **Byte-for-Byte Payload Invariant**: Scheduler worker retrieves `opaquePayload` from PostgreSQL / Redis and serializes it directly into Kafka without schema filtering or mutation.

---

## 8. BullMQ / Redis Execution Flow

```
   [ Schedule Command ]
             │
             ▼
   PostgreSQL Record Created (status = PENDING)
             │
             ▼
   Redis Delayed ZSET (Key: perc-scheduler-delay-queue, Score: TargetTimestamp, JobId: timerKey)
             │
             │ [ Delay Countdown ]
             ▼
   Redis Stream / Active Worker Queue
             │
             ▼
   BullMQ Worker Processor Pops Job
             │
             ├── 1. Acquire DB Row Lock: SELECT ... FOR UPDATE
             ├── 2. Verify status == PENDING
             │      ├── IF NOT PENDING ──► Complete job immediately & Skip
             │      └── IF PENDING     ──► UPDATE status = EXECUTED, executed_at = NOW()
             ├── 3. Publish to Kafka: perc.scheduler.timer-triggered
             └── 4. BullMQ removeOnComplete: true (Purge from active memory)
```

* **Worker Concurrency**: `20` concurrent jobs per worker instance.
* **Lock Renewal**: Automatic lock extension every 15 seconds for long-running publishers.
* **Stalled Job Detection**: BullMQ monitors worker heartbeats every 30 seconds.

---

## 9. Retry Policies & Dead-Letter Queue (DLQ) Topology

```
   Inbound Kafka Command
            │
            ▼
    [ Schema Valid? ] ───NO───► [ perc.scheduler.commands.dlq ]
            │
           YES
            │
            ▼
    [ Database / Redis OK? ] ───NO (Transient) ──► Retry 3x (1s, 2s, 4s Backoff)
            │                                             │
            │                                       Exhausted?
            │                                             │
            │                                            YES ──► [ perc.scheduler.commands.dlq ]
           YES
            │
            ▼
    [ BullMQ Worker Pops Job ]
            │
            ▼
    [ Kafka Publish OK? ] ───NO───► BullMQ Worker Auto-Retry (3x with Exp. Backoff)
            │                                             │
           YES                                       Exhausted?
            │                                             │
            ▼                                            YES ──► [ perc.scheduler.timer-triggered.dlq ]
       Job Complete
```

* **Non-Retryable Errors**: JSON syntax errors, missing mandatory fields, invalid dates $\rightarrow$ published directly to `perc.scheduler.commands.dlq`.
* **Retryable Errors**: PostgreSQL connection drops, Redis timeout, Kafka producer buffer overflow $\rightarrow$ retried 3 times with exponential backoff before sending to DLQ.

---

## 10. Comprehensive Failure Scenarios & Self-Healing

1. **Kafka Command Duplicated**: Processed event store (`processed_events`) drops duplicate `eventId`; offset committed.
2. **PostgreSQL Unavailable during Scheduling**: Kafka consumer throws connection error; consumer pauses and retries with backoff. Zero data lost.
3. **Redis Unavailable during Scheduling**: Prisma transaction rolls back; Kafka offset not committed; command retries upon Redis restoration.
4. **Worker Crashes Mid-Execution**: BullMQ stalled job monitor detects lost worker and re-assigns job to another worker pod.
5. **Worker Crashes after DB Update but Before Kafka Publish**: Startup reconciliation scanner identifies orphaned `EXECUTED` records without outbound Kafka receipt and safely republishes.
6. **Cancel Arrives for Non-Existent or Already-Executed Timer**: SQL updates 0 rows; BullMQ remove is a no-op; returns success idempotently.
7. **Reschedule Races with Worker Execution**: PostgreSQL `SELECT ... FOR UPDATE` serializes the transaction: either the worker executes first (reschedule rejected) or reschedule executes first (timer delay updated).
8. **Redis Crashes & Loses Delayed ZSET**: Startup Recovery queries PostgreSQL for `PENDING` timers and re-populates Redis BullMQ queue.

---

## 11. Startup Recovery & Queue Reconciliation Engine

```typescript
@Injectable()
export class StartupRecoveryService implements OnApplicationBootstrap {
  constructor(
    private readonly prisma: PrismaService,
    @InjectQueue('perc-scheduler-delay-queue') private readonly delayQueue: Queue,
    private readonly logger: CustomLogger
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    this.logger.log('Initiating Scheduler crash recovery and reconciliation...');
    const now = new Date();

    // 1. Fetch pending timers within immediate 24-hour horizon
    const pendingTimers = await this.prisma.timer.findMany({
      where: {
        status: TimerStatus.PENDING,
        targetExecutionTime: {
          lte: new Date(now.getTime() + 24 * 60 * 60 * 1000)
        }
      }
    });

    for (const timer of pendingTimers) {
      const existingJob = await this.delayQueue.getJob(timer.timerKey);
      if (!existingJob) {
        const delayMs = Math.max(0, timer.targetExecutionTime.getTime() - Date.now());
        await this.delayQueue.add(
          'EXECUTE_TIMER',
          { timerId: timer.id, timerKey: timer.timerKey },
          { delay: delayMs, jobId: timer.timerKey, removeOnComplete: true }
        );
        this.logger.log(`Re-enqueued missing timer: ${timer.timerKey} (Delay: ${delayMs}ms)`);
      }
    }
    this.logger.log('Scheduler crash recovery complete.');
  }
}
```

---

## 12. REST APIs (Optional Fast-Path & Management)

### 12.1 `POST /api/v1/timers`
* **Purpose**: Synchronous timer creation for testing or direct HTTP clients.
* **Request**:
  ```json
  {
    "timerKey": "lead:L-101:followup:step-1",
    "targetExecutionTime": "2026-08-14T12:00:00.000Z",
    "requestingService": "followup-engine",
    "correlationId": "L-101",
    "opaquePayload": { "leadId": "L-101", "step": 1 }
  }
  ```
* **Response (201 Created)**:
  ```json
  {
    "status": "SCHEDULED",
    "timerId": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
    "timerKey": "lead:L-101:followup:step-1",
    "targetExecutionTime": "2026-08-14T12:00:00.000Z"
  }
  ```

### 12.2 `DELETE /api/v1/timers/{timerKey}`
* **Purpose**: Synchronous timer cancellation.
* **Response (200 OK)**:
  ```json
  {
    "status": "CANCELLED",
    "timerKey": "lead:L-101:followup:step-1",
    "cancelledAt": "2026-08-14T10:30:00.000Z"
  }
  ```

### 12.3 `GET /api/v1/timers/{timerKey}`
* **Purpose**: Status inspection for debugging/dashboard.
* **Response (200 OK)**:
  ```json
  {
    "timerId": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
    "timerKey": "lead:L-101:followup:step-1",
    "status": "PENDING",
    "targetExecutionTime": "2026-08-14T12:00:00.000Z",
    "requestingService": "followup-engine",
    "correlationId": "L-101",
    "opaquePayload": { "leadId": "L-101", "step": 1 }
  }
  ```

---

## 13. Idempotency Specification

* **Tier 1 (Kafka Message Deduplication)**: Incoming `eventId` is checked against the `processed_events` table within the database transaction.
* **Tier 2 (PostgreSQL State Locking)**: Enforces single active timers via `UNIQUE(timer_key, status)` logic where `status = 'PENDING'`.
* **Tier 3 (BullMQ Deduplication)**: `jobId = timerKey` prevents multiple instances of the same timer running concurrently in Redis.

---

## 14. Security & Input Constraints

* **Kafka Transport Security**: SASL_SSL with SCRAM-SHA-512 authentication.
* **Payload Size Constraints**: Maximum `opaquePayload` size = `64 KB`. Maximum `timerKey` length = `255 characters`.
* **REST Security**: Internal microservice Bearer Token authentication via API Gateway.
* **Least-Privilege Database Access**: Dedicated `perc_scheduler_user` restricted to `timers` and `processed_events` tables.

---

## 15. Testing Strategy

1. **Unit Tests (Jest)**:
   - Delay calculation validation ($T_{\text{target}} - \text{now}$).
   - DTO validation and error schema formatting.
2. **Integration Tests (Testcontainers)**:
   - Spin up real PostgreSQL and Redis containers.
   - Enqueue delay job $\rightarrow$ fast-forward time $\rightarrow$ verify worker updates status to `EXECUTED`.
   - Flush Redis $\rightarrow$ trigger `StartupRecoveryService` $\rightarrow$ verify Redis jobs restored from PostgreSQL.
3. **Kafka Contract Verification Tests**:
   - Verify that publishing `timer-schedule-requested` results in `timer-triggered` with untouched `opaquePayload`.
4. **Concurrency & Race Condition Tests**:
   - Concurrently send 50 reschedule and 50 cancel requests for the same `timerKey` and verify deterministic state resolution without deadlocks.

---

## 16. Production Deployment Architecture

```
                                 KUBERNETES CLUSTER DEPLOYMENT
                                 
                       ┌────────────────────────────────────────────────┐
                       │               Ingress Controller               │
                       └───────────────────────┬────────────────────────┘
                                               │ (Health Checks / REST)
                                               ▼
    ┌───────────────────────────────────────────────────────────────────────────────────────┐
    │                            perc-scheduler-service (Pod Replica)                       │
    │                                                                                       │
    │   ┌───────────────────────────┐                       ┌───────────────────────────┐   │
    │   │    NestJS HTTP / Kafka    │                       │    BullMQ Worker Thread   │   │
    │   │    (Consumer Instance)    │                       │    (Concurrency: 20)      │   │
    │   └─────────────┬─────────────┘                       └─────────────┬─────────────┘   │
    └─────────────────┼───────────────────────────────────────────────────┼─────────────────┘
                      │                                                   │
                      ▼                                                   ▼
    ┌───────────────────────────────────┐               ┌───────────────────────────────────┐
    │        PostgreSQL Database        │               │       Redis Master / Replica      │
    │         (Source of Truth)         │               │       (Delayed Queue Engine)      │
    └───────────────────────────────────┘               └───────────────────────────────────┘
```

* **Health Probes**:
  - **Liveness (`/health/liveness`)**: Validates Node.js runtime process health.
  - **Readiness (`/health/readiness`)**: Confirms active connections to PostgreSQL, Redis, and Kafka brokers.
* **Autoscaling (HPA)**: Scales replicas based on Kafka consumer lag ($> 100$ messages) and BullMQ waiting queue depth ($> 5000$ jobs).
* **Graceful Shutdown**:
  1. Pause Kafka consumers.
  2. Wait for active BullMQ worker jobs to finish (timeout: 15s).
  3. Close Prisma database connections and Redis socket.
