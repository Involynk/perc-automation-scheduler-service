import { Injectable, Logger, ConflictException, NotFoundException } from '@nestjs/common';
import { PrismaService } from './prisma.service';
import { CreateTimerDto, ITimerRepository } from '../../domain/interfaces/timer-repository.interface';
import { TimerEntity } from '../../domain/entities/timer.entity';
import { TimerStatus } from '../../domain/enums/timer-status.enum';
import { OutboxStatus } from '../../domain/enums/outbox-status.enum';
import { v4 as uuidv4 } from 'uuid';

const TRANSACTION_OPTIONS = {
  maxWait: 15000,
  timeout: 30000,
};

@Injectable()
export class PrismaTimerRepository implements ITimerRepository {
  private readonly logger = new Logger(PrismaTimerRepository.name);

  constructor(private readonly prisma: PrismaService) {}

  async createOrUpsertPending(
    data: CreateTimerDto,
    eventId?: string,
  ): Promise<{ timer: TimerEntity; isDuplicate: boolean }> {
    return this.prisma.$transaction(async (tx) => {
      // 1. ProcessedEvent idempotency check if eventId is supplied
      if (eventId) {
        const alreadyProcessed = await tx.processedEvent.findUnique({ where: { eventId } });
        if (alreadyProcessed) {
          this.logger.warn(`Duplicate eventId ${eventId} detected. Idempotently ignoring schedule command.`);
          const existing = await tx.timer.findUnique({ where: { timerKey: data.timerKey } });
          if (existing) {
            return { timer: this.mapToEntity(existing), isDuplicate: true };
          }
        }
        try {
          await tx.processedEvent.create({
            data: {
              eventId,
              consumerGroup: 'perc-scheduler-group',
            },
          });
        } catch (error: any) {
          if (error.code === 'P2002') {
            const existing = await tx.timer.findUnique({ where: { timerKey: data.timerKey } });
            if (existing) return { timer: this.mapToEntity(existing), isDuplicate: true };
          }
          throw error;
        }
      }

      // 2. Query for existing timer
      const existing = await tx.timer.findUnique({
        where: { timerKey: data.timerKey },
      });

      if (existing) {
        // Update target execution time and payload for existing pending, cancelled, or executed timer
        const updated = await tx.timer.update({
          where: { id: existing.id },
          data: {
            requestingService: data.requestingService,
            correlationId: data.correlationId,
            targetExecutionTime: data.targetExecutionTime,
            opaquePayload: data.opaquePayload,
            status: TimerStatus.PENDING,
            executedAt: null,
            cancelledAt: null,
            cancelReason: null,
            retryCount: 0,
            lastError: null,
          },
        });
        return { timer: this.mapToEntity(updated), isDuplicate: false };
      }

      // 3. Insert new timer
      const created = await tx.timer.create({
        data: {
          timerKey: data.timerKey,
          requestingService: data.requestingService,
          correlationId: data.correlationId,
          targetExecutionTime: data.targetExecutionTime,
          status: TimerStatus.PENDING,
          opaquePayload: data.opaquePayload,
        },
      });

      return { timer: this.mapToEntity(created), isDuplicate: false };
    }, TRANSACTION_OPTIONS);
  }

  async cancelByKey(
    timerKey: string,
    reason?: string,
    eventId?: string,
  ): Promise<{ cancelledKeys: string[]; isDuplicate: boolean }> {
    return this.prisma.$transaction(async (tx) => {
      if (eventId) {
        const alreadyProcessed = await tx.processedEvent.findUnique({ where: { eventId } });
        if (alreadyProcessed) {
          this.logger.warn(`Duplicate eventId ${eventId} on cancel. Ignoring.`);
          return { cancelledKeys: [timerKey], isDuplicate: true };
        }
        try {
          await tx.processedEvent.create({
            data: {
              eventId,
              consumerGroup: 'perc-scheduler-group',
            },
          });
        } catch (error: any) {
          if (error.code === 'P2002') {
            return { cancelledKeys: [timerKey], isDuplicate: true };
          }
          throw error;
        }
      }

      const existing = await tx.timer.findUnique({
        where: { timerKey },
      });

      if (!existing || existing.status !== TimerStatus.PENDING) {
        this.logger.log(`Cancel no-op: Timer ${timerKey} not found or not in PENDING status.`);
        return { cancelledKeys: [], isDuplicate: false };
      }

      await tx.timer.update({
        where: { id: existing.id },
        data: {
          status: TimerStatus.CANCELLED,
          cancelledAt: new Date(),
          cancelReason: reason || 'CANCEL_REQUESTED',
        },
      });

      return { cancelledKeys: [timerKey], isDuplicate: false };
    }, TRANSACTION_OPTIONS);
  }

  async cancelByPrefix(
    prefix: string,
    reason?: string,
    eventId?: string,
  ): Promise<{ cancelledKeys: string[]; isDuplicate: boolean }> {
    return this.prisma.$transaction(async (tx) => {
      if (eventId) {
        const alreadyProcessed = await tx.processedEvent.findUnique({ where: { eventId } });
        if (alreadyProcessed) {
          this.logger.warn(`Duplicate eventId ${eventId} on prefix cancel. Ignoring.`);
          return { cancelledKeys: [], isDuplicate: true };
        }
        try {
          await tx.processedEvent.create({
            data: {
              eventId,
              consumerGroup: 'perc-scheduler-group',
            },
          });
        } catch (error: any) {
          if (error.code === 'P2002') {
            return { cancelledKeys: [], isDuplicate: true };
          }
          throw error;
        }
      }

      const matchingTimers = await tx.timer.findMany({
        where: {
          timerKey: { startsWith: prefix },
          status: TimerStatus.PENDING,
        },
      });

      if (matchingTimers.length === 0) {
        return { cancelledKeys: [], isDuplicate: false };
      }

      const keys = matchingTimers.map((t) => t.timerKey);

      await tx.timer.updateMany({
        where: {
          id: { in: matchingTimers.map((t) => t.id) },
        },
        data: {
          status: TimerStatus.CANCELLED,
          cancelledAt: new Date(),
          cancelReason: reason || `BULK_CANCEL_PREFIX:${prefix}`,
        },
      });

      return { cancelledKeys: keys, isDuplicate: false };
    }, TRANSACTION_OPTIONS);
  }

  async rescheduleByKey(
    timerKey: string,
    newTargetExecutionTime: Date,
    newPayload?: Record<string, any>,
    eventId?: string,
  ): Promise<{ timer: TimerEntity; isDuplicate: boolean }> {
    return this.prisma.$transaction(async (tx) => {
      if (eventId) {
        const alreadyProcessed = await tx.processedEvent.findUnique({ where: { eventId } });
        if (alreadyProcessed) {
          this.logger.warn(`Duplicate eventId ${eventId} on reschedule. Ignoring.`);
          const existing = await tx.timer.findUnique({ where: { timerKey } });
          if (existing) return { timer: this.mapToEntity(existing), isDuplicate: true };
        }
        try {
          await tx.processedEvent.create({
            data: {
              eventId,
              consumerGroup: 'perc-scheduler-group',
            },
          });
        } catch (error: any) {
          if (error.code === 'P2002') {
            const existing = await tx.timer.findUnique({ where: { timerKey } });
            if (existing) return { timer: this.mapToEntity(existing), isDuplicate: true };
          }
          throw error;
        }
      }

      const existing = await tx.timer.findUnique({
        where: { timerKey },
      });

      if (!existing) {
        throw new NotFoundException(`Timer with key ${timerKey} does not exist.`);
      }

      if (existing.status === TimerStatus.EXECUTED) {
        throw new ConflictException(`Cannot reschedule already EXECUTED timer ${timerKey}.`);
      }

      const updated = await tx.timer.update({
        where: { id: existing.id },
        data: {
          targetExecutionTime: newTargetExecutionTime,
          opaquePayload: newPayload !== undefined ? newPayload : existing.opaquePayload,
          status: TimerStatus.PENDING,
          cancelledAt: null,
          cancelReason: null,
        },
      });

      return { timer: this.mapToEntity(updated), isDuplicate: false };
    }, TRANSACTION_OPTIONS);
  }

  async findByKey(timerKey: string): Promise<TimerEntity | null> {
    const record = await this.prisma.timer.findUnique({ where: { timerKey } });
    return record ? this.mapToEntity(record) : null;
  }

  async findPendingOrRecent(horizonDate: Date): Promise<TimerEntity[]> {
    const records = await this.prisma.timer.findMany({
      where: {
        OR: [
          { status: TimerStatus.PENDING },
          { updatedAt: { gte: horizonDate } },
        ],
      },
    });
    return records.map(this.mapToEntity);
  }

  async executeTimerAtomically(
    timerId: string,
    topic: string,
    eventType: string,
  ): Promise<{ timer: TimerEntity; outboxId: string } | null> {
    return this.prisma.$transaction(async (tx) => {
      const record = await tx.timer.findUnique({
        where: { id: timerId },
      });

      if (!record || record.status !== TimerStatus.PENDING) {
        return null;
      }

      const now = new Date();

      // 1. Transition timer status to EXECUTED
      const updatedTimer = await tx.timer.update({
        where: { id: timerId },
        data: {
          status: TimerStatus.EXECUTED,
          executedAt: now,
        },
      });

      // 2. Prepare payload for perc.scheduler.timer-triggered
      const outboxEventId = uuidv4();
      const triggerPayload = {
        eventId: outboxEventId,
        eventType,
        timerId: updatedTimer.id,
        timerKey: updatedTimer.timerKey,
        requestingService: updatedTimer.requestingService,
        targetService: (updatedTimer.opaquePayload as any)?.targetService || updatedTimer.requestingService,
        correlationId: updatedTimer.correlationId,
        opaquePayload: updatedTimer.opaquePayload,
        firedAt: now.toISOString(),
      };

      // 3. Atomically write to OutboxEvent table
      const outbox = await tx.outboxEvent.create({
        data: {
          eventId: outboxEventId,
          topic,
          kafkaKey: updatedTimer.correlationId,
          eventType,
          payload: triggerPayload,
          status: OutboxStatus.PENDING,
        },
      });

      return {
        timer: this.mapToEntity(updatedTimer),
        outboxId: outbox.id,
      };
    }, TRANSACTION_OPTIONS);
  }

  private mapToEntity(record: any): TimerEntity {
    return {
      id: record.id,
      timerKey: record.timerKey,
      requestingService: record.requestingService,
      correlationId: record.correlationId,
      targetExecutionTime: record.targetExecutionTime,
      status: record.status as TimerStatus,
      opaquePayload: record.opaquePayload as Record<string, any>,
      executedAt: record.executedAt,
      cancelledAt: record.cancelledAt,
      cancelReason: record.cancelReason,
      retryCount: record.retryCount,
      lastError: record.lastError,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
    };
  }
}
