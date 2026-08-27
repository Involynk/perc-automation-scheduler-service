import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { PrismaTimerRepository } from '../../infrastructure/persistence/prisma-timer.repository';
import { TimerQueueService } from '../../infrastructure/bullmq/timer-queue.service';
import { ScheduleTimerCommand } from '../commands/schedule-timer.command';
import { CancelTimerCommand } from '../commands/cancel-timer.command';
import { RescheduleTimerCommand } from '../commands/reschedule-timer.command';
import { TimerResponseDto, CancelResponseDto } from '../dto/timer-response.dto';
import { TimerEntity } from '../../domain/entities/timer.entity';

@Injectable()
export class TimerService {
  private readonly logger = new Logger(TimerService.name);

  constructor(
    private readonly timerRepo: PrismaTimerRepository,
    private readonly queueService: TimerQueueService,
  ) {}

  async scheduleTimer(command: ScheduleTimerCommand): Promise<TimerResponseDto> {
    const now = Date.now();
    const targetTimeMs = command.targetExecutionTime.getTime();
    const delayMs = Math.max(0, targetTimeMs - now);

    this.logger.log(`Scheduling timer ${command.timerKey} (Target: ${command.targetExecutionTime.toISOString()}, Delay: ${delayMs}ms)`);

    // 1. Transactionally persist in PostgreSQL with idempotency
    const { timer } = await this.timerRepo.createOrUpsertPending(
      {
        timerKey: command.timerKey,
        requestingService: command.requestingService,
        correlationId: command.correlationId,
        targetExecutionTime: command.targetExecutionTime,
        opaquePayload: command.opaquePayload,
      },
      command.eventId,
    );

    // 2. Register delayed BullMQ job (jobId = timerKey)
    await this.queueService.addDelayedTimer(timer.id, timer.timerKey, delayMs);

    return this.mapToResponseDto(timer);
  }

  async cancelTimer(command: CancelTimerCommand): Promise<CancelResponseDto> {
    if (!command.timerKey && !command.timerKeyPrefix) {
      throw new BadRequestException('Either timerKey or timerKeyPrefix must be specified for cancellation.');
    }

    let cancelledKeys: string[] = [];

    if (command.timerKey) {
      this.logger.log(`Cancelling timer ${command.timerKey}`);
      const result = await this.timerRepo.cancelByKey(command.timerKey, command.reason, command.eventId);
      cancelledKeys = result.cancelledKeys;
      await this.queueService.removeTimerJob(command.timerKey);
    } else if (command.timerKeyPrefix) {
      this.logger.log(`Cancelling timers with prefix ${command.timerKeyPrefix}`);
      const result = await this.timerRepo.cancelByPrefix(command.timerKeyPrefix, command.reason, command.eventId);
      cancelledKeys = result.cancelledKeys;
      for (const key of cancelledKeys) {
        await this.queueService.removeTimerJob(key);
      }
    }

    return {
      status: 'CANCELLED',
      cancelledKeys,
      reason: command.reason,
    };
  }

  async rescheduleTimer(command: RescheduleTimerCommand): Promise<TimerResponseDto> {
    const now = Date.now();
    const targetTimeMs = command.newTargetExecutionTime.getTime();

    if (targetTimeMs <= now) {
      throw new BadRequestException('newTargetExecutionTime must be in the future.');
    }

    const delayMs = Math.max(0, targetTimeMs - now);
    this.logger.log(`Rescheduling timer ${command.timerKey} to ${command.newTargetExecutionTime.toISOString()} (Delay: ${delayMs}ms)`);

    // 1. Transactional update in DB
    const { timer } = await this.timerRepo.rescheduleByKey(
      command.timerKey,
      command.newTargetExecutionTime,
      command.updatedOpaquePayload,
      command.eventId,
    );

    // 2. Remove old delayed BullMQ job and re-enqueue new delay
    await this.queueService.addDelayedTimer(timer.id, timer.timerKey, delayMs);

    return this.mapToResponseDto(timer);
  }

  async getTimerByKey(timerKey: string): Promise<TimerResponseDto | null> {
    const timer = await this.timerRepo.findByKey(timerKey);
    return timer ? this.mapToResponseDto(timer) : null;
  }

  private mapToResponseDto(timer: TimerEntity): TimerResponseDto {
    return {
      id: timer.id,
      timerKey: timer.timerKey,
      requestingService: timer.requestingService,
      correlationId: timer.correlationId,
      targetExecutionTime: timer.targetExecutionTime.toISOString(),
      status: timer.status,
      opaquePayload: timer.opaquePayload,
      executedAt: timer.executedAt ? timer.executedAt.toISOString() : undefined,
      cancelledAt: timer.cancelledAt ? timer.cancelledAt.toISOString() : undefined,
      cancelReason: timer.cancelReason || undefined,
      createdAt: timer.createdAt.toISOString(),
      updatedAt: timer.updatedAt.toISOString(),
    };
  }
}
