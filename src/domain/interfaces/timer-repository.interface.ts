import { TimerEntity } from '../entities/timer.entity';
import { TimerStatus } from '../enums/timer-status.enum';

export interface CreateTimerDto {
  timerKey: string;
  requestingService: string;
  correlationId: string;
  targetExecutionTime: Date;
  opaquePayload: Record<string, any>;
}

export interface ITimerRepository {
  createOrUpsertPending(data: CreateTimerDto, eventId?: string): Promise<{ timer: TimerEntity; isDuplicate: boolean }>;
  cancelByKey(timerKey: string, reason?: string, eventId?: string): Promise<{ cancelledKeys: string[]; isDuplicate: boolean }>;
  cancelByPrefix(prefix: string, reason?: string, eventId?: string): Promise<{ cancelledKeys: string[]; isDuplicate: boolean }>;
  rescheduleByKey(
    timerKey: string,
    newTargetExecutionTime: Date,
    newPayload?: Record<string, any>,
    eventId?: string,
  ): Promise<{ timer: TimerEntity; isDuplicate: boolean }>;
  findByKey(timerKey: string): Promise<TimerEntity | null>;
  findPendingOrRecent(horizonDate: Date): Promise<TimerEntity[]>;
  executeTimerAtomically(
    timerId: string,
    topic: string,
    eventType: string,
  ): Promise<{ timer: TimerEntity; outboxId: string } | null>;
}
