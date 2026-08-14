import { TimerStatus } from '../enums/timer-status.enum';

export interface TimerEntity {
  id: string;
  timerKey: string;
  requestingService: string;
  correlationId: string;
  targetExecutionTime: Date;
  status: TimerStatus;
  opaquePayload: Record<string, any>;
  executedAt?: Date | null;
  cancelledAt?: Date | null;
  cancelReason?: string | null;
  retryCount: number;
  lastError?: string | null;
  createdAt: Date;
  updatedAt: Date;
}
