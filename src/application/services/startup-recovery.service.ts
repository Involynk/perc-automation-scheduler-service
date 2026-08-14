import { Injectable, OnApplicationBootstrap, Logger } from '@nestjs/common';
import { PrismaService } from '../../infrastructure/persistence/prisma.service';
import { TimerQueueService } from '../../infrastructure/bullmq/timer-queue.service';
import { TimerStatus } from '../../domain/enums/timer-status.enum';

@Injectable()
export class StartupRecoveryService implements OnApplicationBootstrap {
  private readonly logger = new Logger(StartupRecoveryService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly queueService: TimerQueueService,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    this.logger.log('Starting Scheduler crash recovery and state reconciliation...');
    await this.reconcileTimers();
  }

  async reconcileTimers(): Promise<{ reconciledCount: number }> {
    const now = new Date();
    const lookbackHorizon = new Date(now.getTime() - 24 * 60 * 60 * 1000); // 24-hour lookback for recent non-pending

    // Fetch all pending timers, plus recent cancelled/executed timers to clean stale Redis entries
    const timers = await this.prisma.timer.findMany({
      where: {
        OR: [
          { status: TimerStatus.PENDING },
          { updatedAt: { gte: lookbackHorizon } },
        ],
      },
    });

    this.logger.log(`Evaluating ${timers.length} database timer records against BullMQ state...`);
    let count = 0;

    for (const timer of timers) {
      const bullJob = await this.queueService.getJob(timer.timerKey);

      // Branch 1: CANCELLED -> Remove stale job if present
      if (timer.status === TimerStatus.CANCELLED) {
        if (bullJob) {
          await this.queueService.removeTimerJob(timer.timerKey);
          this.logger.log(`[Reconciliation] Removed stale BullMQ job for CANCELLED timer: ${timer.timerKey}`);
          count++;
        }
      }
      // Branch 2: EXECUTED -> Remove stale job if present
      else if (timer.status === TimerStatus.EXECUTED) {
        if (bullJob) {
          await this.queueService.removeTimerJob(timer.timerKey);
          this.logger.log(`[Reconciliation] Removed stale BullMQ job for EXECUTED timer: ${timer.timerKey}`);
          count++;
        }
      }
      // Branch 3: PENDING
      else if (timer.status === TimerStatus.PENDING) {
        // Sub-branch 3a: targetExecutionTime <= now (Expired/overdue) -> Ensure immediate BullMQ job exists
        if (timer.targetExecutionTime <= now) {
          this.logger.warn(`[Reconciliation] Overdue PENDING timer detected: ${timer.timerKey}. Enqueuing for immediate execution.`);
          await this.queueService.addDelayedTimer(timer.id, timer.timerKey, 0);
          count++;
        }
        // Sub-branch 3b: BullMQ job does not exist -> Enqueue with remaining delay
        else if (!bullJob) {
          const remainingDelayMs = timer.targetExecutionTime.getTime() - now.getTime();
          await this.queueService.addDelayedTimer(timer.id, timer.timerKey, remainingDelayMs);
          this.logger.log(`[Reconciliation] Re-enqueued missing BullMQ job for PENDING timer: ${timer.timerKey} (Delay: ${remainingDelayMs}ms)`);
          count++;
        }
        // Sub-branch 3c: Job already exists in BullMQ -> Leave unchanged
        else {
          // No-op
        }
      }
    }

    this.logger.log(`Reconciliation completed. Total reconciled operations: ${count}.`);
    return { reconciledCount: count };
  }
}
