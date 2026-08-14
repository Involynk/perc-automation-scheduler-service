import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PrismaTimerRepository } from '../persistence/prisma-timer.repository';
import { TimerJobData } from './timer-queue.service';

@Processor('perc-scheduler-delay-queue', {
  concurrency: 20,
  lockDuration: 30000,
})
export class TimerWorkerProcessor extends WorkerHost {
  private readonly logger = new Logger(TimerWorkerProcessor.name);

  constructor(
    private readonly timerRepo: PrismaTimerRepository,
    private readonly configService: ConfigService,
  ) {
    super();
  }

  async process(job: Job<TimerJobData>): Promise<void> {
    const { timerId, timerKey } = job.data;
    const targetTopic = this.configService.get<string>('topics.timerTriggered', 'perc.scheduler.timer-triggered');

    this.logger.log(`Worker processing matured timer: ${timerKey} (ID: ${timerId})`);

    // 1. Transactionally update timer state to EXECUTED and create outbox_events record
    const result = await this.timerRepo.executeTimerAtomically(
      timerId,
      targetTopic,
      'TIMER_TRIGGERED',
    );

    if (!result) {
      this.logger.warn(`Timer ${timerKey} (${timerId}) skipped: Record missing or not in PENDING status.`);
      return;
    }

    this.logger.log(`Timer ${timerKey} marked EXECUTED and written to outbox (Outbox ID: ${result.outboxId}).`);
  }
}
