import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';

export interface TimerJobData {
  timerId: string;
  timerKey: string;
}

@Injectable()
export class TimerQueueService {
  private readonly logger = new Logger(TimerQueueService.name);

  constructor(@InjectQueue('perc-scheduler-delay-queue') private readonly queue: Queue<TimerJobData>) {}

  private toJobId(timerKey: string): string {
    return timerKey.replace(/:/g, '__');
  }

  async addDelayedTimer(timerId: string, timerKey: string, delayMs: number): Promise<void> {
    const jobId = this.toJobId(timerKey);
    const existingJob = await this.queue.getJob(jobId);
    if (existingJob) {
      this.logger.warn(`BullMQ job with ID ${jobId} already exists. Removing old job before enqueuing.`);
      await existingJob.remove();
    }

    await this.queue.add(
      'EXECUTE_TIMER',
      { timerId, timerKey },
      {
        jobId, // Deterministic sanitized Job ID
        delay: Math.max(0, delayMs),
        removeOnComplete: true,
        removeOnFail: false,
      },
    );
    this.logger.log(`Enqueued delayed BullMQ timer: ${timerKey} [JobId: ${jobId}] (Delay: ${delayMs}ms)`);
  }

  async removeTimerJob(timerKey: string): Promise<boolean> {
    const jobId = this.toJobId(timerKey);
    const job = await this.queue.getJob(jobId);
    if (job) {
      await job.remove();
      this.logger.log(`Removed BullMQ job for timerKey: ${timerKey} [JobId: ${jobId}]`);
      return true;
    }
    return false;
  }

  async getJob(timerKey: string) {
    const jobId = this.toJobId(timerKey);
    return this.queue.getJob(jobId);
  }
}
