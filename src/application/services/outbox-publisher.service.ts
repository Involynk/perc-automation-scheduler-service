import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../infrastructure/persistence/prisma.service';
import { SchedulerKafkaProducer } from '../../infrastructure/kafka/scheduler-kafka.producer';
import { OutboxStatus } from '../../domain/enums/outbox-status.enum';

@Injectable()
export class OutboxPublisherService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxPublisherService.name);
  private timerHandle: NodeJS.Timeout | null = null;
  private isProcessing = false;
  private isRunning = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: SchedulerKafkaProducer,
    private readonly configService: ConfigService,
  ) {}

  onModuleInit() {
    const pollInterval = this.configService.get<number>('outbox.pollIntervalMs', 500);
    this.isRunning = true;
    this.timerHandle = setInterval(() => this.processOutboxBatch(), pollInterval);
    this.logger.log(`Outbox Publisher Service started (Polling every ${pollInterval}ms).`);
  }

  onModuleDestroy() {
    this.isRunning = false;
    if (this.timerHandle) {
      clearInterval(this.timerHandle);
      this.timerHandle = null;
    }
    this.logger.log('Outbox Publisher Service stopped.');
  }

  async processOutboxBatch(): Promise<void> {
    if (this.isProcessing || !this.isRunning) return;
    this.isProcessing = true;

    try {
      const batchSize = this.configService.get<number>('outbox.batchSize', 50);
      const maxRetries = this.configService.get<number>('outbox.maxRetries', 5);

      const pendingEvents = await this.prisma.outboxEvent.findMany({
        where: {
          status: OutboxStatus.PENDING,
          retryCount: { lt: maxRetries },
        },
        orderBy: { createdAt: 'asc' },
        take: batchSize,
      });

      for (const event of pendingEvents) {
        try {
          await this.kafkaProducer.publish(
            event.topic,
            event.kafkaKey,
            event.payload as Record<string, any>,
          );

          await this.prisma.outboxEvent.update({
            where: { id: event.id },
            data: {
              status: OutboxStatus.PUBLISHED,
              publishedAt: new Date(),
            },
          });
        } catch (publishError: any) {
          this.logger.error(`Failed to publish outbox event ${event.id}:`, publishError);

          const updatedRetry = event.retryCount + 1;
          const isFailed = updatedRetry >= maxRetries;

          await this.prisma.outboxEvent.update({
            where: { id: event.id },
            data: {
              retryCount: updatedRetry,
              status: isFailed ? OutboxStatus.FAILED : OutboxStatus.PENDING,
              lastError: publishError.message,
            },
          });

          if (isFailed) {
            const dlqTopic = this.configService.get<string>(
              'topics.timerTriggeredDlq',
              'perc.scheduler.timer-triggered.dlq',
            );
            await this.kafkaProducer.publishToDlq(dlqTopic, event.kafkaKey, {
              outboxId: event.id,
              originalEvent: event.payload,
              error: publishError.message,
              failedAt: new Date().toISOString(),
            });
          }
        }
      }
    } catch (error) {
      this.logger.error('Error during outbox batch processing:', error);
    } finally {
      this.isProcessing = false;
    }
  }
}
