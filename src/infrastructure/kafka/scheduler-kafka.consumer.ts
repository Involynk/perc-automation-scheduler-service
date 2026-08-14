import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';
import { TimerService } from '../../application/services/timer.service';
import { ScheduleTimerCommand } from '../../application/commands/schedule-timer.command';
import { CancelTimerCommand } from '../../application/commands/cancel-timer.command';
import { RescheduleTimerCommand } from '../../application/commands/reschedule-timer.command';
import { SchedulerKafkaProducer } from './scheduler-kafka.producer';

@Injectable()
export class SchedulerKafkaConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SchedulerKafkaConsumer.name);
  private kafka: Kafka;
  private consumer: Consumer;
  private isConnected = false;

  constructor(
    private readonly configService: ConfigService,
    private readonly timerService: TimerService,
    private readonly kafkaProducer: SchedulerKafkaProducer,
  ) {
    const brokers = this.configService.get<string[]>('kafka.brokers', ['localhost:9092']);
    const clientId = this.configService.get<string>('kafka.clientId', 'perc-scheduler-service');
    const groupId = this.configService.get<string>('kafka.groupId', 'perc-scheduler-group');
    const ssl = this.configService.get<boolean>('kafka.ssl', false);

    this.kafka = new Kafka({
      clientId: `${clientId}-consumer`,
      brokers,
      ssl,
    });

    this.consumer = this.kafka.consumer({ groupId });
  }

  async onModuleInit() {
    try {
      await this.consumer.connect();
      this.isConnected = true;
      this.logger.log('Scheduler Kafka Consumer connected.');

      const scheduleTopic = this.configService.get<string>(
        'topics.scheduleRequested',
        'perc.scheduler.timer-schedule-requested',
      );
      const cancelTopic = this.configService.get<string>(
        'topics.cancelRequested',
        'perc.scheduler.timer-cancel-requested',
      );
      const rescheduleTopic = this.configService.get<string>(
        'topics.rescheduleRequested',
        'perc.scheduler.timer-reschedule-requested',
      );

      await this.consumer.subscribe({
        topics: [scheduleTopic, cancelTopic, rescheduleTopic],
        fromBeginning: false,
      });

      await this.consumer.run({
        eachMessage: async (payload: EachMessagePayload) => {
          await this.handleMessage(payload);
        },
      });

      this.logger.log(`Subscribed to Kafka topics: ${scheduleTopic}, ${cancelTopic}, ${rescheduleTopic}`);
    } catch (error) {
      this.logger.error('Failed to initialize Kafka Consumer:', error);
      this.isConnected = false;
      throw error;
    }
  }

  async onModuleDestroy() {
    if (this.isConnected) {
      await this.consumer.disconnect();
      this.isConnected = false;
      this.logger.log('Scheduler Kafka Consumer disconnected.');
    }
  }

  getIsConnected(): boolean {
    return this.isConnected;
  }

  private async handleMessage({ topic, message }: EachMessagePayload): Promise<void> {
    const rawValue = message.value?.toString();
    const messageKey = message.key?.toString() || 'unknown';

    if (!rawValue) {
      this.logger.warn(`Received empty message on topic ${topic}`);
      return;
    }

    let payload: any;
    try {
      payload = JSON.parse(rawValue);
    } catch (parseError: any) {
      this.logger.error(`JSON Parse error on topic ${topic}:`, parseError);
      const dlqTopic = this.configService.get<string>('topics.commandsDlq', 'perc.scheduler.commands.dlq');
      await this.kafkaProducer.publishToDlq(dlqTopic, messageKey, {
        rawMessage: rawValue,
        error: 'INVALID_JSON_PAYLOAD',
        failedAt: new Date().toISOString(),
      });
      return;
    }

    const scheduleTopic = this.configService.get<string>('topics.scheduleRequested');
    const cancelTopic = this.configService.get<string>('topics.cancelRequested');
    const rescheduleTopic = this.configService.get<string>('topics.rescheduleRequested');

    try {
      if (topic === scheduleTopic) {
        await this.handleSchedule(payload);
      } else if (topic === cancelTopic) {
        await this.handleCancel(payload);
      } else if (topic === rescheduleTopic) {
        await this.handleReschedule(payload);
      } else {
        this.logger.warn(`Unhandled topic received: ${topic}`);
      }
    } catch (handlerError: any) {
      this.logger.error(`Error processing message from topic ${topic}:`, handlerError);
      const dlqTopic = this.configService.get<string>('topics.commandsDlq', 'perc.scheduler.commands.dlq');
      await this.kafkaProducer.publishToDlq(dlqTopic, payload.correlationId || messageKey, {
        topic,
        payload,
        error: handlerError.message,
        failedAt: new Date().toISOString(),
      });
    }
  }

  private async handleSchedule(payload: any): Promise<void> {
    if (!payload.timerKey || !payload.targetExecutionTime || !payload.requestingService || !payload.correlationId) {
      throw new Error('Missing required fields for timer-schedule-requested event.');
    }

    const targetDate = new Date(payload.targetExecutionTime);
    if (isNaN(targetDate.getTime())) {
      throw new Error(`Invalid targetExecutionTime format: ${payload.targetExecutionTime}`);
    }

    const command = new ScheduleTimerCommand(
      payload.eventId,
      payload.timerKey,
      targetDate,
      payload.requestingService,
      payload.correlationId,
      payload.opaquePayload || {},
    );

    await this.timerService.scheduleTimer(command);
  }

  private async handleCancel(payload: any): Promise<void> {
    if (!payload.timerKey && !payload.timerKeyPrefix) {
      throw new Error('Either timerKey or timerKeyPrefix is required for timer-cancel-requested event.');
    }

    const command = new CancelTimerCommand(
      payload.eventId,
      payload.timerKey,
      payload.timerKeyPrefix,
      payload.requestingService || 'unknown',
      payload.reason,
    );

    await this.timerService.cancelTimer(command);
  }

  private async handleReschedule(payload: any): Promise<void> {
    if (!payload.timerKey || !payload.newTargetExecutionTime) {
      throw new Error('Missing required fields for timer-reschedule-requested event.');
    }

    const newTargetDate = new Date(payload.newTargetExecutionTime);
    if (isNaN(newTargetDate.getTime())) {
      throw new Error(`Invalid newTargetExecutionTime format: ${payload.newTargetExecutionTime}`);
    }

    const command = new RescheduleTimerCommand(
      payload.eventId,
      payload.timerKey,
      newTargetDate,
      payload.requestingService || 'unknown',
      payload.correlationId || 'unknown',
      payload.updatedOpaquePayload,
    );

    await this.timerService.rescheduleTimer(command);
  }
}
