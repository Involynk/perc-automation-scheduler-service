import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Producer, SASLOptions } from 'kafkajs';

@Injectable()
export class SchedulerKafkaProducer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SchedulerKafkaProducer.name);
  private kafka: Kafka;
  private producer: Producer;
  private isConnected = false;

  constructor(private readonly configService: ConfigService) {
    const brokers = this.configService.get<string[]>('kafka.brokers', ['localhost:9092']);
    const clientId = this.configService.get<string>('kafka.clientId', 'perc-scheduler-service');
    const ssl = this.configService.get<boolean>('kafka.ssl', false);
    const saslMechanism = this.configService.get<string>('kafka.saslMechanism');

    let sasl: SASLOptions | undefined = undefined;
    if (saslMechanism) {
      sasl = {
        mechanism: saslMechanism as any,
        username: this.configService.get<string>('kafka.saslUsername', ''),
        password: this.configService.get<string>('kafka.saslPassword', ''),
      };
    }

    this.kafka = new Kafka({
      clientId,
      brokers,
      ssl,
      sasl,
    });

    this.producer = this.kafka.producer({
      allowAutoTopicCreation: true,
    });
  }

  async onModuleInit() {
    try {
      await this.producer.connect();
      this.isConnected = true;
      this.logger.log('Scheduler Kafka Producer connected successfully.');
    } catch (error) {
      this.logger.error('Failed to connect Kafka Producer (Broker may be unreachable). Continuing without Kafka producer.', error);
      this.isConnected = false;
    }
  }

  async onModuleDestroy() {
    if (this.isConnected) {
      await this.producer.disconnect();
      this.isConnected = false;
      this.logger.log('Scheduler Kafka Producer disconnected.');
    }
  }

  getIsConnected(): boolean {
    return this.isConnected;
  }

  async publish(topic: string, key: string, payload: Record<string, any>): Promise<void> {
    try {
      await this.producer.send({
        topic,
        messages: [
          {
            key,
            value: JSON.stringify(payload),
            timestamp: Date.now().toString(),
          },
        ],
      });
      this.logger.log(`Published event to topic: ${topic} [Key: ${key}]`);
    } catch (error) {
      this.logger.error(`Error publishing event to Kafka [Topic: ${topic}]:`, error);
      throw error;
    }
  }

  async publishToDlq(topic: string, key: string, errorPayload: Record<string, any>): Promise<void> {
    try {
      await this.producer.send({
        topic,
        messages: [
          {
            key,
            value: JSON.stringify(errorPayload),
            timestamp: Date.now().toString(),
          },
        ],
      });
      this.logger.warn(`Event routed to DLQ topic: ${topic} [Key: ${key}]`);
    } catch (error) {
      this.logger.error(`Failed to route event to DLQ [Topic: ${topic}]:`, error);
    }
  }
}
