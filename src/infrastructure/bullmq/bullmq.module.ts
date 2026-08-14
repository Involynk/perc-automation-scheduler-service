import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TimerQueueService } from './timer-queue.service';
import { TimerWorkerProcessor } from './timer-worker.processor';

@Module({
  imports: [
    BullModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: async (configService: ConfigService) => ({
        connection: {
          host: configService.get<string>('redis.host', 'localhost'),
          port: configService.get<number>('redis.port', 6379),
          password: configService.get<string>('redis.password'),
          db: configService.get<number>('redis.db', 0),
          maxRetriesPerRequest: null,
        },
      }),
      inject: [ConfigService],
    }),
    BullModule.registerQueue({
      name: 'perc-scheduler-delay-queue',
    }),
  ],
  providers: [TimerQueueService, TimerWorkerProcessor],
  exports: [TimerQueueService, BullModule],
})
export class BullMQModule {}
