import { Module, forwardRef } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { SchedulerKafkaProducer } from './scheduler-kafka.producer';
import { SchedulerKafkaConsumer } from './scheduler-kafka.consumer';
import { TimerService } from '../../application/services/timer.service';
import { PrismaTimerRepository } from '../persistence/prisma-timer.repository';
import { PrismaService } from '../persistence/prisma.service';
import { BullMQModule } from '../bullmq/bullmq.module';

@Module({
  imports: [ConfigModule, forwardRef(() => BullMQModule)],
  providers: [
    SchedulerKafkaProducer,
    SchedulerKafkaConsumer,
    TimerService,
    PrismaTimerRepository,
    PrismaService,
  ],
  exports: [SchedulerKafkaProducer, SchedulerKafkaConsumer],
})
export class KafkaModule {}
