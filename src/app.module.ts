import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import configuration from './infrastructure/config/configuration';
import { PrismaModule } from './infrastructure/persistence/prisma.module';
import { BullMQModule } from './infrastructure/bullmq/bullmq.module';
import { KafkaModule } from './infrastructure/kafka/kafka.module';
import { TimerService } from './application/services/timer.service';
import { OutboxPublisherService } from './application/services/outbox-publisher.service';
import { StartupRecoveryService } from './application/services/startup-recovery.service';
import { TimerController } from './presentation/http/timer.controller';
import { HealthController } from './presentation/http/health.controller';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: [configuration],
    }),
    PrismaModule,
    BullMQModule,
    KafkaModule,
  ],
  controllers: [TimerController, HealthController],
  providers: [
    TimerService,
    OutboxPublisherService,
    StartupRecoveryService,
  ],
})
export class AppModule {}
