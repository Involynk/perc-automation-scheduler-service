import { Controller, Get, HttpStatus, Res } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse } from '@nestjs/swagger';
import { Response } from 'express';
import { PrismaService } from '../../infrastructure/persistence/prisma.service';
import { SchedulerKafkaProducer } from '../../infrastructure/kafka/scheduler-kafka.producer';
import { SchedulerKafkaConsumer } from '../../infrastructure/kafka/scheduler-kafka.consumer';

@ApiTags('Health')
@Controller('health')
export class HealthController {
  constructor(
    private readonly prisma: PrismaService,
    private readonly kafkaProducer: SchedulerKafkaProducer,
    private readonly kafkaConsumer: SchedulerKafkaConsumer,
  ) {}

  @Get('liveness')
  @ApiOperation({ summary: 'Liveness probe (Node.js runtime status)' })
  @ApiResponse({ status: 200, description: 'Service is alive' })
  getLiveness(@Res() res: Response) {
    return res.status(HttpStatus.OK).json({
      status: 'UP',
      timestamp: new Date().toISOString(),
    });
  }

  @Get('readiness')
  @ApiOperation({ summary: 'Readiness probe (PostgreSQL & Kafka broker status)' })
  @ApiResponse({ status: 200, description: 'Service is ready to accept traffic' })
  @ApiResponse({ status: 503, description: 'One or more downstream dependencies are unready' })
  async getReadiness(@Res() res: Response) {
    let dbStatus = 'DOWN';
    let kafkaStatus = 'DOWN';

    try {
      await this.prisma.$queryRaw`SELECT 1`;
      dbStatus = 'UP';
    } catch {
      dbStatus = 'DOWN';
    }

    const producerConnected = this.kafkaProducer.getIsConnected();
    const consumerConnected = this.kafkaConsumer.getIsConnected();

    if (producerConnected && consumerConnected) {
      kafkaStatus = 'UP';
    } else {
      kafkaStatus = 'DOWN';
    }

    const isReady = dbStatus === 'UP' && kafkaStatus === 'UP';

    return res.status(isReady ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE).json({
      status: isReady ? 'UP' : 'DOWN',
      checks: {
        database: dbStatus,
        kafka: kafkaStatus,
      },
      timestamp: new Date().toISOString(),
    });
  }
}
