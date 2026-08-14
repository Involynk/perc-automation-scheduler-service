import { Module, Global } from '@nestjs/common';
import { PrismaService } from './prisma.service';
import { PrismaTimerRepository } from './prisma-timer.repository';

@Global()
@Module({
  providers: [PrismaService, PrismaTimerRepository],
  exports: [PrismaService, PrismaTimerRepository],
})
export class PrismaModule {}
