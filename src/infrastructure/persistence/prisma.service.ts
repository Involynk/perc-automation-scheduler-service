import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { PrismaClient } from '@prisma/client';

@Injectable()
export class PrismaService extends PrismaClient implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PrismaService.name);

  async onModuleInit() {
    const maxRetries = 5;
    let attempt = 0;

    while (attempt < maxRetries) {
      try {
        attempt++;
        await this.$connect();
        this.logger.log('Connected to PostgreSQL (Supabase) successfully.');
        return;
      } catch (error) {
        this.logger.warn(`Database connection attempt ${attempt}/${maxRetries} failed: ${(error as Error).message}`);
        if (attempt >= maxRetries) {
          this.logger.error('Exhausted all PostgreSQL database connection attempts.', error);
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 2000 * attempt));
      }
    }
  }

  async onModuleDestroy() {
    await this.$disconnect();
    this.logger.log('Disconnected from PostgreSQL.');
  }
}
