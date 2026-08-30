import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import * as https from 'https';
import * as http from 'http';

@Injectable()
export class KeepAliveService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KeepAliveService.name);
  private timer: NodeJS.Timeout | null = null;

  onModuleInit() {
    const url =
      process.env.RENDER_EXTERNAL_URL
        ? `${process.env.RENDER_EXTERNAL_URL}/health/liveness`
        : 'https://perc-automation-scheduler-service.onrender.com/health/liveness';

    // Initial ping 30s after startup
    setTimeout(() => this.ping(url), 30000);

    // Ping every 5 minutes (300,000ms) to reset Render 15-minute idle timer
    this.timer = setInterval(() => this.ping(url), 300000);
    this.logger.log(`KeepAliveService started. Pinging ${url} every 5 minutes.`);
  }

  onModuleDestroy() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private ping(targetUrl: string) {
    try {
      const client = targetUrl.startsWith('https') ? https : http;
      client
        .get(targetUrl, (res) => {
          this.logger.log(`⏰ [KeepAlive] Self-ping to ${targetUrl} -> Status ${res.statusCode}`);
        })
        .on('error', (err) => {
          this.logger.warn(`⚠️ [KeepAlive] Self-ping error: ${err.message}`);
        });
    } catch (err: any) {
      this.logger.warn(`⚠️ [KeepAlive] Failed to execute self-ping: ${err.message}`);
    }
  }
}
