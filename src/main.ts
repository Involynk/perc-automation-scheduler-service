import { NestFactory } from '@nestjs/core';
import { ValidationPipe, Logger } from '@nestjs/common';
import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('SchedulerBootstrap');
  const app = await NestFactory.create(AppModule);

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
      forbidNonWhitelisted: true,
    }),
  );

  const config = new DocumentBuilder()
    .setTitle('PERC Scheduler Engine API')
    .setDescription('Generic domain-agnostic time management and delayed execution primitive for PERC Admission Operations Platform')
    .setVersion('1.0.0')
    .addTag('Timers', 'Timer scheduling, cancellation, and inspection endpoints')
    .addTag('Health', 'Kubernetes Liveness and Readiness probes')
    .build();

  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('api/docs', app, document);

  const port = process.env.PORT || 3000;
  await app.listen(port);
  logger.log(`PERC Scheduler Service is running on http://localhost:${port}`);
  logger.log(`Swagger OpenAPI Documentation available at http://localhost:${port}/api/docs`);
}

bootstrap();
