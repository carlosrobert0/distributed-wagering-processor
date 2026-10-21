import 'reflect-metadata';
import { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { Express } from 'express';
import { APP_LOGGER, AppLogger } from './application/ports/system';
import { AppModule } from './app.module';
import { AppConfig } from './infrastructure/config/app-config';
import { createPinoLogger, PinoNestLogger } from './infrastructure/observability/pino-logger';
import { correlationIdMiddleware } from './interfaces/http/correlation-id.middleware';
import { HttpExceptionFilter } from './interfaces/http/http-exception.filter';

/** Monta a aplicação Nest. Usado pelo `main.ts` e pelos testes de integração (mesmo código). */
export async function createApp(
  config: AppConfig,
  options: { shutdownHooks?: boolean } = {},
): Promise<INestApplication> {
  const rootLogger = createPinoLogger({ level: config.logLevel, instanceId: config.instanceId });
  const app = await NestFactory.create(AppModule.register(config, rootLogger), {
    logger: new PinoNestLogger(rootLogger),
    abortOnError: false,
  });

  (app.getHttpAdapter().getInstance() as Express).disable('x-powered-by');
  app.use(correlationIdMiddleware);
  app.useGlobalFilters(new HttpExceptionFilter(app.get<AppLogger>(APP_LOGGER)));
  // SIGTERM/SIGINT → beforeApplicationShutdown (consumer e workers drenam) → onApplicationShutdown.
  if (options.shutdownHooks ?? true) {
    app.enableShutdownHooks();
  }
  return app;
}
