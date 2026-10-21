import { LoggerService } from '@nestjs/common';
import pino, { Logger } from 'pino';
import { AppLogger, LogFields } from '../../application/ports/system';
import { RequestContextStore } from './request-context';

/**
 * Logger JSON estruturado (AC-OBS-01/02).
 *
 * - `correlationId`/`messageId` entram automaticamente via AsyncLocalStorage (mixin);
 * - campos sensíveis são removidos por `redact` como segunda barreira — a primeira é a porta
 *   `LogFields`, que não aceita objetos (logo, nenhum payload/Money completo chega aqui).
 */
export function createPinoLogger(options: { level: string; instanceId: string }): Logger {
  return pino({
    level: options.level,
    base: { service: 'wagering-processor', instance: options.instanceId },
    timestamp: pino.stdTimeFunctions.isoTime,
    messageKey: 'message',
    formatters: { level: (label) => ({ level: label }) },
    mixin: () => {
      const context = RequestContextStore.current();
      return context ? { correlationId: context.correlationId, messageId: context.messageId } : {};
    },
    redact: {
      paths: ['amount', 'money', 'balance', 'balanceAfter', 'balanceBefore', 'payload', '*.amount', '*.payload', 'authorization'],
      censor: '[REDACTED]',
    },
  });
}

export class PinoAppLogger implements AppLogger {
  constructor(private readonly logger: Logger) {}

  debug(message: string, fields?: LogFields): void {
    this.logger.debug(clean(fields), message);
  }

  info(message: string, fields?: LogFields): void {
    this.logger.info(clean(fields), message);
  }

  warn(message: string, fields?: LogFields): void {
    this.logger.warn(clean(fields), message);
  }

  error(message: string, fields?: LogFields, error?: unknown): void {
    this.logger.error({ ...clean(fields), ...(error !== undefined ? { err: serializeError(error) } : {}) }, message);
  }
}

/** Adaptador para o logger interno do Nest (bootstrap, rotas), mantendo JSON. */
export class PinoNestLogger implements LoggerService {
  constructor(private readonly logger: Logger) {}

  log(message: unknown, context?: string): void {
    this.logger.info({ context }, String(message));
  }

  error(message: unknown, trace?: string, context?: string): void {
    this.logger.error({ context, trace }, String(message));
  }

  warn(message: unknown, context?: string): void {
    this.logger.warn({ context }, String(message));
  }

  debug(message: unknown, context?: string): void {
    this.logger.debug({ context }, String(message));
  }

  verbose(message: unknown, context?: string): void {
    this.logger.trace({ context }, String(message));
  }
}

function clean(fields: LogFields | undefined): Record<string, unknown> {
  if (!fields) return {};
  return Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== undefined));
}

function serializeError(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    return {
      type: error.name,
      message: error.message,
      code: (error as { code?: unknown }).code,
      stack: error.stack,
    };
  }
  return { message: String(error) };
}
