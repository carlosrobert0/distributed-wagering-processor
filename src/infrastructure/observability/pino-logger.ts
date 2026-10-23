import { LoggerService } from '@nestjs/common';
import pino, { DestinationStream, Logger } from 'pino';
import { AppLogger, LogFields } from '../../application/ports/system';
import { RequestContextStore } from './request-context';

export interface PinoLoggerOptions {
  level: string;
  instanceId: string;
  destination?: DestinationStream;
}

export function createPinoLogger(options: PinoLoggerOptions): Logger {
  return pino(
    {
      level: options.level,
      base: { service: 'wagering-processor', instance: options.instanceId },
      timestamp: pino.stdTimeFunctions.isoTime,
      messageKey: 'message',
      formatters: { level: (label) => ({ level: label }) },
      mixin: () => {
        const context = RequestContextStore.current();
        return context ? { correlationId: context.correlationId, messageId: context.messageId } : {};
      },
      serializers: { err: serializeError },
      redact: {
        paths: [
          'amount',
          'money',
          'balance',
          'balanceAfter',
          'balanceBefore',
          'payload',
          '*.amount',
          '*.payload',
          'authorization',
        ],
        censor: '[REDACTED]',
      },
    },
    options.destination,
  );
}

export class PinoAppLogger implements AppLogger {
  constructor(private readonly logger: Logger) {}

  debug(message: string, fields?: LogFields): void {
    this.logger.debug(clean(fields), message);
  }

  info(message: string, fields?: LogFields): void {
    this.logger.info(clean(fields), message);
  }

  warn(message: string, fields?: LogFields, error?: unknown): void {
    this.logger.warn(withError(fields, error), message);
  }

  error(message: string, fields?: LogFields, error?: unknown): void {
    this.logger.error(withError(fields, error), message);
  }
}

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

function withError(fields: LogFields | undefined, error: unknown): Record<string, unknown> {
  return { ...clean(fields), ...(error !== undefined ? { err: error } : {}) };
}

const DECIMAL_AMOUNT = /(?<![\w.])-?\d+\.\d{2}(?![\w.])/g;
const MASKED_AMOUNT = '[AMOUNT]';

export function maskAmounts(text: string): string {
  return text.replace(DECIMAL_AMOUNT, MASKED_AMOUNT);
}

export function serializeError(error: unknown): Record<string, unknown> {
  const serialized: Record<string, unknown> =
    error instanceof Error ? { ...pino.stdSerializers.err(error) } : { message: String(error) };
  return Object.fromEntries(
    Object.entries(serialized).map(([key, value]) => [key, typeof value === 'string' ? maskAmounts(value) : value]),
  );
}
