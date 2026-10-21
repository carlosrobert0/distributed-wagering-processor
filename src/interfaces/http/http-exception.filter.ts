import { ArgumentsHost, Catch, ExceptionFilter, HttpException, HttpStatus } from '@nestjs/common';
import type { Response } from 'express';
import {
  ApplicationError,
  ConflictError,
  InvalidPayloadError,
  NotFoundError,
  TransientInfrastructureError,
} from '../../application/errors';
import { AppLogger } from '../../application/ports/system';
import { DomainValidationError } from '../../domain/shared/domain-error';

export interface ErrorBody {
  error: string;
  message: string;
  details?: readonly string[];
}

/** Segundos sugeridos ao provedor antes de reenviar após uma falha transitória. */
const RETRY_AFTER_SECONDS = '1';

/**
 * Mapeamento ÚNICO de erro → status HTTP, aplicado a todos os endpoints (SPEC §8):
 *   400 payload inválido · 404 inexistente · 409 conflito · 503 transitório (+Retry-After) · 500 bug.
 * Rejeição de negócio (422) e pendência (202) não são exceções: são resultados do use case,
 * mapeados no controller.
 */
@Catch()
export class HttpExceptionFilter implements ExceptionFilter {
  constructor(private readonly logger: AppLogger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<Response>();
    const { status, body } = this.map(exception);

    if (status === HttpStatus.SERVICE_UNAVAILABLE) {
      response.setHeader('Retry-After', RETRY_AFTER_SECONDS);
    }
    if (status >= 500) {
      this.logger.error('request failed', { status, error: body.error }, exception);
    }
    response.status(status).json(body);
  }

  private map(exception: unknown): { status: number; body: ErrorBody } {
    if (exception instanceof InvalidPayloadError) {
      return {
        status: HttpStatus.BAD_REQUEST,
        body: { error: exception.code, message: exception.message, ...(exception.details ? { details: exception.details } : {}) },
      };
    }
    if (exception instanceof DomainValidationError) {
      return { status: HttpStatus.BAD_REQUEST, body: { error: 'INVALID_PAYLOAD', message: exception.message } };
    }
    if (exception instanceof NotFoundError) {
      return { status: HttpStatus.NOT_FOUND, body: { error: exception.code, message: exception.message } };
    }
    if (exception instanceof ConflictError) {
      return { status: HttpStatus.CONFLICT, body: { error: exception.code, message: exception.message } };
    }
    if (exception instanceof TransientInfrastructureError) {
      return {
        status: HttpStatus.SERVICE_UNAVAILABLE,
        body: { error: exception.code, message: 'Temporary failure, retry with the same Idempotency-Key' },
      };
    }
    if (exception instanceof HttpException) {
      // Erros do próprio framework (JSON malformado no body, rota inexistente, …).
      const status = exception.getStatus();
      return {
        status,
        body: {
          error: status === HttpStatus.BAD_REQUEST ? 'INVALID_PAYLOAD' : HttpStatus[status] ?? 'HTTP_ERROR',
          message: exception.message,
        },
      };
    }
    const code = exception instanceof ApplicationError ? exception.code : 'INTERNAL_ERROR';
    return { status: HttpStatus.INTERNAL_SERVER_ERROR, body: { error: code, message: 'Internal error' } };
  }
}
