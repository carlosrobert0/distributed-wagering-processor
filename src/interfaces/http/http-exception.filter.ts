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

export interface ErrorResponse {
  status: number;
  body: ErrorBody;
}

export const RETRY_AFTER_SECONDS = '1';

export function toErrorResponse(exception: unknown): ErrorResponse {
  if (exception instanceof InvalidPayloadError) {
    return {
      status: HttpStatus.BAD_REQUEST,
      body: {
        error: exception.code,
        message: exception.message,
        ...(exception.details ? { details: exception.details } : {}),
      },
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
    const status = exception.getStatus();
    const error = status === HttpStatus.BAD_REQUEST ? 'INVALID_PAYLOAD' : (HttpStatus[status] ?? 'HTTP_ERROR');
    return { status, body: { error, message: exception.message } };
  }
  const error = exception instanceof ApplicationError ? exception.code : 'INTERNAL_ERROR';
  return { status: HttpStatus.INTERNAL_SERVER_ERROR, body: { error, message: 'Internal error' } };
}

@Catch()
export class HttpExceptionFilter implements ExceptionFilter {
  constructor(private readonly logger: AppLogger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<Response>();
    const { status, body } = toErrorResponse(exception);

    if (status === HttpStatus.SERVICE_UNAVAILABLE) {
      response.setHeader('Retry-After', RETRY_AFTER_SECONDS);
    }
    if (status >= HttpStatus.INTERNAL_SERVER_ERROR) {
      this.logger.error('request failed', { status, error: body.error }, exception);
    }
    response.status(status).json(body);
  }
}
