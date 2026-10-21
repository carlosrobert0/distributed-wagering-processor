import { randomUUID } from 'node:crypto';
import type { NextFunction, Request, Response } from 'express';
import { RequestContextStore } from '../../infrastructure/observability/request-context';

export const CORRELATION_HEADER = 'x-correlation-id';

/** Aceita só ids "seguros" vindos de fora, para não abrir espaço para log injection. */
const SAFE_ID = /^[A-Za-z0-9._:-]{1,128}$/;

/** Propaga (ou cria) o correlation id por toda a cadeia assíncrona da requisição. */
export function correlationIdMiddleware(request: Request, response: Response, next: NextFunction): void {
  const incoming = request.header(CORRELATION_HEADER);
  const correlationId = incoming && SAFE_ID.test(incoming) ? incoming : randomUUID();
  response.setHeader(CORRELATION_HEADER, correlationId);
  RequestContextStore.run({ correlationId }, next);
}

export function currentCorrelationId(): string {
  return RequestContextStore.current()?.correlationId ?? randomUUID();
}
