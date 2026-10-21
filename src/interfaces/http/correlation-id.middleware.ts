import { randomUUID } from 'node:crypto';
import type { NextFunction, Request, Response } from 'express';
import { RequestContextStore } from '../../infrastructure/observability/request-context';

export const CORRELATION_HEADER = 'x-correlation-id';

const SAFE_ID = /^[A-Za-z0-9._:-]{1,128}$/;

export function correlationIdMiddleware(request: Request, response: Response, next: NextFunction): void {
  const incoming = request.header(CORRELATION_HEADER);
  const correlationId = incoming && SAFE_ID.test(incoming) ? incoming : randomUUID();
  response.setHeader(CORRELATION_HEADER, correlationId);
  RequestContextStore.run({ correlationId }, next);
}

export function currentCorrelationId(): string {
  return RequestContextStore.current()?.correlationId ?? randomUUID();
}
