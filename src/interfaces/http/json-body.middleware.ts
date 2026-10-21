import { json, NextFunction, Request, Response } from 'express';
import { ErrorBody } from './http-exception.filter';

const MAX_BODY_SIZE = '100kb';

interface BodyParserError {
  type: string;
  status: number;
}

const BODY_PARSER_ERRORS: Readonly<Record<string, ErrorBody>> = {
  'entity.parse.failed': { error: 'INVALID_PAYLOAD', message: 'Malformed JSON body' },
  'entity.too.large': { error: 'PAYLOAD_TOO_LARGE', message: `Body exceeds ${MAX_BODY_SIZE}` },
  'encoding.unsupported': { error: 'INVALID_PAYLOAD', message: 'Unsupported body encoding' },
  'charset.unsupported': { error: 'INVALID_PAYLOAD', message: 'Unsupported body charset' },
};

export const jsonBodyParser = json({ limit: MAX_BODY_SIZE });

export function bodyParserErrorHandler(
  error: unknown,
  _request: Request,
  response: Response,
  next: NextFunction,
): void {
  const parserError = error as Partial<BodyParserError> | undefined;
  const body = parserError?.type ? BODY_PARSER_ERRORS[parserError.type] : undefined;
  if (!body || typeof parserError?.status !== 'number') {
    next(error);
    return;
  }
  response.status(parserError.status).json(body);
}
