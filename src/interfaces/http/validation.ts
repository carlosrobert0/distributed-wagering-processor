import { z } from 'zod';
import { InvalidPayloadError } from '../../application/errors';
import { describeZodError } from '../contracts/wager-transaction.contract';

export function parseBody<T extends z.ZodTypeAny>(schema: T, body: unknown): z.infer<T> {
  const result = schema.safeParse(body);
  if (!result.success) {
    throw new InvalidPayloadError('Invalid request payload', describeZodError(result.error));
  }
  return result.data;
}
