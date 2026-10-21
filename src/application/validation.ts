import { DomainValidationError } from '../domain/shared/domain-error';
import { InvalidPayloadError } from './errors';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function isUuid(value: unknown): value is string {
  return typeof value === 'string' && UUID.test(value);
}

export function assertUuid(field: string, value: unknown): asserts value is string {
  if (!isUuid(value)) {
    throw new InvalidPayloadError(`${field} must be a UUID`, [`${field}: invalid uuid`]);
  }
}

/** Converte violações de contrato detectadas pelo domínio em erro de payload da aplicação. */
export function asPayloadError<T>(build: () => T): T {
  try {
    return build();
  } catch (error) {
    if (error instanceof DomainValidationError) {
      throw new InvalidPayloadError(error.message, [error.message]);
    }
    throw error;
  }
}
