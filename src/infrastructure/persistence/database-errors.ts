import {
  ConnectionException,
  DeadlockException,
  DriverException,
  LockWaitTimeoutException,
  UniqueConstraintViolationException,
} from '@mikro-orm/core';
import { TransientInfrastructureError } from '../../application/errors';

/** SQLSTATEs do Postgres que indicam falha temporária (vale a pena tentar de novo). */
const TRANSIENT_SQLSTATES: Record<string, TransientInfrastructureError['reason']> = {
  '55P03': 'lock_timeout', // lock_not_available (lock_timeout)
  '57014': 'lock_timeout', // query_canceled (statement_timeout)
  '40001': 'deadlock', // serialization_failure
  '40P01': 'deadlock', // deadlock_detected
  '57P01': 'connection', // admin_shutdown
  '57P02': 'connection', // crash_shutdown
  '57P03': 'connection', // cannot_connect_now
  '53300': 'connection', // too_many_connections
  '08000': 'connection',
  '08003': 'connection',
  '08006': 'connection',
  '08001': 'connection',
  '08004': 'connection',
};

const TRANSIENT_NODE_ERRORS = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN']);

export function constraintOf(error: unknown): string | undefined {
  return (error as { constraint?: string } | undefined)?.constraint;
}

export function isUniqueViolation(error: unknown, constraint?: string): boolean {
  if (!(error instanceof UniqueConstraintViolationException)) {
    return false;
  }
  return constraint === undefined || constraintOf(error) === constraint;
}

/**
 * Converte erros de driver/rede em `TransientInfrastructureError` quando apropriado.
 * Retorna `undefined` se o erro não for transitório (deve ser propagado como está).
 */
export function toTransientError(error: unknown): TransientInfrastructureError | undefined {
  if (error instanceof TransientInfrastructureError) {
    return error;
  }
  if (error instanceof LockWaitTimeoutException) {
    return new TransientInfrastructureError('Wallet is busy (lock timeout)', 'lock_timeout', { cause: error });
  }
  if (error instanceof DeadlockException) {
    return new TransientInfrastructureError('Transaction aborted by deadlock/serialization', 'deadlock', { cause: error });
  }
  if (error instanceof ConnectionException) {
    return new TransientInfrastructureError('Database connection failure', 'connection', { cause: error });
  }
  const code = (error as { code?: unknown } | undefined)?.code;
  if (typeof code === 'string') {
    const reason = TRANSIENT_SQLSTATES[code];
    if (reason) {
      return new TransientInfrastructureError(`Database transient failure (${code})`, reason, { cause: error });
    }
    if (TRANSIENT_NODE_ERRORS.has(code)) {
      return new TransientInfrastructureError(`Database unreachable (${code})`, 'connection', { cause: error });
    }
  }
  if (error instanceof DriverException && /timeout|terminat|connect/i.test(error.message)) {
    return new TransientInfrastructureError('Database driver failure', 'connection', { cause: error });
  }
  if (error instanceof Error && /Connection terminated|timeout exceeded when trying to connect/i.test(error.message)) {
    return new TransientInfrastructureError('Database connection failure', 'connection', { cause: error });
  }
  return undefined;
}
