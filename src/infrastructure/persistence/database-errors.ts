import {
  ConnectionException,
  ConstraintViolationException,
  DeadlockException,
  LockWaitTimeoutException,
  UniqueConstraintViolationException,
} from '@mikro-orm/core';
import { IntegrityViolationError, TransientInfrastructureError, TransientReason } from '../../application/errors';

enum SqlState {
  LockNotAvailable = '55P03',
  QueryCanceled = '57014',
  SerializationFailure = '40001',
  DeadlockDetected = '40P01',
  AdminShutdown = '57P01',
  CrashShutdown = '57P02',
  CannotConnectNow = '57P03',
  TooManyConnections = '53300',
  ConnectionException = '08000',
  ConnectionDoesNotExist = '08003',
  ConnectionFailure = '08006',
  UnableToEstablishConnection = '08001',
  ConnectionRejected = '08004',
  RestrictViolation = '23001',
}

const TRANSIENT_SQL_STATES: Readonly<Record<string, TransientReason>> = {
  [SqlState.LockNotAvailable]: 'lock_timeout',
  [SqlState.QueryCanceled]: 'statement_timeout',
  [SqlState.SerializationFailure]: 'deadlock',
  [SqlState.DeadlockDetected]: 'deadlock',
  [SqlState.AdminShutdown]: 'connection',
  [SqlState.CrashShutdown]: 'connection',
  [SqlState.CannotConnectNow]: 'connection',
  [SqlState.TooManyConnections]: 'connection',
  [SqlState.ConnectionException]: 'connection',
  [SqlState.ConnectionDoesNotExist]: 'connection',
  [SqlState.ConnectionFailure]: 'connection',
  [SqlState.UnableToEstablishConnection]: 'connection',
  [SqlState.ConnectionRejected]: 'connection',
};

const TRANSIENT_NETWORK_ERRORS: ReadonlySet<string> = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'EPIPE',
  'ENOTFOUND',
  'EAI_AGAIN',
]);

const CONNECTION_FAILURE_MESSAGE = /Connection terminated|timeout exceeded when trying to connect/i;

export function constraintOf(error: unknown): string | undefined {
  return (error as { constraint?: string } | undefined)?.constraint;
}

export function isUniqueViolation(error: unknown, constraint?: string): boolean {
  if (!(error instanceof UniqueConstraintViolationException)) {
    return false;
  }
  return constraint === undefined || constraintOf(error) === constraint;
}

export function toTransientError(error: unknown): TransientInfrastructureError | undefined {
  if (error instanceof TransientInfrastructureError) {
    return error;
  }
  if (error instanceof LockWaitTimeoutException) {
    return new TransientInfrastructureError('Wallet is busy (lock timeout)', 'lock_timeout', { cause: error });
  }
  if (error instanceof DeadlockException) {
    return new TransientInfrastructureError('Transaction aborted by deadlock or serialization failure', 'deadlock', {
      cause: error,
    });
  }
  if (error instanceof ConnectionException) {
    return new TransientInfrastructureError('Database connection failure', 'connection', { cause: error });
  }

  const code = (error as { code?: unknown } | undefined)?.code;
  if (typeof code === 'string') {
    const reason = TRANSIENT_SQL_STATES[code];
    if (reason) {
      return new TransientInfrastructureError(`Database transient failure (${code})`, reason, { cause: error });
    }
    if (TRANSIENT_NETWORK_ERRORS.has(code)) {
      return new TransientInfrastructureError(`Database unreachable (${code})`, 'connection', { cause: error });
    }
  }

  if (error instanceof Error && CONNECTION_FAILURE_MESSAGE.test(error.message)) {
    return new TransientInfrastructureError('Database connection failure', 'connection', { cause: error });
  }
  return undefined;
}

export function toIntegrityError(error: unknown): IntegrityViolationError | undefined {
  const guardTriggered = (error as { code?: unknown } | undefined)?.code === SqlState.RestrictViolation;
  const constraintViolated =
    error instanceof ConstraintViolationException && !(error instanceof UniqueConstraintViolationException);
  if (guardTriggered || constraintViolated) {
    const detail = error instanceof Error ? error.message : String(error);
    return new IntegrityViolationError(`Database integrity check rejected the operation: ${detail}`, {
      cause: error,
    });
  }
  return undefined;
}
