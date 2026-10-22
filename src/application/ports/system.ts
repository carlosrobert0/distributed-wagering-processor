import { OutboxMessage } from '../../domain/messaging/outbox-message';

export interface Clock {
  now(): Date;
}

export interface IdGenerator {
  next(): string;
}

export interface DeliveryAttempts {
  recordFailure(consumerName: string, messageId: string, reason: string, at: Date): Promise<number>;
}

export interface EventPublisher {
  publish(message: OutboxMessage): Promise<void>;
}

export interface LogFields {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
  [key: string]: string | number | boolean | undefined;
}

export interface AppLogger {
  debug(message: string, fields?: LogFields): void;
  info(message: string, fields?: LogFields): void;
  warn(message: string, fields?: LogFields, error?: unknown): void;
  error(message: string, fields?: LogFields, error?: unknown): void;
}

export type ProcessingSource = 'http' | 'sqs' | 'worker';

export interface Metrics {
  transactionSettled(status: string, kind: string, source: ProcessingSource): void;
  duplicateDetected(type: 'idempotent_replay' | 'idempotency_conflict' | 'inbox_duplicate'): void;
  retry(component: 'sqs_consumer' | 'outbox_relay' | 'pending_reference' | 'race'): void;
  deadLettered(reason: 'permanent' | 'max_attempts'): void;
  lockConflict(kind: 'lock_timeout' | 'deadlock' | 'unique_race'): void;
  observeProcessingLatency(source: ProcessingSource, seconds: number): void;
  observeLockWait(seconds: number): void;
  outboxPublished(count: number): void;
  setOutboxBacklog(pending: number, lagSeconds: number): void;
  reconciliationChecked(consistent: boolean): void;
}

export const CLOCK = Symbol('Clock');
export const DELIVERY_ATTEMPTS = Symbol('DeliveryAttempts');
export const ID_GENERATOR = Symbol('IdGenerator');
export const EVENT_PUBLISHER = Symbol('EventPublisher');
export const APP_LOGGER = Symbol('AppLogger');
export const METRICS = Symbol('Metrics');
