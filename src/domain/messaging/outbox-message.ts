import { IntegrationEvent } from '../events/integration-event';
import { InvalidTransactionStateError } from '../shared/domain-error';
import { ExponentialBackoff } from '../shared/exponential-backoff';

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  partitionKey: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
  lastError?: string;
}

export const OUTBOX_RETRY_POLICY = new ExponentialBackoff(1_000, 300_000);

const MAX_ERROR_LENGTH = 500;

export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly partitionKey: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
    private _lastError?: string,
  ) {}

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    const payload = JSON.parse(JSON.stringify(event.toJSON())) as Record<string, unknown>;
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.partitionKey,
      event.eventType,
      Object.freeze(payload),
      event.occurredAt,
      0,
      event.occurredAt,
    );
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.aggregateId,
      state.partitionKey,
      state.eventType,
      state.payload,
      new Date(state.occurredAt),
      state.attempts,
      state.nextAttemptAt ? new Date(state.nextAttemptAt) : undefined,
      state.publishedAt ? new Date(state.publishedAt) : undefined,
      state.lastError,
    );
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  get lastError(): string | undefined {
    return this._lastError;
  }

  isPending(): boolean {
    return this._publishedAt === undefined;
  }

  isDue(now: Date): boolean {
    return this.isPending() && (this._nextAttemptAt === undefined || this._nextAttemptAt.getTime() <= now.getTime());
  }

  lease(until: Date): void {
    this.assertPending('LEASE');
    this._nextAttemptAt = until;
  }

  release(now: Date): void {
    this.assertPending('RELEASE');
    this._nextAttemptAt = now;
  }

  markPublished(at: Date): void {
    this.assertPending('PUBLISHED');
    this._attempts += 1;
    this._publishedAt = at;
    this._nextAttemptAt = undefined;
    this._lastError = undefined;
  }

  scheduleRetry(now: Date, reason?: string): void {
    this.assertPending('RETRY');
    this._attempts += 1;
    this._nextAttemptAt = OUTBOX_RETRY_POLICY.nextAttemptAt(now, this._attempts);
    this._lastError = reason?.slice(0, MAX_ERROR_LENGTH);
  }

  private assertPending(attempted: string): void {
    if (!this.isPending()) {
      throw new InvalidTransactionStateError(this.id, 'PUBLISHED', attempted);
    }
  }
}
