import { InvariantViolationError } from './domain-error';

export class ExponentialBackoff {
  constructor(
    readonly baseDelayMs: number,
    readonly maxDelayMs: number,
    readonly maxAttempts?: number,
  ) {
    if (baseDelayMs <= 0 || maxDelayMs < baseDelayMs) {
      throw new InvariantViolationError('Invalid backoff configuration');
    }
    if (maxAttempts !== undefined && maxAttempts < 1) {
      throw new InvariantViolationError('maxAttempts must be >= 1');
    }
  }

  delayFor(attempt: number): number {
    const exponent = Math.max(0, attempt - 1);
    const factor = 2 ** Math.min(exponent, 30);
    return Math.min(this.baseDelayMs * factor, this.maxDelayMs);
  }

  nextAttemptAt(now: Date, attempt: number): Date {
    return new Date(now.getTime() + this.delayFor(attempt));
  }

  isExhausted(attemptsMade: number): boolean {
    return this.maxAttempts !== undefined && attemptsMade >= this.maxAttempts;
  }
}
