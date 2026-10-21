import { InvariantViolationError } from './domain-error';

/**
 * Política de backoff exponencial com teto, usada pela outbox e pelo reprocessamento de
 * PENDING_REFERENCE. É determinística (sem jitter) para que os testes sejam reproduzíveis;
 * o espalhamento entre instâncias vem do `SKIP LOCKED`, não de aleatoriedade.
 */
export class ExponentialBackoff {
  constructor(
    readonly baseDelayMs: number,
    readonly maxDelayMs: number,
    /** `undefined` = nunca esgota (ex.: outbox não pode desistir de um evento confirmado). */
    readonly maxAttempts?: number,
  ) {
    if (baseDelayMs <= 0 || maxDelayMs < baseDelayMs) {
      throw new InvariantViolationError('Invalid backoff configuration');
    }
    if (maxAttempts !== undefined && maxAttempts < 1) {
      throw new InvariantViolationError('maxAttempts must be >= 1');
    }
  }

  /** Atraso antes da tentativa `attempt` (1-based): base · 2^(attempt−1), limitado ao teto. */
  delayFor(attempt: number): number {
    const exponent = Math.max(0, attempt - 1);
    // Evita overflow numérico em expoentes altos: acima de 2^30 já passamos de qualquer teto razoável.
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
