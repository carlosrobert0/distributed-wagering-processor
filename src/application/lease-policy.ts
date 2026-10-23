const MAX_SAFETY_MARGIN_MS = 10_000;
const SAFETY_MARGIN_RATIO = 4;

export class LeasePolicy {
  readonly safetyMarginMs: number;

  constructor(readonly durationMs: number) {
    this.safetyMarginMs = Math.min(MAX_SAFETY_MARGIN_MS, Math.floor(durationMs / SAFETY_MARGIN_RATIO));
  }

  expiresAt(now: Date): Date {
    return new Date(now.getTime() + this.durationMs);
  }

  isStillValid(now: Date, expiresAt: Date): boolean {
    return now.getTime() < expiresAt.getTime() - this.safetyMarginMs;
  }
}

export const DEFAULT_LEASE_POLICY = new LeasePolicy(30_000);
