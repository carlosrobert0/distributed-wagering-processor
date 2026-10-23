import { describe, expect, test } from 'bun:test';
import { DEFAULT_LEASE_POLICY, LeasePolicy } from '../../../src/application/lease-policy';

describe('LeasePolicy', () => {
  const now = new Date('2026-10-21T12:00:00.000Z');

  test('expira após a duração configurada', () => {
    expect(new LeasePolicy(30_000).expiresAt(now)).toEqual(new Date('2026-10-21T12:00:30.000Z'));
  });

  test('margem de segurança é 1/4 da duração, limitada a 10 s', () => {
    expect(new LeasePolicy(2_000).safetyMarginMs).toBe(500);
    expect(new LeasePolicy(30_000).safetyMarginMs).toBe(7_500);
    expect(new LeasePolicy(600_000).safetyMarginMs).toBe(10_000);
  });

  test('deixa de ser válido na margem antes de expirar', () => {
    const lease = new LeasePolicy(8_000);
    const expiresAt = lease.expiresAt(now);
    expect(lease.isStillValid(new Date(now.getTime() + 5_999), expiresAt)).toBe(true);
    expect(lease.isStillValid(new Date(now.getTime() + 6_000), expiresAt)).toBe(false);
  });

  test('padrão de 30 s', () => {
    expect(DEFAULT_LEASE_POLICY.durationMs).toBe(30_000);
  });
});
