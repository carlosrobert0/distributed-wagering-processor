import { describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { EventContext } from '../../../src/domain/events/integration-event';
import { WagerTransactionProcessed, WalletBalanceChanged } from '../../../src/domain/events/wager-events';
import { InboxMessage } from '../../../src/domain/messaging/inbox-message';
import { OutboxMessage } from '../../../src/domain/messaging/outbox-message';
import { InvalidTransactionStateError } from '../../../src/domain/shared/domain-error';
import { ExponentialBackoff } from '../../../src/domain/shared/exponential-backoff';
import { settleWagerTransaction, DEFAULT_REFERENCE_RETRY_POLICY } from '../../../src/domain/wagering/wager-settlement';
import { WagerTransactionKind } from '../../../src/domain/wagering/wager-transaction';
import { brl, newTransaction, NOW, openWallet } from '../../support/domain-builders';

const ctx: EventContext = {
  correlationId: 'corr-1',
  causationId: 'msg-1',
  occurredAt: NOW,
  newEventId: () => randomUUID(),
};

describe('ExponentialBackoff', () => {
  test('dobra a cada tentativa até o teto', () => {
    const backoff = new ExponentialBackoff(1_000, 5_000, 4);
    expect([1, 2, 3, 4, 5].map((n) => backoff.delayFor(n))).toEqual([1_000, 2_000, 4_000, 5_000, 5_000]);
    expect(backoff.isExhausted(3)).toBe(false);
    expect(backoff.isExhausted(4)).toBe(true);
  });

  test('sem maxAttempts nunca esgota', () => {
    expect(new ExponentialBackoff(1, 10).isExhausted(1_000_000)).toBe(false);
  });

  test('política padrão de PENDING_REFERENCE soma ~8,5 min (INT-06)', () => {
    const total = Array.from({ length: 8 }, (_, i) => DEFAULT_REFERENCE_RETRY_POLICY.delayFor(i + 1)).reduce(
      (a, b) => a + b,
      0,
    );
    expect(total).toBe(510_000);
  });
});

describe('InboxMessage', () => {
  test('receive → markProcessed; processar duas vezes é erro de programação', () => {
    const inbox = InboxMessage.receive({ messageId: 'm1', consumerName: 'c', payloadHash: 'h', receivedAt: NOW });
    expect(inbox.isProcessed()).toBe(false);
    inbox.markProcessed(NOW);
    expect(inbox.isProcessed()).toBe(true);
    expect(() => inbox.markProcessed(NOW)).toThrow(InvalidTransactionStateError);
  });
});

describe('IntegrationEvent + OutboxMessage (AC-OUT-05/06/07)', () => {
  test('envelope com eventType/version no tipo e Money serializado como string', () => {
    const wallet = openWallet('100.00');
    const tx = newTransaction(wallet, { money: brl('25.00') });
    const outcome = settleWagerTransaction({
      wallet,
      transaction: tx,
      ledgerEntryId: randomUUID(),
      now: NOW,
      referenceRetryPolicy: DEFAULT_REFERENCE_RETRY_POLICY,
    });
    if (outcome.type !== 'processed' || !outcome.ledgerEntry) throw new Error('expected processed BET');

    const processed = WagerTransactionProcessed.from(tx, ctx).toJSON();
    expect(processed.eventType).toBe('WagerTransactionProcessed');
    expect(processed.version).toBe(1);
    expect(processed.occurredAt).toBe(NOW.toISOString());
    expect(processed.causationId).toBe('msg-1');
    expect(processed.data.money).toEqual({ amount: '25.00', currency: 'BRL' });
    expect(processed.data.balanceAfter).toEqual({ amount: '75.00', currency: 'BRL' });

    const changed = WalletBalanceChanged.from(wallet, outcome.ledgerEntry, ctx);
    expect(changed.toJSON().data).toEqual({
      walletId: wallet.id,
      transactionId: tx.id,
      direction: 'DEBIT' as never,
      money: { amount: '25.00', currency: 'BRL' },
      balanceBefore: { amount: '100.00', currency: 'BRL' },
      balanceAfter: { amount: '75.00', currency: 'BRL' },
      walletVersion: 2,
    });
    expect(changed.partitionKey).toBe(wallet.id);
  });

  test('outbox: enqueue → retry com backoff → published; eventId estável', () => {
    const wallet = openWallet('100.00');
    const tx = newTransaction(wallet, { kind: WagerTransactionKind.Loss, money: brl('0.00') });
    settleWagerTransaction({
      wallet,
      transaction: tx,
      ledgerEntryId: randomUUID(),
      now: NOW,
      referenceRetryPolicy: DEFAULT_REFERENCE_RETRY_POLICY,
    });
    const event = WagerTransactionProcessed.from(tx, ctx);
    const message = OutboxMessage.enqueue(event);

    expect(message.id).toBe(event.eventId);
    expect(message.partitionKey).toBe(wallet.id);
    expect(message.isDue(NOW)).toBe(true);

    message.scheduleRetry(NOW, 'boom');
    expect(message.attempts).toBe(1);
    expect(message.nextAttemptAt).toEqual(new Date(NOW.getTime() + 1_000));
    expect(message.isDue(NOW)).toBe(false);

    message.scheduleRetry(NOW);
    expect(message.nextAttemptAt).toEqual(new Date(NOW.getTime() + 2_000));

    message.markPublished(NOW);
    expect(message.isPending()).toBe(false);
    expect(() => message.markPublished(NOW)).toThrow(InvalidTransactionStateError);
  });
});
