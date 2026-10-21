import { beforeEach, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { TransientInfrastructureError } from '../../../src/application/errors';
import { TransactionScope } from '../../../src/application/ports/unit-of-work';
import { WagerMessageHandler } from '../../../src/interfaces/sqs/wager-message.handler';
import { SilentLogger } from '../../support/in-memory';
import { buildInMemoryContext, InMemoryContext } from '../../support/in-memory-context';

function envelope(data: object, messageId = `msg-${randomUUID()}`): string {
  return JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data });
}

describe('WagerMessageHandler', () => {
  let ctx: InMemoryContext;
  let handler: WagerMessageHandler;

  beforeEach(() => {
    ctx = buildInMemoryContext();
    handler = new WagerMessageHandler(ctx.processWager, 'consumer', new SilentLogger());
  });

  test('mensagem válida → ack após commit, com inbox gravada', async () => {
    const wallet = await ctx.openWallet('100.00');
    const decision = await handler.handle(envelope(ctx.command(wallet), 'msg-1'));

    expect(decision).toEqual({ action: 'ack', committed: true });
    expect(ctx.uow.store.inbox.has('consumer/msg-1')).toBe(true);
    expect(ctx.uow.store.wallets.get(wallet.id)!.balance.toJSON().amount).toBe('90.00');
  });

  test('redelivery da mesma mensagem → ack sem novo efeito', async () => {
    const wallet = await ctx.openWallet('100.00');
    const body = envelope(ctx.command(wallet), 'msg-2');
    await handler.handle(body);

    expect(await handler.handle(body)).toEqual({ action: 'ack', committed: true });
    expect(ctx.uow.store.ledgerFor(wallet.id)).toHaveLength(2);
  });

  test('rejeição de negócio persistida (saldo insuficiente) → ack', async () => {
    const wallet = await ctx.openWallet('1.00');
    expect(await handler.handle(envelope(ctx.command(wallet)))).toEqual({ action: 'ack', committed: true });
  });

  test('wallet inexistente e conflito de key → ack sem commit', async () => {
    expect(await handler.handle(envelope(ctx.command({ id: randomUUID(), playerId: 'p' })))).toEqual({
      action: 'ack',
      committed: false,
    });

    const wallet = await ctx.openWallet();
    const command = ctx.command(wallet);
    await handler.handle(envelope(command));
    expect(await handler.handle(envelope({ ...command, money: { amount: '99.00', currency: 'BRL' } }))).toEqual({
      action: 'ack',
      committed: false,
    });
  });

  test.each([
    ['JSON malformado', '{nope', /^MALFORMED_JSON/],
    [
      'tipo de mensagem errado',
      JSON.stringify({ messageId: 'm', type: 'Other', occurredAt: new Date().toISOString(), data: {} }),
      /^INVALID_ENVELOPE/,
    ],
    ['corpo vazio', undefined, /^MALFORMED_JSON/],
  ])('%s → DLQ', async (_label, body, reason) => {
    const decision = await handler.handle(body);
    expect(decision.action).toBe('dead-letter');
    expect(decision.action === 'dead-letter' && decision.reason).toMatch(reason);
  });

  test('payload com regra de forma violada (OPENING) → DLQ', async () => {
    const wallet = await ctx.openWallet();
    const decision = await handler.handle(envelope(ctx.command(wallet, { kind: 'OPENING' })));
    expect(decision.action).toBe('dead-letter');
  });

  test('falha transitória → retry, sem efeito', async () => {
    const wallet = await ctx.openWallet('100.00');
    ctx.uow.run = async <T>(_work: (scope: TransactionScope) => Promise<T>): Promise<T> => {
      throw new TransientInfrastructureError('db down', 'connection');
    };
    expect(await handler.handle(envelope(ctx.command(wallet)))).toEqual({
      action: 'retry',
      reason: 'TRANSIENT_FAILURE',
    });
  });
});
