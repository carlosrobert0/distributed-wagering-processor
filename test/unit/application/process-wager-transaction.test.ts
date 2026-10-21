import { beforeEach, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  DuplicateTransactionError,
  ExternalTransactionConflictError,
  IdempotencyKeyConflictError,
  InvalidPayloadError,
  WalletNotFoundError,
} from '../../../src/application/errors';
import { MAX_RACE_ATTEMPTS } from '../../../src/application/wagering/process-wager-transaction.use-case';
import { WagerTransactionStatus } from '../../../src/domain/wagering/wager-transaction';
import { buildInMemoryContext, HTTP, InMemoryContext } from '../../support/in-memory-context';

describe('ProcessWagerTransactionUseCase', () => {
  let ctx: InMemoryContext;

  beforeEach(() => {
    ctx = buildInMemoryContext();
  });

  describe('validação antes de tocar no armazenamento', () => {
    test.each([
      ['OPENING', { kind: 'OPENING' }],
      ['kind desconhecido', { kind: 'JACKPOT' }],
      ['amount com 3 casas', { money: { amount: '1.001', currency: 'BRL' } }],
      ['REFUND sem referência', { kind: 'REFUND' }],
      ['walletId inválido', { walletId: 'nope' }],
    ])('%s → InvalidPayloadError sem abrir transação', async (_label, overrides) => {
      const wallet = await ctx.openWallet();
      const commitsBefore = ctx.uow.commits;
      await expect(ctx.processWager.execute(ctx.command(wallet, overrides), HTTP)).rejects.toBeInstanceOf(
        InvalidPayloadError,
      );
      expect(ctx.uow.commits).toBe(commitsBefore);
    });
  });

  test('BET processada grava transação, lançamento, saldo e os dois eventos', async () => {
    const wallet = await ctx.openWallet('100.00');
    const result = await ctx.processWager.execute(
      ctx.command(wallet, { money: { amount: '25.00', currency: 'BRL' } }),
      HTTP,
    );

    expect(result).toEqual({
      transactionId: expect.any(String),
      status: WagerTransactionStatus.Processed,
      balance: { amount: '75.00', currency: 'BRL' },
      idempotentReplay: false,
    });
    expect(ctx.uow.store.wallets.get(wallet.id)!.version).toBe(2);
    expect(ctx.uow.store.ledgerFor(wallet.id)).toHaveLength(2);
    expect(ctx.uow.store.outboxTypesFor(result.transactionId)).toEqual(['WagerTransactionProcessed']);
    expect(ctx.uow.store.outboxTypesFor(wallet.id)).toContain('WalletBalanceChanged');
  });

  test('replay devolve o resultado original mesmo depois de o saldo mudar', async () => {
    const wallet = await ctx.openWallet('100.00');
    const command = ctx.command(wallet, { money: { amount: '30.00', currency: 'BRL' } });
    const original = await ctx.processWager.execute(command, HTTP);
    await ctx.processWager.execute(ctx.command(wallet), HTTP);

    const replay = await ctx.processWager.execute(command, HTTP);

    expect(replay).toEqual({ ...original, idempotentReplay: true });
    expect(ctx.uow.store.wallets.get(wallet.id)!.balance.toJSON().amount).toBe('60.00');
    expect(ctx.metrics.count('duplicate:idempotent_replay')).toBe(1);
  });

  test('mesma key com payload diferente → IdempotencyKeyConflictError', async () => {
    const wallet = await ctx.openWallet();
    const command = ctx.command(wallet);
    await ctx.processWager.execute(command, HTTP);

    await expect(
      ctx.processWager.execute({ ...command, money: { amount: '10.01', currency: 'BRL' } }, HTTP),
    ).rejects.toBeInstanceOf(IdempotencyKeyConflictError);
    expect(ctx.metrics.count('duplicate:idempotency_conflict')).toBe(1);
  });

  test('mesmo external id com outra key → ExternalTransactionConflictError', async () => {
    const wallet = await ctx.openWallet();
    const command = ctx.command(wallet);
    await ctx.processWager.execute(command, HTTP);

    await expect(ctx.processWager.execute({ ...command, idempotencyKey: 'another-key' }, HTTP)).rejects.toBeInstanceOf(
      ExternalTransactionConflictError,
    );
  });

  test('wallet inexistente → WalletNotFoundError e nada persiste', async () => {
    await expect(
      ctx.processWager.execute(ctx.command({ id: randomUUID(), playerId: 'p' }), HTTP),
    ).rejects.toBeInstanceOf(WalletNotFoundError);
    expect(ctx.uow.store.transactions.size).toBe(0);
  });

  test('rejeição por saldo persiste REJECTED com o saldo observado e só o evento de rejeição', async () => {
    const wallet = await ctx.openWallet('5.00');
    const result = await ctx.processWager.execute(ctx.command(wallet), HTTP);

    expect(result).toMatchObject({
      status: 'REJECTED',
      failureCode: 'INSUFFICIENT_FUNDS',
      balance: { amount: '5.00' },
    });
    expect(ctx.uow.store.ledgerFor(wallet.id)).toHaveLength(1);
    expect(ctx.uow.store.outboxTypesFor(result.transactionId)).toEqual(['WagerTransactionRejected']);
  });

  test('LOSS publica WagerTransactionProcessed e não WalletBalanceChanged', async () => {
    const wallet = await ctx.openWallet();
    const changedBefore = ctx.uow.store.outboxTypesFor(wallet.id).length;
    const result = await ctx.processWager.execute(
      ctx.command(wallet, { kind: 'LOSS', money: { amount: '0.00', currency: 'BRL' } }),
      HTTP,
    );
    expect(ctx.uow.store.outboxTypesFor(result.transactionId)).toEqual(['WagerTransactionProcessed']);
    expect(ctx.uow.store.outboxTypesFor(wallet.id)).toHaveLength(changedBefore);
  });

  test('mensagem repetida da fila é reconhecida pela inbox e não reaplica', async () => {
    const wallet = await ctx.openWallet('100.00');
    const command = ctx.command(wallet);
    const sqs = {
      correlationId: 'c',
      source: 'sqs' as const,
      inbox: { consumerName: 'consumer', messageId: 'msg-1', payloadHash: 'h'.repeat(64) },
    };
    await ctx.processWager.execute(command, sqs);
    const again = await ctx.processWager.execute(command, sqs);

    expect(again.idempotentReplay).toBe(true);
    expect(ctx.metrics.count('duplicate:inbox_duplicate')).toBe(1);
    expect(ctx.uow.store.inbox.size).toBe(1);
    expect(ctx.uow.store.ledgerFor(wallet.id)).toHaveLength(2);
  });

  test('corrida de unicidade com outra instância → nova tentativa encontra a vencedora e faz replay', async () => {
    const wallet = await ctx.openWallet('100.00');
    const command = ctx.command(wallet);
    const winner = await ctx.processWager.execute(command, HTTP);
    const winnerTx = ctx.uow.store.transactions.get(winner.transactionId)!;

    ctx.uow.store.transactions.delete(winner.transactionId);
    let raced = false;
    ctx.uow.hooks.beforeTransactionInsert = () => {
      if (!raced) {
        raced = true;
        ctx.uow.store.transactions.set(winnerTx.id, winnerTx);
        throw new DuplicateTransactionError('wager_tx_idempotency_key_uq');
      }
    };

    const result = await ctx.processWager.execute(command, HTTP);
    expect(result).toEqual({ ...winner, idempotentReplay: true });
    expect(ctx.metrics.count('lock:unique_race')).toBe(1);
  });

  test(`desiste depois de ${MAX_RACE_ATTEMPTS} corridas seguidas`, async () => {
    const wallet = await ctx.openWallet('100.00');
    ctx.uow.hooks.beforeTransactionInsert = () => {
      throw new DuplicateTransactionError();
    };
    await expect(ctx.processWager.execute(ctx.command(wallet), HTTP)).rejects.toBeInstanceOf(DuplicateTransactionError);
    expect(ctx.metrics.count('retry:race')).toBe(MAX_RACE_ATTEMPTS - 1);
  });
});
