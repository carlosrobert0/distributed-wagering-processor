import { beforeEach, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  InvalidPayloadError,
  TransactionNotFoundError,
  WalletAlreadyExistsError,
  WalletNotFoundError,
} from '../../../src/application/errors';
import { Money } from '../../../src/domain/shared/money';
import { Wallet } from '../../../src/domain/wallet/wallet';
import { buildInMemoryContext, HTTP, InMemoryContext } from '../../support/in-memory-context';

describe('casos de uso de wallet', () => {
  let ctx: InMemoryContext;

  beforeEach(() => {
    ctx = buildInMemoryContext();
  });

  describe('CreateWalletUseCase', () => {
    test('saldo inicial positivo cria OPENING processada, lançamento e eventos', async () => {
      const wallet = await ctx.openWallet('1000.00');
      const [opening] = [...ctx.uow.store.transactions.values()];

      expect(wallet).toMatchObject({ balance: { amount: '1000.00', currency: 'BRL' }, version: 1 });
      expect(opening).toMatchObject({ kind: 'OPENING', status: 'PROCESSED', providerId: 'internal' });
      expect(ctx.uow.store.ledgerFor(wallet.id)).toHaveLength(1);
      expect(ctx.uow.store.outboxTypesFor(opening!.id)).toEqual(['WagerTransactionProcessed']);
      expect(ctx.uow.store.outboxTypesFor(wallet.id)).toEqual(['WalletBalanceChanged']);
    });

    test('saldo inicial zero não cria OPENING nem eventos', async () => {
      const wallet = await ctx.openWallet('0.00');
      expect(ctx.uow.store.transactions.size).toBe(0);
      expect(ctx.uow.store.ledgerFor(wallet.id)).toHaveLength(0);
      expect(ctx.uow.store.outbox.size).toBe(0);
    });

    test('wallet duplicada para player+moeda → WalletAlreadyExistsError', async () => {
      const playerId = randomUUID();
      await ctx.createWallet.execute({ playerId, initialBalance: { amount: '1.00', currency: 'BRL' } }, HTTP);
      await expect(
        ctx.createWallet.execute({ playerId, initialBalance: { amount: '1.00', currency: 'BRL' } }, HTTP),
      ).rejects.toBeInstanceOf(WalletAlreadyExistsError);
    });

    test.each([
      ['amount inválido', { amount: '1', currency: 'BRL' }],
      ['moeda inválida', { amount: '1.00', currency: 'brl' }],
    ])('%s → InvalidPayloadError', async (_label, initialBalance) => {
      await expect(ctx.createWallet.execute({ playerId: 'p', initialBalance }, HTTP)).rejects.toBeInstanceOf(
        InvalidPayloadError,
      );
    });
  });

  describe('WalletQueries', () => {
    test('ledger paginado percorre todas as versões sem repetir', async () => {
      const wallet = await ctx.openWallet('100.00');
      for (let i = 0; i < 4; i++) {
        await ctx.processWager.execute(ctx.command(wallet), HTTP);
      }

      const versions: number[] = [];
      let cursor: string | undefined;
      do {
        const page = await ctx.walletQueries.ledger(wallet.id, { cursor, limit: 2 });
        versions.push(...page.entries.map((entry) => entry.walletVersion));
        cursor = page.nextCursor;
      } while (cursor);

      expect(versions).toEqual([1, 2, 3, 4, 5]);
    });

    test('limite fora do intervalo → InvalidPayloadError', async () => {
      const wallet = await ctx.openWallet();
      await expect(ctx.walletQueries.ledger(wallet.id, { limit: 0 })).rejects.toBeInstanceOf(InvalidPayloadError);
      await expect(ctx.walletQueries.ledger(wallet.id, { limit: 201 })).rejects.toBeInstanceOf(InvalidPayloadError);
    });

    test('wallet e transação inexistentes → NotFound', async () => {
      await expect(ctx.walletQueries.byId(randomUUID())).rejects.toBeInstanceOf(WalletNotFoundError);
      await expect(ctx.transactionQueries.byId(randomUUID())).rejects.toBeInstanceOf(TransactionNotFoundError);
      await expect(ctx.transactionQueries.byExternalId('p', 'x')).rejects.toBeInstanceOf(TransactionNotFoundError);
    });

    test('consulta de transação por id e por external id', async () => {
      const wallet = await ctx.openWallet();
      const command = ctx.command(wallet);
      const result = await ctx.processWager.execute(command, HTTP);

      expect(await ctx.transactionQueries.byId(result.transactionId)).toMatchObject({
        transactionId: result.transactionId,
        kind: 'BET',
        money: { amount: '10.00', currency: 'BRL' },
      });
      expect(
        (await ctx.transactionQueries.byExternalId(command.providerId, command.externalTransactionId)).transactionId,
      ).toBe(result.transactionId);
    });
  });

  describe('ReconcileWalletUseCase', () => {
    test('wallet íntegra → consistente', async () => {
      const wallet = await ctx.openWallet('100.00');
      await ctx.processWager.execute(ctx.command(wallet), HTTP);

      const view = await ctx.reconcile.execute(wallet.id, HTTP);
      expect(view).toMatchObject({
        storedBalance: { amount: '90.00' },
        calculatedBalance: { amount: '90.00' },
        difference: { amount: '0.00' },
        consistent: true,
        checkedEntries: 2,
        issues: [],
      });
      expect(ctx.metrics.count('reconciliation:true')).toBe(1);
    });

    test('saldo divergente é sinalizado e não corrigido', async () => {
      const wallet = await ctx.openWallet('100.00');
      const stored = ctx.uow.store.wallets.get(wallet.id)!;
      ctx.uow.store.wallets.set(
        wallet.id,
        Wallet.rehydrate({
          id: stored.id,
          playerId: stored.playerId,
          currency: stored.currency,
          balance: Money.from({ amount: '150.00', currency: 'BRL' }),
          version: 3,
          createdAt: stored.createdAt,
          updatedAt: stored.updatedAt,
        }),
      );

      const view = await ctx.reconcile.execute(wallet.id, HTTP);
      expect(view.consistent).toBe(false);
      expect(view.difference).toEqual({ amount: '50.00', currency: 'BRL' });
      expect(view.issues).toEqual(
        expect.arrayContaining([
          'STORED_BALANCE_DIFFERS_FROM_LEDGER',
          'LAST_ENTRY_BALANCE_DIFFERS',
          'WALLET_VERSION_DIFFERS_FROM_LEDGER',
        ]),
      );
      expect(ctx.uow.store.wallets.get(wallet.id)!.balance.toJSON().amount).toBe('150.00');
      expect(ctx.metrics.count('reconciliation:false')).toBe(1);
    });
  });
});
