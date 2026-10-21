import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { prepareTestDatabase } from '../support/environment';
import {
  countRows,
  expectWalletMatchesLedger,
  ledgerRows,
  outboxEventTypes,
  transactionRows,
  walletRow,
} from '../support/database';
import { buildUseCases, FailAfterOutboxUnitOfWork } from '../support/use-cases';

describe('atomicidade wallet + ledger + inbox + outbox', () => {
  let ctx: Awaited<ReturnType<typeof buildUseCases>>;

  beforeAll(async () => {
    await prepareTestDatabase();
    ctx = await buildUseCases();
  });
  afterAll(() => ctx.close());

  async function newWallet(amount = '100.00') {
    return ctx.createWallet.execute(
      { playerId: randomUUID(), initialBalance: { amount, currency: 'BRL' } },
      { correlationId: 'test' },
    );
  }

  function betCommand(wallet: { id: string; playerId: string }, externalTransactionId = `tx-${randomUUID()}`) {
    return {
      idempotencyKey: `provider-a:${externalTransactionId}`,
      providerId: 'provider-a',
      externalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };
  }

  test('criação de wallet: wallet + OPENING + lançamento + eventos na mesma transação (AC-WAL-06)', async () => {
    const wallet = await newWallet('1000.00');
    const transactions = await transactionRows(wallet.id);

    expect(wallet.version).toBe(1);
    expect(transactions).toHaveLength(1);
    expect(transactions[0]).toMatchObject({ kind: 'OPENING', status: 'PROCESSED', balance_after: '1000.00' });
    expect(await ledgerRows(wallet.id)).toEqual([
      expect.objectContaining({
        wallet_version: 1,
        direction: 'CREDIT',
        amount: '1000.00',
        balance_before: '0.00',
        balance_after: '1000.00',
      }),
    ]);
    expect(await outboxEventTypes([wallet.id, transactions[0]!.id])).toEqual(
      expect.arrayContaining(['WagerTransactionProcessed', 'WalletBalanceChanged']),
    );
    await expectWalletMatchesLedger(wallet.id);
  });

  test('sucesso via fila: inbox + transação + lançamento + saldo + outbox gravados juntos', async () => {
    const wallet = await newWallet();
    const messageId = `msg-${randomUUID()}`;
    const result = await ctx.processWager().execute(betCommand(wallet), {
      correlationId: 'c-1',
      causationId: messageId,
      source: 'sqs',
      inbox: { consumerName: 'test-consumer', messageId, payloadHash: 'a'.repeat(64) },
    });

    expect(result).toMatchObject({
      status: 'PROCESSED',
      balance: { amount: '75.00', currency: 'BRL' },
      idempotentReplay: false,
    });
    expect((await walletRow(wallet.id)).balance).toBe('75.00');
    expect(await ledgerRows(wallet.id)).toHaveLength(2);
    expect(await countRows(`SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1`, [messageId])).toBe(1);
    expect(await outboxEventTypes([result.transactionId, wallet.id])).toEqual(
      expect.arrayContaining(['WagerTransactionProcessed', 'WalletBalanceChanged']),
    );
    await expectWalletMatchesLedger(wallet.id);
  });

  test('falha depois de gravar tudo e antes do commit → NADA persiste (nem inbox, nem outbox)', async () => {
    const wallet = await newWallet();
    const command = betCommand(wallet);
    const messageId = `msg-${randomUUID()}`;
    const faulty = ctx.processWager(new FailAfterOutboxUnitOfWork(ctx.uow));

    await expect(
      faulty.execute(command, {
        correlationId: 'c-2',
        source: 'sqs',
        inbox: { consumerName: 'test-consumer', messageId, payloadHash: 'b'.repeat(64) },
      }),
    ).rejects.toThrow('simulated crash');

    expect(await walletRow(wallet.id)).toMatchObject({ balance: '100.00', version: 1 });
    expect(await ledgerRows(wallet.id)).toHaveLength(1);
    expect(
      await countRows(`SELECT COUNT(*) FROM wager_transactions WHERE external_transaction_id = $1`, [
        command.externalTransactionId,
      ]),
    ).toBe(0);
    expect(await countRows(`SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1`, [messageId])).toBe(0);
    expect(
      await countRows(
        `SELECT COUNT(*) FROM outbox_messages WHERE partition_key = $1 AND event_type = 'WalletBalanceChanged' AND payload->'data'->>'walletVersion' = '2'`,
        [wallet.id],
      ),
    ).toBe(0);

    const retried = await ctx.processWager().execute(command, {
      correlationId: 'c-2',
      source: 'sqs',
      inbox: { consumerName: 'test-consumer', messageId, payloadHash: 'b'.repeat(64) },
    });
    expect(retried).toMatchObject({ status: 'PROCESSED', idempotentReplay: false });
    expect((await walletRow(wallet.id)).balance).toBe('75.00');
    await expectWalletMatchesLedger(wallet.id);
  });

  test('rejeição de negócio persiste a transação REJECTED + evento, sem lançamento e sem mudar a version', async () => {
    const wallet = await newWallet('10.00');
    const result = await ctx.processWager().execute(betCommand(wallet), { correlationId: 'c-3', source: 'http' });

    expect(result).toMatchObject({
      status: 'REJECTED',
      failureCode: 'INSUFFICIENT_FUNDS',
      balance: { amount: '10.00' },
    });
    expect(await walletRow(wallet.id)).toMatchObject({ balance: '10.00', version: 1 });
    expect(await ledgerRows(wallet.id)).toHaveLength(1);
    expect(await outboxEventTypes([result.transactionId])).toEqual(['WagerTransactionRejected']);
    await expectWalletMatchesLedger(wallet.id);
  });
});
