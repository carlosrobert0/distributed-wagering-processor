import { IsolationLevel } from '@mikro-orm/core';
import { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { Metrics } from '../../application/ports/system';
import { TransactionScope, UnitOfWork } from '../../application/ports/unit-of-work';
import { toTransientError } from './database-errors';
import { MikroOrmInboxRepository, MikroOrmOutboxRepository } from './repositories/messaging.repositories';
import { MikroOrmLedgerRepository } from './repositories/ledger.repository';
import { MikroOrmWagerTransactionRepository } from './repositories/wager-transaction.repository';
import { MikroOrmWalletRepository } from './repositories/wallet.repository';

export interface UnitOfWorkOptions {
  lockTimeoutMs: number;
  statementTimeoutMs: number;
}

export class MikroOrmUnitOfWork implements UnitOfWork {
  private readonly timeouts: [string, string];

  constructor(
    private readonly orm: MikroORM,
    options: UnitOfWorkOptions,
    private readonly metrics?: Metrics,
  ) {
    this.timeouts = [
      `${Math.max(1, Math.trunc(options.lockTimeoutMs))}ms`,
      `${Math.max(1, Math.trunc(options.statementTimeoutMs))}ms`,
    ];
  }

  async run<T>(work: (scope: TransactionScope) => Promise<T>): Promise<T> {
    try {
      return await this.orm.em.fork().transactional(
        async (em) => {
          await this.applyTimeouts(em);
          return work(createScope(em));
        },
        { isolationLevel: IsolationLevel.READ_COMMITTED },
      );
    } catch (error) {
      throw this.translate(error);
    }
  }

  private async applyTimeouts(em: EntityManager): Promise<void> {
    await em.execute(
      `SELECT set_config('lock_timeout', ?, true), set_config('statement_timeout', ?, true)`,
      this.timeouts,
    );
  }

  private translate(error: unknown): unknown {
    const transient = toTransientError(error);
    if (transient?.reason === 'lock_timeout' || transient?.reason === 'deadlock') {
      this.metrics?.lockConflict(transient.reason);
    }
    return transient ?? error;
  }
}

function createScope(em: EntityManager): TransactionScope {
  return {
    wallets: new MikroOrmWalletRepository(em),
    transactions: new MikroOrmWagerTransactionRepository(em),
    ledger: new MikroOrmLedgerRepository(em),
    inbox: new MikroOrmInboxRepository(em),
    outbox: new MikroOrmOutboxRepository(em),
  };
}
