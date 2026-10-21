import { IsolationLevel } from '@mikro-orm/core';
import { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { TransactionScope, UnitOfWork } from '../../application/ports/unit-of-work';
import { toTransientError } from './database-errors';
import { MikroOrmInboxRepository, MikroOrmOutboxRepository } from './repositories/messaging.repositories';
import { MikroOrmLedgerRepository } from './repositories/ledger.repository';
import { MikroOrmWagerTransactionRepository } from './repositories/wager-transaction.repository';
import { MikroOrmWalletRepository } from './repositories/wallet.repository';

export interface UnitOfWorkOptions {
  /** Tempo máximo esperando o lock de uma wallet antes de desistir com 503/retry (AC-CONC-06). */
  lockTimeoutMs: number;
  /** Teto para qualquer comando dentro da transação. */
  statementTimeoutMs: number;
}

/**
 * Implementação do `UnitOfWork` sobre `EntityManager.transactional()`.
 *
 * - Cada `run` usa um `fork()` do EntityManager: contexto isolado por transação, nada
 *   compartilhado entre requisições concorrentes da mesma instância.
 * - READ COMMITTED + lock pessimista de linha. Não precisamos de SERIALIZABLE porque toda escrita
 *   de saldo passa pelo `FOR UPDATE` da wallet, e as unicidades estão em constraints.
 * - `SET LOCAL lock_timeout`: a espera pelo lock de uma hot wallet é limitada; estourou →
 *   `TransientInfrastructureError` (HTTP 503 / SQS retry), nunca fila infinita.
 */
export class MikroOrmUnitOfWork implements UnitOfWork {
  private readonly timeouts: [string, string];

  constructor(
    private readonly orm: MikroORM,
    options: UnitOfWorkOptions,
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
          // set_config(..., true) == SET LOCAL: vale só para esta transação.
          await em.execute(
            `SELECT set_config('lock_timeout', ?, true), set_config('statement_timeout', ?, true)`,
            this.timeouts,
          );
          return work(createScope(em));
        },
        { isolationLevel: IsolationLevel.READ_COMMITTED },
      );
    } catch (error) {
      throw toTransientError(error) ?? error;
    }
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
