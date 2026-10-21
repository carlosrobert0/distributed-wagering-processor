import {
  InboxRepository,
  LedgerRepository,
  OutboxRepository,
  WagerTransactionRepository,
  WalletRepository,
} from './repositories';

/** Repositórios ligados a UMA transação SQL. Tudo o que for feito por eles é commitado junto. */
export interface TransactionScope {
  readonly wallets: WalletRepository;
  readonly transactions: WagerTransactionRepository;
  readonly ledger: LedgerRepository;
  readonly inbox: InboxRepository;
  readonly outbox: OutboxRepository;
}

/**
 * Fronteira transacional. `run` abre uma transação, entrega o escopo e faz commit se o callback
 * resolver, ou rollback se lançar. Erros transitórios de infraestrutura chegam como
 * `TransientInfrastructureError`.
 */
export interface UnitOfWork {
  run<T>(work: (scope: TransactionScope) => Promise<T>): Promise<T>;
}

export const UNIT_OF_WORK = Symbol('UnitOfWork');
