import { MoneyProps } from '../../domain/shared/money';
import { FailureCode } from '../../domain/wagering/failure-code';
import { WagerTransaction, WagerTransactionStatus } from '../../domain/wagering/wager-transaction';

/** Resposta de submissão (POST /wagering/transactions e consumo SQS). */
export interface WagerTransactionResult {
  transactionId: string;
  status: WagerTransactionStatus;
  failureCode?: FailureCode;
  /** Saldo observado no momento da decisão (ausente enquanto PENDING_REFERENCE). */
  balance?: MoneyProps;
  idempotentReplay: boolean;
}

export function toResult(transaction: WagerTransaction, idempotentReplay: boolean): WagerTransactionResult {
  return {
    transactionId: transaction.id,
    status: transaction.status,
    ...(transaction.failureCode ? { failureCode: transaction.failureCode } : {}),
    ...(transaction.balanceAfter ? { balance: transaction.balanceAfter.toJSON() } : {}),
    idempotentReplay,
  };
}

/** Representação completa para consultas. */
export interface WagerTransactionView {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  referenceTransactionId?: string;
  status: WagerTransactionStatus;
  failureCode?: FailureCode;
  balanceAfter?: MoneyProps;
  createdAt: string;
  processedAt?: string;
}

export function toView(transaction: WagerTransaction): WagerTransactionView {
  return {
    transactionId: transaction.id,
    providerId: transaction.providerId,
    externalTransactionId: transaction.externalTransactionId,
    walletId: transaction.walletId,
    playerId: transaction.playerId,
    roundId: transaction.roundId,
    gameId: transaction.gameId,
    kind: transaction.kind,
    money: transaction.money.toJSON(),
    ...(transaction.referenceExternalTransactionId
      ? { referenceExternalTransactionId: transaction.referenceExternalTransactionId }
      : {}),
    ...(transaction.referenceTransactionId ? { referenceTransactionId: transaction.referenceTransactionId } : {}),
    status: transaction.status,
    ...(transaction.failureCode ? { failureCode: transaction.failureCode } : {}),
    ...(transaction.balanceAfter ? { balanceAfter: transaction.balanceAfter.toJSON() } : {}),
    createdAt: transaction.createdAt.toISOString(),
    ...(transaction.processedAt ? { processedAt: transaction.processedAt.toISOString() } : {}),
  };
}
