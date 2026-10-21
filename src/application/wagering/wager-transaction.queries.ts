import { TransactionNotFoundError } from '../errors';
import { UnitOfWork } from '../ports/unit-of-work';
import { assertUuid } from '../validation';
import { toView, WagerTransactionView } from './wager-transaction.views';

export class WagerTransactionQueries {
  constructor(private readonly uow: UnitOfWork) {}

  async byId(transactionId: string): Promise<WagerTransactionView> {
    assertUuid('transactionId', transactionId);
    const transaction = await this.uow.run((scope) => scope.transactions.findById(transactionId.toLowerCase()));
    if (!transaction) {
      throw new TransactionNotFoundError(transactionId);
    }
    return toView(transaction);
  }

  async byExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransactionView> {
    const transaction = await this.uow.run((scope) =>
      scope.transactions.findByExternalId(providerId, externalTransactionId),
    );
    if (!transaction) {
      throw new TransactionNotFoundError(`${providerId}/${externalTransactionId}`);
    }
    return toView(transaction);
  }
}
