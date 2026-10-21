import { Money } from '../../domain/shared/money';
import { WalletNotFoundError } from '../errors';
import { AppLogger, Metrics } from '../ports/system';
import { UnitOfWork } from '../ports/unit-of-work';
import { assertUuid } from '../validation';
import { ReconciliationView } from './wallet.views';

export class ReconcileWalletUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async execute(walletId: string, ctx: { correlationId: string }): Promise<ReconciliationView> {
    assertUuid('walletId', walletId);
    const id = walletId.toLowerCase();

    const view = await this.uow.run(async (scope) => {
      const wallet = await scope.wallets.findByIdForShare(id);
      if (!wallet) {
        throw new WalletNotFoundError(walletId);
      }
      const summary = await scope.ledger.summarize(id);

      const stored = wallet.balance;
      const calculated = Money.rehydrate({ amount: summary.netAmount, currency: wallet.currency });
      const difference = stored.subtract(calculated);

      const issues: string[] = [];
      if (!difference.isZero()) {
        issues.push('STORED_BALANCE_DIFFERS_FROM_LEDGER');
      }
      if (summary.chainBreaks > 0) {
        issues.push('LEDGER_CHAIN_BROKEN');
      }
      if (summary.lastBalanceAfter !== undefined && summary.lastBalanceAfter !== stored.toJSON().amount) {
        issues.push('LAST_ENTRY_BALANCE_DIFFERS');
      }
      if (summary.lastWalletVersion !== undefined && summary.lastWalletVersion !== wallet.version) {
        issues.push('WALLET_VERSION_DIFFERS_FROM_LEDGER');
      }

      return {
        walletId: id,
        storedBalance: stored.toJSON(),
        calculatedBalance: calculated.toJSON(),
        difference: difference.toJSON(),
        consistent: issues.length === 0,
        checkedEntries: summary.entries,
        issues,
      } satisfies ReconciliationView;
    });

    this.metrics.reconciliationChecked(view.consistent);
    if (!view.consistent) {
      this.logger.error('wallet reconciliation mismatch', {
        correlationId: ctx.correlationId,
        walletId: id,
        issues: view.issues.join(','),
        checkedEntries: view.checkedEntries,
      });
    }
    return view;
  }
}
