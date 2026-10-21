import { InvalidPayloadError, WalletNotFoundError } from '../errors';
import { UnitOfWork } from '../ports/unit-of-work';
import { assertUuid } from '../validation';
import { LedgerCursor } from './ledger-cursor';
import { LedgerPageView, toLedgerEntryView, toWalletView, WalletView } from './wallet.views';

export const DEFAULT_LEDGER_PAGE_SIZE = 50;
export const MAX_LEDGER_PAGE_SIZE = 200;

export class WalletQueries {
  constructor(private readonly uow: UnitOfWork) {}

  async byId(walletId: string): Promise<WalletView> {
    assertUuid('walletId', walletId);
    const wallet = await this.uow.run((scope) => scope.wallets.findById(walletId.toLowerCase()));
    if (!wallet) {
      throw new WalletNotFoundError(walletId);
    }
    return toWalletView(wallet);
  }

  async ledger(walletId: string, params: { cursor?: string; limit?: number }): Promise<LedgerPageView> {
    assertUuid('walletId', walletId);
    const limit = params.limit ?? DEFAULT_LEDGER_PAGE_SIZE;
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LEDGER_PAGE_SIZE) {
      throw new InvalidPayloadError(`limit must be an integer between 1 and ${MAX_LEDGER_PAGE_SIZE}`);
    }
    const afterVersion = LedgerCursor.decode(params.cursor);
    const id = walletId.toLowerCase();

    return this.uow.run(async (scope) => {
      const wallet = await scope.wallets.findById(id);
      if (!wallet) {
        throw new WalletNotFoundError(walletId);
      }
      const entries = await scope.ledger.page(id, afterVersion, limit + 1);
      const page = entries.slice(0, limit);
      const last = page.at(-1);
      return {
        walletId: id,
        entries: page.map(toLedgerEntryView),
        ...(entries.length > limit && last ? { nextCursor: LedgerCursor.encode(last.walletVersion) } : {}),
      };
    });
  }
}
