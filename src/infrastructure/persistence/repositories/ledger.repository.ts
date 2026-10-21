import { EntityManager } from '@mikro-orm/postgresql';
import { LedgerRepository, LedgerSummary } from '../../../application/ports/repositories';
import { WalletLedgerEntry } from '../../../domain/wallet/wallet-ledger-entry';
import { LedgerEntryMapper } from '../mappers';
import { LedgerEntryRecord } from '../records';

interface SummaryRow {
  net_amount: string;
  entries: number;
  last_balance_after: string | null;
  last_wallet_version: number | null;
  chain_breaks: number;
}

const SUMMARY_SQL = `
  WITH ordered AS (
    SELECT direction, amount, balance_before, balance_after, wallet_version,
           LAG(balance_after)  OVER (ORDER BY wallet_version) AS previous_after,
           LAG(wallet_version) OVER (ORDER BY wallet_version) AS previous_version
      FROM wallet_ledger_entries
     WHERE wallet_id = ?
  )
  SELECT
    COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)::numeric(22,2)::text AS net_amount,
    COUNT(*)::int AS entries,
    (SELECT o.balance_after::text FROM ordered o ORDER BY o.wallet_version DESC LIMIT 1) AS last_balance_after,
    MAX(wallet_version)::int AS last_wallet_version,
    COUNT(*) FILTER (
      WHERE previous_after IS NOT NULL
        AND (balance_before <> previous_after OR wallet_version <> previous_version + 1)
    )::int AS chain_breaks
  FROM ordered
`;

export class MikroOrmLedgerRepository implements LedgerRepository {
  constructor(private readonly em: EntityManager) {}

  async append(entry: WalletLedgerEntry): Promise<void> {
    await this.em.insert(LedgerEntryRecord, LedgerEntryMapper.toRecord(entry));
  }

  async page(walletId: string, afterVersion: number | undefined, limit: number): Promise<WalletLedgerEntry[]> {
    const records = await this.em.find(
      LedgerEntryRecord,
      afterVersion === undefined ? { walletId } : { walletId, walletVersion: { $gt: afterVersion } },
      { disableIdentityMap: true, orderBy: { walletVersion: 'asc' }, limit },
    );
    return records.map((record) => LedgerEntryMapper.toDomain(record));
  }

  async summarize(walletId: string): Promise<LedgerSummary> {
    const [row] = await this.em.execute<SummaryRow[]>(SUMMARY_SQL, [walletId]);
    if (!row) {
      return { netAmount: '0.00', entries: 0, chainBreaks: 0 };
    }
    return {
      netAmount: row.net_amount,
      entries: Number(row.entries),
      ...(row.last_balance_after !== null ? { lastBalanceAfter: row.last_balance_after } : {}),
      ...(row.last_wallet_version !== null ? { lastWalletVersion: Number(row.last_wallet_version) } : {}),
      chainBreaks: Number(row.chain_breaks),
    };
  }
}
