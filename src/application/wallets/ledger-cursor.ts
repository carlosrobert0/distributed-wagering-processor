import { InvalidPayloadError } from '../errors';

/**
 * Cursor opaco e estável da paginação do ledger (AC-API-03).
 *
 * Codifica a última `wallet_version` entregue. Como a versão é única por wallet e o ledger é
 * append-only, o cursor nunca pula nem repete lançamentos, mesmo com escritas concorrentes.
 * O formato é versionado (`v`) para poder evoluir sem quebrar clientes.
 */
export const LedgerCursor = {
  encode(afterVersion: number): string {
    return Buffer.from(JSON.stringify({ v: 1, after: afterVersion }), 'utf8').toString('base64url');
  },

  decode(cursor: string | undefined): number | undefined {
    if (cursor === undefined || cursor === '') {
      return undefined;
    }
    try {
      const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as { v?: unknown; after?: unknown };
      if (parsed.v === 1 && Number.isSafeInteger(parsed.after) && (parsed.after as number) >= 0) {
        return parsed.after as number;
      }
    } catch {
      // cai no erro abaixo
    }
    throw new InvalidPayloadError('Invalid ledger cursor', ['cursor: malformed']);
  },
};
