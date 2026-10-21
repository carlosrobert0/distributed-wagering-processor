import { createHash } from 'node:crypto';
import { MoneyProps } from '../../domain/shared/money';

/**
 * Campos de negócio que compõem a impressão digital de uma transação (AC-IDEM-02).
 * Ficam de fora: o header `Idempotency-Key`/`idempotencyKey`, `messageId`, `occurredAt`,
 * correlation ids e qualquer outro metadado de transporte.
 */
export interface WagerBusinessFields {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

/**
 * JSON canônico: chaves de objetos ordenadas lexicograficamente (recursivo), sem espaços,
 * `undefined` omitido. Arrays mantêm a ordem. Resultado é determinístico para o mesmo conteúdo.
 */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item ?? null)).join(',')}]`;
  }
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`);
  return `{${entries.join(',')}}`;
}

export function sha256Hex(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex');
}

/** `payloadHash = sha256(canonicalJson(businessFields))` em hexadecimal (64 caracteres). */
export function computeWagerPayloadHash(fields: WagerBusinessFields): string {
  const business: WagerBusinessFields = {
    providerId: fields.providerId,
    externalTransactionId: fields.externalTransactionId,
    playerId: fields.playerId,
    walletId: fields.walletId,
    roundId: fields.roundId,
    gameId: fields.gameId,
    kind: fields.kind,
    money: { amount: fields.money.amount, currency: fields.money.currency },
    referenceExternalTransactionId: fields.referenceExternalTransactionId,
  };
  return sha256Hex(canonicalJson(business));
}
