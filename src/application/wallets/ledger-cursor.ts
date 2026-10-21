import { InvalidPayloadError } from '../errors';

const CURSOR_VERSION = 1;

interface CursorPayload {
  v: number;
  after: number;
}

function parsePayload(cursor: string): unknown {
  try {
    return JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch {
    return undefined;
  }
}

function isValidPayload(payload: unknown): payload is CursorPayload {
  const candidate = payload as Partial<CursorPayload> | undefined;
  return candidate?.v === CURSOR_VERSION && Number.isSafeInteger(candidate.after) && (candidate.after ?? -1) >= 0;
}

export const LedgerCursor = {
  encode(afterVersion: number): string {
    const payload: CursorPayload = { v: CURSOR_VERSION, after: afterVersion };
    return Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
  },

  decode(cursor: string | undefined): number | undefined {
    if (cursor === undefined || cursor === '') {
      return undefined;
    }
    const payload = parsePayload(cursor);
    if (!isValidPayload(payload)) {
      throw new InvalidPayloadError('Invalid ledger cursor', ['cursor: malformed']);
    }
    return payload.after;
  },
};
