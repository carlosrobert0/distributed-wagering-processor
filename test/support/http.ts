import { randomUUID } from 'node:crypto';

export interface HttpResult<T = Record<string, unknown>> {
  status: number;
  body: T;
  headers: Headers;
}

export interface WagerBody {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: { amount: string; currency: string };
  referenceExternalTransactionId?: string;
}

export interface WagerResponse {
  transactionId: string;
  status: string;
  failureCode?: string;
  balance?: { amount: string; currency: string };
  idempotentReplay: boolean;
  error?: string;
}

/** Cliente HTTP mínimo (fetch nativo) para exercitar a API real. */
export class ApiClient {
  constructor(readonly baseUrl: string) {}

  async request<T = Record<string, unknown>>(
    method: string,
    path: string,
    body?: unknown,
    headers: Record<string, string> = {},
  ): Promise<HttpResult<T>> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { 'content-type': 'application/json', ...headers },
      body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
    });
    const text = await response.text();
    return { status: response.status, body: (text ? JSON.parse(text) : {}) as T, headers: response.headers };
  }

  async createWallet(initialAmount: string, options: { playerId?: string; currency?: string } = {}) {
    const result = await this.request<{ id: string; playerId: string; balance: { amount: string }; version: number }>(
      'POST',
      '/wallets',
      {
        playerId: options.playerId ?? randomUUID(),
        initialBalance: { amount: initialAmount, currency: options.currency ?? 'BRL' },
      },
    );
    if (result.status !== 201) {
      throw new Error(`createWallet failed: ${result.status} ${JSON.stringify(result.body)}`);
    }
    return result.body;
  }

  submit(body: WagerBody, idempotencyKey: string | null = `${body.providerId}:${body.externalTransactionId}`) {
    return this.request<WagerResponse>(
      'POST',
      '/wagering/transactions',
      body,
      idempotencyKey === null ? {} : { 'idempotency-key': idempotencyKey },
    );
  }
}

/** Monta o corpo de uma transação com defaults razoáveis. */
export function wager(
  wallet: { id: string; playerId: string },
  overrides: Partial<WagerBody> & { amount?: string } = {},
): WagerBody {
  const { amount, ...rest } = overrides;
  return {
    providerId: 'provider-a',
    externalTransactionId: `tx-${randomUUID()}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: 'round-1',
    gameId: 'fortune-chimp',
    kind: 'BET',
    money: { amount: amount ?? '10.00', currency: 'BRL' },
    ...rest,
  };
}
