import { z } from 'zod';

const notBlank = (value: string) => value.trim().length > 0;
const identifier = z.string().min(1).max(128).refine(notBlank, 'must not be blank');

export const MoneyContract = z.object({
  amount: z.string({ invalid_type_error: 'amount must be a decimal string, e.g. "25.00"' }),
  currency: z.string(),
});

export const WagerTransactionContract = z.object({
  providerId: identifier,
  externalTransactionId: identifier,
  playerId: identifier,
  walletId: z.string().uuid(),
  roundId: identifier,
  gameId: identifier,
  kind: z.string(),
  money: MoneyContract,
  referenceExternalTransactionId: identifier.optional(),
});

export type WagerTransactionInput = z.infer<typeof WagerTransactionContract>;

export const WagerTransactionMessageContract = z.object({
  messageId: z.string().min(1).max(256).refine(notBlank, 'must not be blank'),
  type: z.literal('WagerTransactionRequested'),
  occurredAt: z.string().datetime({ offset: true }),
  correlationId: z.string().min(1).max(128).optional(),
  data: WagerTransactionContract.extend({
    idempotencyKey: z.string().min(1).max(255).refine(notBlank, 'must not be blank'),
  }),
});

export type WagerTransactionMessage = z.infer<typeof WagerTransactionMessageContract>;

export const CreateWalletContract = z.object({
  playerId: identifier,
  initialBalance: MoneyContract,
});

export function describeZodError(error: z.ZodError): string[] {
  return error.issues.map((issue) => `${issue.path.join('.') || '(root)'}: ${issue.message}`);
}
