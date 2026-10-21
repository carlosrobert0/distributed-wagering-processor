import { ConflictError, InvalidPayloadError, NotFoundError } from '../../application/errors';
import { AppLogger } from '../../application/ports/system';
import { canonicalJson, sha256Hex } from '../../application/wagering/payload-hash';
import { ProcessWagerTransactionUseCase } from '../../application/wagering/process-wager-transaction.use-case';
import { RequestContextStore } from '../../infrastructure/observability/request-context';
import {
  describeZodError,
  WagerTransactionMessage,
  WagerTransactionMessageContract,
} from '../contracts/wager-transaction.contract';

export type ErrorClass = 'business' | 'transient' | 'permanent';

export type MessageDecision =
  | { action: 'ack'; committed: boolean }
  | { action: 'retry'; reason: string }
  | { action: 'dead-letter'; reason: string };

type ParsedEnvelope = { ok: true; envelope: WagerTransactionMessage } | { ok: false; reason: string };

export function classify(error: unknown): ErrorClass {
  if (error instanceof InvalidPayloadError) return 'permanent';
  if (error instanceof ConflictError || error instanceof NotFoundError) return 'business';
  return 'transient';
}

export function parseEnvelope(body: string | undefined): ParsedEnvelope {
  let json: unknown;
  try {
    json = JSON.parse(body ?? '');
  } catch {
    return { ok: false, reason: 'MALFORMED_JSON' };
  }
  const result = WagerTransactionMessageContract.safeParse(json);
  if (!result.success) {
    return { ok: false, reason: `INVALID_ENVELOPE: ${describeZodError(result.error).join('; ')}` };
  }
  return { ok: true, envelope: result.data };
}

function errorCode(error: unknown): string {
  const code = (error as { code?: unknown } | undefined)?.code;
  if (typeof code === 'string') return code;
  return error instanceof Error ? error.name : 'UNKNOWN';
}

export class WagerMessageHandler {
  constructor(
    private readonly processWager: ProcessWagerTransactionUseCase,
    private readonly consumerName: string,
    private readonly logger: AppLogger,
  ) {}

  async handle(body: string | undefined): Promise<MessageDecision> {
    const parsed = parseEnvelope(body);
    if (!parsed.ok) {
      return { action: 'dead-letter', reason: parsed.reason };
    }
    const { envelope } = parsed;
    const correlationId = envelope.correlationId ?? envelope.messageId;
    return RequestContextStore.run({ correlationId, messageId: envelope.messageId }, () =>
      this.process(envelope, correlationId),
    );
  }

  private async process(envelope: WagerTransactionMessage, correlationId: string): Promise<MessageDecision> {
    const { idempotencyKey, ...business } = envelope.data;
    const fields = { walletId: business.walletId, providerId: business.providerId };
    try {
      const result = await this.processWager.execute(
        { ...business, idempotencyKey },
        {
          correlationId,
          causationId: envelope.messageId,
          source: 'sqs',
          inbox: {
            consumerName: this.consumerName,
            messageId: envelope.messageId,
            payloadHash: sha256Hex(canonicalJson(envelope.data)),
          },
        },
      );
      this.logger.info('sqs message processed', {
        ...fields,
        transactionId: result.transactionId,
        status: result.status,
        idempotentReplay: result.idempotentReplay,
      });
      return { action: 'ack', committed: true };
    } catch (error) {
      return this.decide(error, fields);
    }
  }

  private decide(error: unknown, fields: { walletId: string; providerId: string }): MessageDecision {
    const code = errorCode(error);
    switch (classify(error)) {
      case 'business':
        this.logger.warn('sqs message rejected by business rule', { ...fields, error: code });
        return { action: 'ack', committed: false };
      case 'permanent':
        return { action: 'dead-letter', reason: `${code}: ${error instanceof Error ? error.message : String(error)}` };
      case 'transient':
        return { action: 'retry', reason: code };
    }
  }
}
