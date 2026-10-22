import { ConflictError, IntegrityViolationError, InvalidPayloadError, NotFoundError } from '../../application/errors';
import { DomainError } from '../../domain/shared/domain-error';
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

export interface MessageContext {
  messageId?: string;
  correlationId?: string;
  walletId?: string;
  providerId?: string;
  externalTransactionId?: string;
}

export type MessageDecision = { context: MessageContext } & (
  | { action: 'ack'; committed: boolean }
  | { action: 'retry'; reason: string }
  | { action: 'dead-letter'; reason: string }
);

type ParsedEnvelope = { ok: true; envelope: WagerTransactionMessage } | { ok: false; reason: string };

export function classify(error: unknown): ErrorClass {
  if (
    error instanceof InvalidPayloadError ||
    error instanceof IntegrityViolationError ||
    error instanceof DomainError
  ) {
    return 'permanent';
  }
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
      return { action: 'dead-letter', reason: parsed.reason, context: {} };
    }
    const { envelope } = parsed;
    const correlationId = envelope.correlationId ?? envelope.messageId;
    const context: MessageContext = {
      messageId: envelope.messageId,
      correlationId,
      walletId: envelope.data.walletId,
      providerId: envelope.data.providerId,
      externalTransactionId: envelope.data.externalTransactionId,
    };
    return RequestContextStore.run({ correlationId, messageId: envelope.messageId }, () =>
      this.process(envelope, correlationId, context),
    );
  }

  private async process(
    envelope: WagerTransactionMessage,
    correlationId: string,
    context: MessageContext,
  ): Promise<MessageDecision> {
    const { idempotencyKey, ...business } = envelope.data;
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
        ...context,
        transactionId: result.transactionId,
        status: result.status,
        idempotentReplay: result.idempotentReplay,
      });
      return { action: 'ack', committed: true, context };
    } catch (error) {
      return this.decide(error, context);
    }
  }

  private decide(error: unknown, context: MessageContext): MessageDecision {
    const code = errorCode(error);
    switch (classify(error)) {
      case 'business':
        this.logger.warn('sqs message rejected by business rule', { ...context, error: code });
        return { action: 'ack', committed: false, context };
      case 'permanent':
        return { action: 'dead-letter', reason: `${code}: ${describe(error)}`, context };
      case 'transient':
        this.logger.warn('sqs message failed transiently', { ...context, error: code }, error);
        return { action: 'retry', reason: code, context };
    }
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
