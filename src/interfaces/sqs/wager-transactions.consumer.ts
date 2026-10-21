import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  Message,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import {
  ConflictError,
  InvalidPayloadError,
  NotFoundError,
  TransientInfrastructureError,
} from '../../application/errors';
import { AppLogger, Metrics } from '../../application/ports/system';
import { canonicalJson, sha256Hex } from '../../application/wagering/payload-hash';
import { ProcessWagerTransactionUseCase } from '../../application/wagering/process-wager-transaction.use-case';
import { ExponentialBackoff } from '../../domain/shared/exponential-backoff';
import { SqsQueues } from '../../infrastructure/messaging/sqs';
import { RequestContextStore } from '../../infrastructure/observability/request-context';
import { describeZodError, WagerTransactionMessageContract } from '../contracts/wager-transaction.contract';

export interface WagerConsumerOptions {
  consumerName: string;
  queueName: string;
  dlqName: string;
  waitTimeSeconds: number;
  visibilityTimeoutSeconds: number;
  batchSize: number;
  retryBaseSeconds: number;
  retryMaxSeconds: number;
  shutdownGraceMs: number;
  faultInjection: 'none' | 'crash-after-commit';
}

/** Desfecho de uma mensagem — usado em logs e testes. */
export type MessageDisposition = 'acked' | 'retry-scheduled' | 'dead-lettered' | 'released';

/** Classificação de erros (AC-SQS-05). */
type ErrorClass = 'business' | 'transient' | 'permanent';

/**
 * Consumer da fila `wager-transactions.fifo` (§10).
 *
 * - Reusa o MESMO use case do HTTP; a inbox é gravada na mesma transação SQL (AC-SQS-02/03).
 * - Ack (`DeleteMessage`) só depois que o use case retorna, ou seja, depois do commit (AC-SQS-04).
 * - Negócio → ack. Transitório → visibilidade com backoff exponencial; quando o
 *   `maxReceiveCount` da redrive policy estoura, o SQS move para a DLQ. Permanente → DLQ já.
 * - Paralelo entre MessageGroups (wallets), sequencial dentro de um grupo.
 * - SIGTERM: para de buscar, termina o que está em andamento e devolve o resto (AC-SQS-07).
 */
export class WagerTransactionsConsumer {
  private running = false;
  private loop?: Promise<void>;
  private pollAbort = new AbortController();
  private readonly inFlight = new Set<Promise<unknown>>();
  private readonly backoff: ExponentialBackoff;
  private maxReceiveCount?: number;

  constructor(
    private readonly processWager: ProcessWagerTransactionUseCase,
    private readonly queues: SqsQueues,
    private readonly options: WagerConsumerOptions,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {
    const base = Math.max(1, options.retryBaseSeconds);
    this.backoff = new ExponentialBackoff(base, Math.max(base, options.retryMaxSeconds));
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.pollAbort = new AbortController();
    this.loop = this.pollLoop();
    this.logger.info('sqs consumer started', { queue: this.options.queueName });
  }

  /** Graceful shutdown: chamado no SIGTERM (via hook do Nest) e pelos testes. */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.pollAbort.abort();
    const drained = Promise.allSettled([...this.inFlight]).then(() => true);
    const timedOut = new Promise<boolean>((resolve) => setTimeout(() => resolve(false), this.options.shutdownGraceMs));
    const finished = await Promise.race([drained, timedOut]);
    await this.loop?.catch(() => undefined);
    this.logger.info('sqs consumer stopped', { drained: finished });
  }

  private async pollLoop(): Promise<void> {
    while (this.running) {
      try {
        const queueUrl = await this.queues.url(this.options.queueName);
        if (this.maxReceiveCount === undefined) {
          this.maxReceiveCount = await this.queues.maxReceiveCount(this.options.queueName).catch(() => undefined);
        }
        const response = await this.queues.client.send(
          new ReceiveMessageCommand({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: this.options.batchSize,
            WaitTimeSeconds: this.options.waitTimeSeconds,
            VisibilityTimeout: this.options.visibilityTimeoutSeconds,
            MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
            AttributeNames: ['All'], // compatibilidade com emuladores que ainda leem o nome antigo
          }),
          { abortSignal: this.pollAbort.signal },
        );
        const messages = response.Messages ?? [];
        if (!this.running) {
          // Chegou depois do SIGTERM: devolve imediatamente para outra instância.
          await this.releaseAll(queueUrl, messages);
          break;
        }
        if (messages.length > 0) {
          await this.track(this.processBatch(queueUrl, messages));
        }
      } catch (error) {
        if (!this.running) break;
        this.logger.warn('sqs poll failed; backing off', { reason: error instanceof Error ? error.message : String(error) });
        await sleep(1_000);
      }
    }
  }

  private async processBatch(queueUrl: string, messages: Message[]): Promise<void> {
    const groups = new Map<string, Message[]>();
    for (const message of messages) {
      const group = message.Attributes?.MessageGroupId ?? message.MessageId ?? 'default';
      groups.set(group, [...(groups.get(group) ?? []), message]);
    }
    await Promise.all([...groups.values()].map((group) => this.processGroup(queueUrl, group)));
  }

  /** Sequencial dentro do grupo para respeitar a ordem FIFO por wallet. */
  private async processGroup(queueUrl: string, group: Message[]): Promise<void> {
    for (let index = 0; index < group.length; index++) {
      const message = group[index]!;
      if (!this.running) {
        await this.releaseAll(queueUrl, group.slice(index));
        return;
      }
      const disposition = await this.handle(queueUrl, message);
      if (disposition === 'retry-scheduled') {
        // Mantém a ordem: o resto do grupo volta junto, depois da mensagem que falhou.
        const delay = this.retryDelaySeconds(message);
        await Promise.all(group.slice(index + 1).map((rest) => this.changeVisibility(queueUrl, rest, delay)));
        return;
      }
    }
  }

  /** Processa uma mensagem e decide ack / retry / DLQ. Público para os testes de integração. */
  async handle(queueUrl: string, message: Message): Promise<MessageDisposition> {
    const parsed = this.parse(message);
    if (!parsed.ok) {
      return this.deadLetter(queueUrl, message, parsed.reason);
    }
    const envelope = parsed.envelope;
    const correlationId = envelope.correlationId ?? envelope.messageId;
    const { idempotencyKey, ...business } = envelope.data;

    return RequestContextStore.run({ correlationId, messageId: envelope.messageId }, async () => {
      try {
        const result = await this.processWager.execute(
          { ...business, idempotencyKey },
          {
            correlationId,
            causationId: envelope.messageId,
            source: 'sqs',
            inbox: {
              consumerName: this.options.consumerName,
              messageId: envelope.messageId,
              payloadHash: sha256Hex(canonicalJson(envelope.data)),
            },
          },
        );
        this.injectCrashAfterCommit();
        await this.ack(queueUrl, message);
        this.logger.info('sqs message processed', {
          transactionId: result.transactionId,
          walletId: business.walletId,
          providerId: business.providerId,
          status: result.status,
          idempotentReplay: result.idempotentReplay,
        });
        return 'acked';
      } catch (error) {
        return this.onFailure(queueUrl, message, error, {
          walletId: business.walletId,
          providerId: business.providerId,
        });
      }
    });
  }

  private async onFailure(
    queueUrl: string,
    message: Message,
    error: unknown,
    fields: { walletId: string; providerId: string },
  ): Promise<MessageDisposition> {
    const kind = classify(error);
    const code = (error as { code?: string }).code;

    if (kind === 'business') {
      // Terminal: reenviar não muda o resultado (conflito de key, wallet inexistente).
      this.logger.warn('sqs message rejected by business rule; acking', { ...fields, error: code });
      await this.ack(queueUrl, message);
      return 'acked';
    }
    if (kind === 'permanent') {
      return this.deadLetter(queueUrl, message, `${code ?? 'PERMANENT'}: ${(error as Error).message}`);
    }

    const receiveCount = receiveCountOf(message);
    const delay = this.retryDelaySeconds(message);
    this.metrics.retry('sqs_consumer');
    if (this.maxReceiveCount !== undefined && receiveCount >= this.maxReceiveCount) {
      // Na próxima entrega o SQS move para a DLQ pela redrive policy.
      this.metrics.deadLettered('max_receive_count');
    }
    this.logger.warn('sqs message failed transiently; retry scheduled', {
      ...fields,
      error: code ?? (error instanceof Error ? error.name : 'unknown'),
      receiveCount,
      retryInSeconds: delay,
    });
    await this.changeVisibility(queueUrl, message, delay);
    return 'retry-scheduled';
  }

  private parse(
    message: Message,
  ):
    | { ok: true; envelope: ReturnType<typeof WagerTransactionMessageContract.parse> }
    | { ok: false; reason: string } {
    let json: unknown;
    try {
      json = JSON.parse(message.Body ?? '');
    } catch {
      return { ok: false, reason: 'MALFORMED_JSON' };
    }
    const result = WagerTransactionMessageContract.safeParse(json);
    if (!result.success) {
      return { ok: false, reason: `INVALID_ENVELOPE: ${describeZodError(result.error).join('; ')}` };
    }
    return { ok: true, envelope: result.data };
  }

  /** Permanente: copia para a DLQ com o motivo e remove da fila principal. */
  private async deadLetter(queueUrl: string, message: Message, reason: string): Promise<MessageDisposition> {
    const dlqUrl = await this.queues.url(this.options.dlqName);
    await this.queues.client.send(
      new SendMessageCommand({
        QueueUrl: dlqUrl,
        MessageBody: message.Body ?? '',
        MessageGroupId: message.Attributes?.MessageGroupId ?? 'dead-letter',
        MessageDeduplicationId: message.MessageId ?? `${Date.now()}`,
        MessageAttributes: {
          deadLetterReason: { DataType: 'String', StringValue: reason.slice(0, 1_000) },
          originalMessageId: { DataType: 'String', StringValue: message.MessageId ?? 'unknown' },
        },
      }),
    );
    await this.ack(queueUrl, message);
    this.metrics.deadLettered('permanent');
    this.logger.error('sqs message moved to DLQ', { sqsMessageId: message.MessageId, reason: reason.slice(0, 200) });
    return 'dead-lettered';
  }

  private async ack(queueUrl: string, message: Message): Promise<void> {
    await this.queues.client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }));
  }

  private async changeVisibility(queueUrl: string, message: Message, seconds: number): Promise<void> {
    try {
      await this.queues.client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: seconds,
        }),
      );
    } catch (error) {
      // Se falhar, o visibility timeout padrão devolve a mensagem de qualquer forma.
      this.logger.warn('could not change message visibility', {
        sqsMessageId: message.MessageId,
        reason: error instanceof Error ? error.message : String(error),
      });
    }
  }

  private async releaseAll(queueUrl: string, messages: Message[]): Promise<void> {
    await Promise.all(messages.map((message) => this.changeVisibility(queueUrl, message, 0)));
  }

  private retryDelaySeconds(message: Message): number {
    return Math.min(43_200, Math.round(this.backoff.delayFor(receiveCountOf(message))));
  }

  private async track<T>(work: Promise<T>): Promise<T> {
    this.inFlight.add(work);
    try {
      return await work;
    } finally {
      this.inFlight.delete(work);
    }
  }

  /** Teste do cenário 5 da §13: morre depois do commit e antes do ack. Nunca ligado em produção. */
  private injectCrashAfterCommit(): void {
    if (this.options.faultInjection === 'crash-after-commit') {
      this.logger.warn('FAULT INJECTION: killing process after commit, before ack');
      process.kill(process.pid, 'SIGKILL');
    }
  }
}

export function classify(error: unknown): ErrorClass {
  if (error instanceof InvalidPayloadError) return 'permanent';
  if (error instanceof ConflictError || error instanceof NotFoundError) return 'business';
  if (error instanceof TransientInfrastructureError) return 'transient';
  // Desconhecido: tratamos como transitório. Se for um bug determinístico, a redrive policy leva
  // a mensagem para a DLQ após `maxReceiveCount` tentativas — nada se perde, nada duplica.
  return 'transient';
}

function receiveCountOf(message: Message): number {
  return Number(message.Attributes?.ApproximateReceiveCount ?? '1');
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
