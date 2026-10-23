import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { Message } from '@aws-sdk/client-sqs';
import { DeliveryAttempts } from '../../../src/application/ports/system';
import { SqsQueues } from '../../../src/infrastructure/messaging/sqs';
import { MessageDecision, WagerMessageHandler } from '../../../src/interfaces/sqs/wager-message.handler';
import {
  WagerConsumerOptions,
  WagerTransactionsConsumer,
} from '../../../src/interfaces/sqs/wager-transactions.consumer';
import { deferred, eventually } from '../../support/eventually';
import { FixedClock, RecordingLogger, RecordingMetrics } from '../../support/in-memory';

interface SentCommand {
  type: string;
  input: Record<string, unknown>;
}

class ScriptedSqsClient {
  readonly sent: SentCommand[] = [];
  readonly receives: Array<() => Promise<{ Messages?: Message[] }>> = [];
  readonly failures = new Map<string, Error>();

  async send(
    command: { constructor: { name: string }; input: Record<string, unknown> },
    options?: { abortSignal?: AbortSignal },
  ) {
    const type = command.constructor.name;
    this.sent.push({ type, input: command.input });
    const failure = this.failures.get(type);
    if (failure) {
      throw failure;
    }
    if (type === 'GetQueueUrlCommand') {
      return { QueueUrl: `http://fake-sqs/${String(command.input.QueueName)}` };
    }
    if (type === 'ReceiveMessageCommand') {
      const next = this.receives.shift();
      if (next) return next();
      await new Promise((resolve) => setTimeout(resolve, 3));
      if (options?.abortSignal?.aborted) throw new Error('aborted');
      return {};
    }
    return {};
  }

  commands(type: string): SentCommand[] {
    return this.sent.filter((command) => command.type === type);
  }

  visibilityChanges(): Array<{ handle: unknown; seconds: unknown }> {
    return this.commands('ChangeMessageVisibilityCommand').map((command) => ({
      handle: command.input.ReceiptHandle,
      seconds: command.input.VisibilityTimeout,
    }));
  }
}

class ScriptedAttempts implements DeliveryAttempts {
  readonly cleared: string[] = [];
  next: number | Error = 1;

  async recordFailure(): Promise<number> {
    if (this.next instanceof Error) throw this.next;
    return this.next;
  }

  async clear(_consumer: string, messageId: string): Promise<void> {
    this.cleared.push(messageId);
  }
}

const context = { messageId: 'msg-1', walletId: 'w1', providerId: 'p' };
const ack = (committed = true): MessageDecision => ({ action: 'ack', committed, context });
const retry = (reason = 'TRANSIENT_FAILURE'): MessageDecision => ({ action: 'retry', reason, context });

function message(id: string, options: { group?: string; receiveCount?: string } = {}): Message {
  return {
    MessageId: id,
    ReceiptHandle: `handle-${id}`,
    Body: JSON.stringify({ id }),
    Attributes: { MessageGroupId: options.group ?? 'group-1', ApproximateReceiveCount: options.receiveCount ?? '1' },
  };
}

describe('WagerTransactionsConsumer (cliente SQS roteirizado)', () => {
  let sqs: ScriptedSqsClient;
  let attempts: ScriptedAttempts;
  let logger: RecordingLogger;
  let metrics: RecordingMetrics;
  let handled: string[];
  let decisions: Array<MessageDecision | Promise<MessageDecision>>;
  let consumer: WagerTransactionsConsumer;

  const options: WagerConsumerOptions = {
    consumerName: 'consumer',
    queueName: 'main.fifo',
    dlqName: 'dlq.fifo',
    waitTimeSeconds: 0,
    visibilityTimeoutSeconds: 30,
    batchSize: 10,
    maxAttempts: 5,
    retryBaseSeconds: 1,
    retryMaxSeconds: 60,
    shutdownGraceMs: 50,
    faultInjection: 'none',
    pollFailureBackoffMs: 2,
    heartbeatIntervalMs: 10,
  };

  function build(overrides: Partial<WagerConsumerOptions> = {}): WagerTransactionsConsumer {
    const handler = {
      handle: async (body: string | undefined) => {
        handled.push(body ?? '');
        return decisions.shift() ?? ack();
      },
    } as unknown as WagerMessageHandler;
    return new WagerTransactionsConsumer(
      handler,
      new SqsQueues(sqs as never),
      { ...options, ...overrides },
      attempts,
      new FixedClock(),
      metrics,
      logger,
    );
  }

  function deliver(...messages: Message[]): void {
    sqs.receives.push(async () => ({ Messages: messages }));
  }

  beforeEach(() => {
    sqs = new ScriptedSqsClient();
    attempts = new ScriptedAttempts();
    logger = new RecordingLogger();
    metrics = new RecordingMetrics();
    handled = [];
    decisions = [];
    consumer = build();
  });

  afterEach(() => consumer.stop());

  test('ack só depois de processar, e a mensagem é apagada da fila principal', async () => {
    deliver(message('a'));
    consumer.start();
    await eventually(() => sqs.commands('DeleteMessageCommand').length === 1, { description: 'message acked' });
    expect(sqs.commands('DeleteMessageCommand')[0]!.input).toMatchObject({
      QueueUrl: 'http://fake-sqs/main.fifo',
      ReceiptHandle: 'handle-a',
    });
    expect(attempts.cleared).toEqual(['msg-1']);
  });

  test('falha ao buscar mensagens não derruba o consumer: registra aviso e volta a buscar', async () => {
    sqs.receives.push(async () => {
      throw new Error('network down');
    });
    deliver(message('a'));
    consumer.start();
    await eventually(() => sqs.commands('DeleteMessageCommand').length === 1, {
      description: 'recovered after poll failure',
    });
    expect(logger.has('warn', 'sqs poll failed; backing off')).toBe(true);
  });

  test('retry: registra a tentativa real, usa backoff exponencial e segura as irmãs do grupo para manter a ordem', async () => {
    decisions.push(retry());
    attempts.next = 3;
    deliver(message('a'), message('b'));
    consumer.start();
    await eventually(() => sqs.visibilityChanges().length >= 2, { description: 'visibility changes' });

    expect(handled).toHaveLength(1);
    expect(sqs.visibilityChanges()).toEqual(
      expect.arrayContaining([
        { handle: 'handle-a', seconds: 4 },
        { handle: 'handle-b', seconds: 1 },
      ]),
    );
    expect(metrics.count('retry:sqs_consumer')).toBe(1);
    expect(sqs.commands('DeleteMessageCommand')).toHaveLength(0);
  });

  test('ao atingir o limite de tentativas reais, copia para a DLQ com o motivo, apaga e zera o contador', async () => {
    decisions.push(retry('LOCK_TIMEOUT'));
    attempts.next = 5;
    deliver(message('a'));
    consumer.start();
    await eventually(() => sqs.commands('SendMessageCommand').length === 1, { description: 'sent to DLQ' });

    const sent = sqs.commands('SendMessageCommand')[0]!.input as {
      QueueUrl: string;
      MessageAttributes: { deadLetterReason: { StringValue: string } };
    };
    expect(sent.QueueUrl).toBe('http://fake-sqs/dlq.fifo');
    expect(sent.MessageAttributes.deadLetterReason.StringValue).toBe('MAX_ATTEMPTS_EXCEEDED after 5: LOCK_TIMEOUT');
    await eventually(() => sqs.commands('DeleteMessageCommand').length === 1);
    expect(attempts.cleared).toEqual(['msg-1']);
    expect(metrics.count('dlq:max_attempts')).toBe(1);
  });

  test('se o contador de tentativas estiver indisponível, usa o ApproximateReceiveCount e segue com o retry', async () => {
    decisions.push(retry());
    attempts.next = new Error('db down');
    deliver(message('a', { receiveCount: '3' }));
    consumer.start();
    await eventually(() => sqs.visibilityChanges().length === 1, { description: 'retry scheduled' });

    expect(sqs.visibilityChanges()[0]).toEqual({ handle: 'handle-a', seconds: 4 });
    expect(logger.has('warn', 'could not record delivery attempt; relying on the redrive policy')).toBe(true);
  });

  test('decisão permanente vai direto para a DLQ com o motivo truncado', async () => {
    decisions.push({ action: 'dead-letter', reason: 'x'.repeat(2_000), context });
    deliver(message('a'));
    consumer.start();
    await eventually(() => sqs.commands('SendMessageCommand').length === 1, { description: 'sent to DLQ' });

    const reason = (
      sqs.commands('SendMessageCommand')[0]!.input as {
        MessageAttributes: { deadLetterReason: { StringValue: string } };
      }
    ).MessageAttributes.deadLetterReason.StringValue;
    expect(reason).toHaveLength(1_000);
    expect(metrics.count('dlq:permanent')).toBe(1);
  });

  test('falha ao apagar (ack) não derruba o grupo: a próxima mensagem é processada', async () => {
    sqs.failures.set('DeleteMessageCommand', new Error('expired receipt handle'));
    deliver(message('a'), message('b'));
    consumer.start();
    await eventually(() => handled.length === 2, { description: 'both messages handled' });
    expect(logger.has('warn', 'sqs ack failed; message will be redelivered and deduplicated')).toBe(true);
  });

  test('mensagens de grupos diferentes são processadas em paralelo', async () => {
    const gate = deferred<MessageDecision>();
    decisions.push(gate.promise);
    deliver(message('a', { group: 'wallet-1' }), message('b', { group: 'wallet-2' }));
    consumer.start();
    await eventually(() => handled.length === 2, { description: 'second group did not wait for the first' });
    gate.resolve(ack());
  });

  test('o heartbeat estende só as mensagens em andamento, não as que já terminaram', async () => {
    const slow = deferred<MessageDecision>();
    decisions.push(ack(), slow.promise);
    deliver(message('done', { group: 'wallet-1' }), message('slow', { group: 'wallet-2' }));
    consumer.start();

    await eventually(() => sqs.commands('DeleteMessageCommand').length === 1, { description: 'first message acked' });
    await eventually(() => sqs.visibilityChanges().some((change) => change.handle === 'handle-slow'), {
      description: 'heartbeat extended the slow message',
    });
    const extendedBefore = sqs.visibilityChanges().filter((change) => change.handle === 'handle-slow').length;
    await new Promise((resolve) => setTimeout(resolve, 60));

    const changes = sqs.visibilityChanges();
    expect(changes.filter((change) => change.handle === 'handle-slow').length).toBeGreaterThan(extendedBefore);
    expect(changes.filter((change) => change.handle === 'handle-done')).toEqual([]);
    slow.resolve(ack());
  });

  test('stop() com grace estourado devolve as mensagens em andamento e não trava', async () => {
    const never = deferred<MessageDecision>();
    decisions.push(never.promise);
    deliver(message('a'));
    consumer.start();
    await eventually(() => handled.length === 1);

    await consumer.stop();

    expect(sqs.visibilityChanges()).toContainEqual({ handle: 'handle-a', seconds: 0 });
    expect(logger.entries.find((entry) => entry.message === 'sqs consumer stopped')?.fields).toMatchObject({
      drained: false,
    });
    never.resolve(ack());
  });

  test('mensagens recebidas depois do stop são devolvidas sem processar', async () => {
    const late = deferred<{ Messages: Message[] }>();
    sqs.receives.push(() => late.promise);
    consumer.start();
    await eventually(() => sqs.commands('ReceiveMessageCommand').length === 1);

    const stopping = consumer.stop();
    late.resolve({ Messages: [message('a')] });
    await stopping;

    expect(handled).toHaveLength(0);
    expect(sqs.visibilityChanges()).toContainEqual({ handle: 'handle-a', seconds: 0 });
  });

  test('start é idempotente e stop sem start é inofensivo', async () => {
    await consumer.stop();
    consumer.start();
    consumer.start();
    await consumer.stop();
    expect(logger.entries.filter((entry) => entry.message === 'sqs consumer started')).toHaveLength(1);
  });
});
