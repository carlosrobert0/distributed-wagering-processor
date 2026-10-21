import { describe, expect, mock, test } from 'bun:test';
import { BadRequestException, NotFoundException } from '@nestjs/common';
import { DeadlockException, LockWaitTimeoutException } from '@mikro-orm/core';
import {
  ConcurrencyViolationError,
  ExternalTransactionConflictError,
  IdempotencyKeyConflictError,
  InvalidPayloadError,
  TransactionNotFoundError,
  TransientInfrastructureError,
  WalletAlreadyExistsError,
  WalletNotFoundError,
} from '../../../src/application/errors';
import { InvalidMoneyError } from '../../../src/domain/shared/domain-error';
import { WagerTransactionStatus } from '../../../src/domain/wagering/wager-transaction';
import { loadConfig } from '../../../src/infrastructure/config/app-config';
import { toTransientError } from '../../../src/infrastructure/persistence/database-errors';
import { toErrorResponse } from '../../../src/interfaces/http/http-exception.filter';
import { bodyParserErrorHandler } from '../../../src/interfaces/http/json-body.middleware';
import { httpStatusFor } from '../../../src/interfaces/http/wagering.controller';
import { classify } from '../../../src/interfaces/sqs/wager-message.handler';

describe('HTTP: erro → status (consistente em todos os endpoints)', () => {
  test.each([
    [new InvalidPayloadError('bad', ['x: y']), 400, 'INVALID_PAYLOAD'],
    [new InvalidPayloadError('no key', undefined, 'IDEMPOTENCY_KEY_REQUIRED'), 400, 'IDEMPOTENCY_KEY_REQUIRED'],
    [new InvalidMoneyError('bad amount'), 400, 'INVALID_PAYLOAD'],
    [new WalletNotFoundError('w'), 404, 'WALLET_NOT_FOUND'],
    [new TransactionNotFoundError('t'), 404, 'TRANSACTION_NOT_FOUND'],
    [new IdempotencyKeyConflictError('k'), 409, 'IDEMPOTENCY_KEY_CONFLICT'],
    [new ExternalTransactionConflictError('p', 'e'), 409, 'EXTERNAL_TRANSACTION_CONFLICT'],
    [new WalletAlreadyExistsError('p', 'BRL'), 409, 'WALLET_ALREADY_EXISTS'],
    [new TransientInfrastructureError('down', 'connection'), 503, 'TRANSIENT_FAILURE'],
    [new BadRequestException('bad json'), 400, 'INVALID_PAYLOAD'],
    [new NotFoundException(), 404, 'NOT_FOUND'],
    [new ConcurrencyViolationError('w', 1), 500, 'CONCURRENCY_VIOLATION'],
    [new Error('boom'), 500, 'INTERNAL_ERROR'],
  ])('%p → %i %s', (exception, status, error) => {
    const response = toErrorResponse(exception);
    expect(response.status).toBe(status);
    expect(response.body.error).toBe(error);
  });

  test('erro interno não vaza a mensagem original', () => {
    expect(toErrorResponse(new Error('secret detail')).body.message).toBe('Internal error');
  });

  test.each([
    [WagerTransactionStatus.Processed, false, 201],
    [WagerTransactionStatus.Processed, true, 200],
    [WagerTransactionStatus.PendingReference, false, 202],
    [WagerTransactionStatus.PendingReference, true, 202],
    [WagerTransactionStatus.Rejected, false, 422],
    [WagerTransactionStatus.Rejected, true, 422],
    [WagerTransactionStatus.Failed, true, 422],
  ])('resultado %s (replay=%p) → %i', (status, idempotentReplay, expected) => {
    expect(httpStatusFor({ transactionId: 't', status, idempotentReplay })).toBe(expected);
  });

  test('JSON malformado vira 400 INVALID_PAYLOAD; outros erros seguem adiante', () => {
    const json = mock(() => undefined);
    const response = { status: mock(() => ({ json })) };
    const next = mock(() => undefined);

    bodyParserErrorHandler({ type: 'entity.parse.failed', status: 400 }, {} as never, response as never, next);
    expect(response.status).toHaveBeenCalledWith(400);
    expect(json).toHaveBeenCalledWith({ error: 'INVALID_PAYLOAD', message: 'Malformed JSON body' });

    const other = new Error('other');
    bodyParserErrorHandler(other, {} as never, response as never, next);
    expect(next).toHaveBeenCalledWith(other);
  });
});

describe('SQS: classificação de erros', () => {
  test.each([
    [new InvalidPayloadError('bad'), 'permanent'],
    [new IdempotencyKeyConflictError('k'), 'business'],
    [new WalletNotFoundError('w'), 'business'],
    [new TransientInfrastructureError('down', 'lock_timeout'), 'transient'],
    [new Error('unknown'), 'transient'],
  ])('%p → %s', (error, expected) => {
    expect<string>(classify(error)).toBe(expected);
  });
});

describe('Postgres: erro → transitório', () => {
  test.each([
    ['55P03', 'lock_timeout'],
    ['57014', 'lock_timeout'],
    ['40001', 'deadlock'],
    ['40P01', 'deadlock'],
    ['57P01', 'connection'],
    ['53300', 'connection'],
    ['08006', 'connection'],
    ['ECONNREFUSED', 'connection'],
  ])('código %s → %s', (code, reason) => {
    expect<string | undefined>(toTransientError(Object.assign(new Error('x'), { code }))?.reason).toBe(reason);
  });

  test('exceções do MikroORM', () => {
    expect(toTransientError(new LockWaitTimeoutException(new Error('lock')))?.reason).toBe('lock_timeout');
    expect(toTransientError(new DeadlockException(new Error('deadlock')))?.reason).toBe('deadlock');
  });

  test('violação de constraint não é transitória', () => {
    expect(toTransientError(Object.assign(new Error('unique'), { code: '23505' }))).toBeUndefined();
  });
});

describe('configuração', () => {
  test('defaults válidos', () => {
    const config = loadConfig({});
    expect(config.database.lockTimeoutMs).toBe(5_000);
    expect(config.queues).toEqual({
      wagerTransactions: 'wager-transactions.fifo',
      wagerTransactionsDlq: 'wager-transactions-dlq.fifo',
      events: 'wagering-events.fifo',
    });
    expect(config.consumer.enabled).toBe(true);
    expect(config.faultInjection).toBe('none');
  });

  test('flags booleanas e números vindos do ambiente', () => {
    const config = loadConfig({ CONSUMER_ENABLED: 'false', PORT: '8080', DB_LOCK_TIMEOUT_MS: '250' });
    expect(config.consumer.enabled).toBe(false);
    expect(config.port).toBe(8080);
    expect(config.database.lockTimeoutMs).toBe(250);
  });

  test('valor inválido falha no boot com mensagem clara', () => {
    expect(() => loadConfig({ PORT: 'abc' })).toThrow(/Invalid configuration: PORT/);
    expect(() => loadConfig({ FAULT_INJECTION: 'explode' })).toThrow(/FAULT_INJECTION/);
  });
});
