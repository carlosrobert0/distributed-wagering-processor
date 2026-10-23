import { describe, expect, test } from 'bun:test';
import {
  createPinoLogger,
  maskAmounts,
  PinoAppLogger,
  PinoNestLogger,
} from '../../../src/infrastructure/observability/pino-logger';
import { RequestContextStore } from '../../../src/infrastructure/observability/request-context';

function capture(level = 'debug') {
  const lines: Array<Record<string, unknown>> = [];
  const pino = createPinoLogger({
    level,
    instanceId: 'instance-1',
    destination: { write: (line: string) => lines.push(JSON.parse(line)) },
  });
  return { lines, pino, logger: new PinoAppLogger(pino), nest: new PinoNestLogger(pino) };
}

describe('logs estruturados (AC-OBS-01/02)', () => {
  test('JSON com serviço, instância, nível textual, hora ISO e a chave "message"', () => {
    const { lines, logger } = capture();
    logger.info('hello', { walletId: 'w1' });

    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({
      level: 'info',
      service: 'wagering-processor',
      instance: 'instance-1',
      message: 'hello',
      walletId: 'w1',
    });
    expect(new Date(String(lines[0]!.time)).toISOString()).toBe(lines[0]!.time as string);
  });

  test('correlationId e messageId entram sozinhos a partir do contexto da requisição/mensagem', () => {
    const { lines, logger } = capture();
    RequestContextStore.run({ correlationId: 'corr-1', messageId: 'msg-1' }, () => logger.info('inside'));
    logger.info('outside');

    expect(lines[0]).toMatchObject({ correlationId: 'corr-1', messageId: 'msg-1' });
    expect(lines[1]).not.toHaveProperty('correlationId');
  });

  test('campos indefinidos são descartados', () => {
    const { lines, logger } = capture();
    logger.info('x', { walletId: 'w1', providerId: undefined });
    expect(lines[0]).not.toHaveProperty('providerId');
  });

  test('erros são serializados com tipo, mensagem, código e stack', () => {
    const { lines, logger } = capture();
    logger.error('boom', { transactionId: 't1' }, Object.assign(new RangeError('bad range'), { code: 'E_RANGE' }));
    logger.warn('careful', {}, 'plain string error');

    expect(lines[0]!.err).toMatchObject({ type: 'RangeError', message: 'bad range', code: 'E_RANGE' });
    expect(String((lines[0]!.err as { stack: string }).stack)).toContain('RangeError');
    expect(lines[1]!.err).toEqual({ message: 'plain string error' });
  });

  test('mensagens de erro não vazam valores monetários (ex.: linha reprovada por um CHECK)', () => {
    const { lines, logger } = capture();
    logger.error(
      'integrity',
      {},
      Object.assign(new Error('wallet w1 (balance 101.00, version 3) diverges from its ledger (balance 100.00)'), {
        detail: 'Failing row contains (0192f291, BET, 25.00, BRL).',
      }),
    );

    const serialized = JSON.stringify(lines[0]);
    expect(serialized).not.toMatch(/101\.00|100\.00|25\.00/);
    expect(serialized).toContain('[AMOUNT]');
    expect(serialized).toContain('version 3');
  });

  test('maskAmounts preserva timestamps, versões de pacote e identificadores', () => {
    expect(maskAmounts('at 2026-10-23T11:28:02.314Z pg@8.23.1 id 01a16884-970a 1.5 and -3.00, 7.25')).toBe(
      'at 2026-10-23T11:28:02.314Z pg@8.23.1 id 01a16884-970a 1.5 and [AMOUNT], [AMOUNT]',
    );
  });

  test('segunda barreira: campos financeiros e credenciais são censurados mesmo se alguém os passar', () => {
    const { lines, pino } = capture();
    pino.info(
      {
        money: { amount: '25.00', currency: 'BRL' },
        balance: '975.00',
        balanceAfter: '950.00',
        balanceBefore: '975.00',
        amount: '25.00',
        payload: { secret: true },
        request: { amount: '1.00', payload: 'x' },
        authorization: 'Bearer abc',
      },
      'leak attempt',
    );

    const serialized = JSON.stringify(lines[0]);
    expect(serialized).not.toMatch(/25\.00|975\.00|950\.00|Bearer|secret/);
    expect(lines[0]).toMatchObject({ money: '[REDACTED]', balance: '[REDACTED]', authorization: '[REDACTED]' });
  });

  test('respeita o nível configurado', () => {
    const { lines, logger } = capture('warn');
    logger.debug('d');
    logger.info('i');
    logger.warn('w');
    expect(lines.map((line) => line.message)).toEqual(['w']);
  });

  test('adaptador do Nest mantém o formato JSON e o contexto', () => {
    const { lines, nest } = capture();
    nest.log('started', 'NestFactory');
    nest.warn('careful', 'Ctx');
    nest.error('failed', 'trace-here', 'Ctx');
    nest.debug('d', 'Ctx');

    expect(lines.map((line) => [line.level, line.message, line.context])).toEqual([
      ['info', 'started', 'NestFactory'],
      ['warn', 'careful', 'Ctx'],
      ['error', 'failed', 'Ctx'],
      ['debug', 'd', 'Ctx'],
    ]);
  });
});
