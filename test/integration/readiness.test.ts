import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { ReadinessProbe } from '../../src/infrastructure/observability/readiness-probe';
import { prepareTestDatabase } from '../support/environment';
import { startTestApp, TestApp } from '../support/app';
import { createTestQueues } from '../support/sqs';

describe('health checks (AC-API-08)', () => {
  let queues: Awaited<ReturnType<typeof createTestQueues>>;
  let healthy: TestApp;
  let sqsDown: TestApp;

  beforeAll(async () => {
    await prepareTestDatabase();
    queues = await createTestQueues();
    healthy = await startTestApp(queues);
    sqsDown = await startTestApp(queues, { AWS_ENDPOINT_URL: 'http://127.0.0.1:1' });
  });

  afterAll(async () => {
    await healthy.close();
    await sqsDown.close();
    await queues.cleanup();
  });

  test('com Postgres e SQS alcançáveis: live e ready respondem 200', async () => {
    expect((await healthy.api.request('GET', '/health/live')).status).toBe(200);
    const ready = await healthy.api.request('GET', '/health/ready');
    expect(ready.status).toBe(200);
    expect(ready.body).toEqual({ status: 'ok', checks: { postgres: 'up', sqs: 'up' } });
  });

  test('com o SQS inalcançável: ready responde 503 apontando a dependência, e live continua 200', async () => {
    const ready = await sqsDown.api.request('GET', '/health/ready');
    expect(ready.status).toBe(503);
    expect(ready.body).toEqual({ status: 'unavailable', checks: { postgres: 'up', sqs: 'down' } });
    expect((await sqsDown.api.request('GET', '/health/live')).status).toBe(200);
  });

  test('em shutdown, ready passa a responder 503 "draining" para o balanceador parar de enviar tráfego', async () => {
    const probe = healthy.app.get(ReadinessProbe);
    probe.beforeApplicationShutdown();
    const ready = await healthy.api.request('GET', '/health/ready');
    expect(ready.status).toBe(503);
    expect(ready.body).toMatchObject({ status: 'draining' });
  });

  test('health e métricas não exigem autenticação (rotas públicas)', async () => {
    const response = await fetch(`${sqsDown.api.baseUrl}/metrics`);
    expect(response.status).toBe(200);
  });
});
