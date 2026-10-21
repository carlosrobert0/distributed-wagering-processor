/**
 * Teste de carga (diferencial opcional, §14): `bun run test:load`.
 *
 * Pré-requisito: `docker compose up -d --build` (3 instâncias em :3001/:3002/:3003).
 *
 * Metodologia:
 *  - cria W wallets com saldo alto; dispara N apostas de 1.00 com concorrência C, distribuídas
 *    em round-robin entre as instâncias; uma fração das wallets é "quente" (recebe 50% do tráfego);
 *  - D% das requisições são reenviadas (duplicatas) para medir o caminho de replay;
 *  - mede latência por requisição (p50/p95/p99), throughput, taxa de erro, 503 (lock timeout),
 *    e lê do /metrics o lag da outbox e os conflitos de lock;
 *  - ao final confere saldo == ledger via /reconciliation em todas as wallets.
 *
 * Variáveis: LOAD_TARGETS, LOAD_WALLETS, LOAD_REQUESTS, LOAD_CONCURRENCY, LOAD_DUPLICATE_RATIO.
 */
import { randomUUID } from 'node:crypto';

const targets = (process.env.LOAD_TARGETS ?? 'http://localhost:3001,http://localhost:3002,http://localhost:3003').split(',');
const walletCount = Number(process.env.LOAD_WALLETS ?? 50);
const totalRequests = Number(process.env.LOAD_REQUESTS ?? 5_000);
const concurrency = Number(process.env.LOAD_CONCURRENCY ?? 64);
const duplicateRatio = Number(process.env.LOAD_DUPLICATE_RATIO ?? 0.1);
const hotWallets = Math.max(1, Math.floor(walletCount * 0.05));

interface Wallet {
  id: string;
  playerId: string;
}

const target = (i: number) => targets[i % targets.length]!;

async function createWallet(i: number): Promise<Wallet> {
  const response = await fetch(`${target(i)}/wallets`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ playerId: randomUUID(), initialBalance: { amount: '1000000.00', currency: 'BRL' } }),
  });
  if (response.status !== 201) throw new Error(`wallet creation failed: ${response.status}`);
  return (await response.json()) as Wallet;
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  return sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)]!;
}

async function scrape(url: string, metric: string): Promise<number> {
  const text = await fetch(`${url}/metrics`).then((r) => r.text());
  return text
    .split('\n')
    .filter((line) => line.startsWith(metric))
    .reduce((sum, line) => sum + Number(line.split(' ').at(-1)), 0);
}

async function main(): Promise<void> {
  console.log(`targets=${targets.join(',')} wallets=${walletCount} requests=${totalRequests} concurrency=${concurrency}`);
  const wallets = await Promise.all(Array.from({ length: walletCount }, (_, i) => createWallet(i)));

  const latencies: number[] = [];
  const statuses = new Map<number, number>();
  let errors = 0;
  let next = 0;

  const pickWallet = (): Wallet =>
    Math.random() < 0.5 ? wallets[Math.floor(Math.random() * hotWallets)]! : wallets[Math.floor(Math.random() * walletCount)]!;

  async function worker(): Promise<void> {
    while (next < totalRequests) {
      const i = next++;
      const wallet = pickWallet();
      const externalTransactionId = `load-${randomUUID()}`;
      const body = JSON.stringify({
        providerId: 'load-provider',
        externalTransactionId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round',
        gameId: 'game',
        kind: 'BET',
        money: { amount: '1.00', currency: 'BRL' },
      });
      const send = async (instance: number) => {
        const started = performance.now();
        try {
          const response = await fetch(`${target(instance)}/wagering/transactions`, {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'idempotency-key': `load-provider:${externalTransactionId}` },
            body,
          });
          await response.arrayBuffer();
          statuses.set(response.status, (statuses.get(response.status) ?? 0) + 1);
        } catch {
          errors += 1;
        } finally {
          latencies.push(performance.now() - started);
        }
      };
      await send(i);
      if (Math.random() < duplicateRatio) await send(i + 1);
    }
  }

  const started = performance.now();
  await Promise.all(Array.from({ length: concurrency }, worker));
  const elapsedSeconds = (performance.now() - started) / 1000;

  latencies.sort((a, b) => a - b);
  const requests = latencies.length;
  const failures = errors + [...statuses.entries()].filter(([s]) => s >= 500).reduce((n, [, c]) => n + c, 0);

  const outboxLag = Math.max(...(await Promise.all(targets.map((t) => scrape(t, 'wagering_outbox_lag_seconds')))));
  const lockConflicts = (await Promise.all(targets.map((t) => scrape(t, 'wagering_lock_conflicts_total')))).reduce((a, b) => a + b, 0);

  let inconsistent = 0;
  for (const wallet of wallets) {
    const reconciliation = (await fetch(`${targets[0]}/wallets/${wallet.id}/reconciliation`, { method: 'POST' }).then((r) =>
      r.json(),
    )) as { consistent: boolean };
    if (!reconciliation.consistent) inconsistent += 1;
  }

  console.table({
    requests,
    'throughput (req/s)': Number((requests / elapsedSeconds).toFixed(1)),
    'p50 (ms)': Number(percentile(latencies, 50).toFixed(1)),
    'p95 (ms)': Number(percentile(latencies, 95).toFixed(1)),
    'p99 (ms)': Number(percentile(latencies, 99).toFixed(1)),
    'error rate (%)': Number(((failures / requests) * 100).toFixed(2)),
    'lock conflicts': lockConflicts,
    'outbox lag at end (s)': outboxLag,
    'wallets inconsistent': inconsistent,
  });
  console.log('status codes:', Object.fromEntries(statuses));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
