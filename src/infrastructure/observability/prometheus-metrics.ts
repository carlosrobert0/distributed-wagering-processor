import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';
import { Metrics, ProcessingSource } from '../../application/ports/system';

export class PrometheusMetrics implements Metrics {
  readonly registry = new Registry();

  private readonly transactions = new Counter({
    name: 'wagering_transactions_total',
    help: 'Transações liquidadas, por status final, kind e origem',
    labelNames: ['status', 'kind', 'source'] as const,
    registers: [this.registry],
  });

  private readonly duplicates = new Counter({
    name: 'wagering_duplicates_total',
    help: 'Duplicatas detectadas (replay idempotente, conflito de key, mensagem repetida no inbox)',
    labelNames: ['type'] as const,
    registers: [this.registry],
  });

  private readonly retries = new Counter({
    name: 'wagering_retries_total',
    help: 'Novas tentativas agendadas, por componente',
    labelNames: ['component'] as const,
    registers: [this.registry],
  });

  private readonly deadLetters = new Counter({
    name: 'wagering_dlq_messages_total',
    help: 'Mensagens enviadas para a DLQ',
    labelNames: ['reason'] as const,
    registers: [this.registry],
  });

  private readonly lockConflicts = new Counter({
    name: 'wagering_lock_conflicts_total',
    help: 'Conflitos de concorrência: lock timeout, deadlock, corrida de unicidade',
    labelNames: ['kind'] as const,
    registers: [this.registry],
  });

  private readonly processingLatency = new Histogram({
    name: 'wagering_processing_duration_seconds',
    help: 'Latência de processamento de uma submissão (inclui espera de lock)',
    labelNames: ['source'] as const,
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    registers: [this.registry],
  });

  private readonly lockWait = new Histogram({
    name: 'wagering_wallet_lock_wait_seconds',
    help: 'Tempo esperando o lock FOR UPDATE da wallet (contenção de hot wallet)',
    buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [this.registry],
  });

  private readonly outboxPublishedTotal = new Counter({
    name: 'wagering_outbox_published_total',
    help: 'Eventos publicados pelo relay da outbox',
    registers: [this.registry],
  });

  private readonly outboxPending = new Gauge({
    name: 'wagering_outbox_pending',
    help: 'Eventos aguardando publicação',
    registers: [this.registry],
  });

  private readonly outboxLag = new Gauge({
    name: 'wagering_outbox_lag_seconds',
    help: 'Idade do evento pendente mais antigo',
    registers: [this.registry],
  });

  private readonly reconciliations = new Counter({
    name: 'wagering_reconciliations_total',
    help: 'Reconciliações executadas, por resultado',
    labelNames: ['result'] as const,
    registers: [this.registry],
  });

  constructor(options: { collectDefaults?: boolean; instanceId?: string } = {}) {
    if (options.instanceId) {
      this.registry.setDefaultLabels({ instance_id: options.instanceId });
    }
    if (options.collectDefaults ?? true) {
      collectDefaultMetrics({ register: this.registry });
    }
  }

  transactionSettled(status: string, kind: string, source: ProcessingSource): void {
    this.transactions.inc({ status, kind, source });
  }

  duplicateDetected(type: 'idempotent_replay' | 'idempotency_conflict' | 'inbox_duplicate'): void {
    this.duplicates.inc({ type });
  }

  retry(component: 'sqs_consumer' | 'outbox_relay' | 'pending_reference' | 'race'): void {
    this.retries.inc({ component });
  }

  deadLettered(reason: 'permanent' | 'max_attempts'): void {
    this.deadLetters.inc({ reason });
  }

  lockConflict(kind: 'lock_timeout' | 'deadlock' | 'unique_race'): void {
    this.lockConflicts.inc({ kind });
  }

  observeProcessingLatency(source: ProcessingSource, seconds: number): void {
    this.processingLatency.observe({ source }, seconds);
  }

  observeLockWait(seconds: number): void {
    this.lockWait.observe(seconds);
  }

  outboxPublished(count: number): void {
    this.outboxPublishedTotal.inc(count);
  }

  setOutboxBacklog(pending: number, lagSeconds: number): void {
    this.outboxPending.set(pending);
    this.outboxLag.set(lagSeconds);
  }

  reconciliationChecked(consistent: boolean): void {
    this.reconciliations.inc({ result: consistent ? 'consistent' : 'mismatch' });
  }
}
