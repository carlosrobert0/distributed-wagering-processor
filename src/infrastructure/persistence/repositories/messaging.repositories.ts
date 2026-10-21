import { LockMode } from '@mikro-orm/core';
import { EntityManager } from '@mikro-orm/postgresql';
import { InboxRepository, OutboxRepository, OutboxStats } from '../../../application/ports/repositories';
import { InboxMessage } from '../../../domain/messaging/inbox-message';
import { OutboxMessage } from '../../../domain/messaging/outbox-message';
import { InboxMessageMapper, OutboxMessageMapper } from '../mappers';
import { InboxMessageRecord, OutboxMessageRecord } from '../records';

export class MikroOrmInboxRepository implements InboxRepository {
  constructor(private readonly em: EntityManager) {}

  async tryReceive(message: InboxMessage): Promise<boolean> {
    // ON CONFLICT DO NOTHING: se outra instância está inserindo a mesma mensagem, esta espera o
    // commit dela e então não insere nada — nunca há dois efeitos para o mesmo messageId.
    const rows = await this.em.execute<{ message_id: string }[]>(
      `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (consumer_name, message_id) DO NOTHING
       RETURNING message_id`,
      [
        message.consumerName,
        message.messageId,
        message.payloadHash,
        // ISO-8601 com fuso explícito: o parâmetro é interpolado como texto em timestamptz.
        message.receivedAt.toISOString(),
        message.processedAt?.toISOString() ?? null,
      ],
    );
    return rows.length === 1;
  }

  async find(consumerName: string, messageId: string): Promise<InboxMessage | null> {
    const record = await this.em.findOne(InboxMessageRecord, { consumerName, messageId }, { disableIdentityMap: true });
    return record ? InboxMessageMapper.toDomain(record) : null;
  }
}

interface OutboxStatsRow {
  pending: number;
  oldest: Date | null;
}

export class MikroOrmOutboxRepository implements OutboxRepository {
  constructor(private readonly em: EntityManager) {}

  async enqueue(messages: readonly OutboxMessage[]): Promise<void> {
    if (messages.length === 0) return;
    await this.em.insertMany(
      OutboxMessageRecord,
      messages.map((message) => OutboxMessageMapper.toRecord(message)),
    );
  }

  async claimDue(now: Date, limit: number): Promise<OutboxMessage[]> {
    // FOR UPDATE SKIP LOCKED: publishers concorrentes reivindicam lotes disjuntos (AC-OUT-02).
    const records = await this.em.find(
      OutboxMessageRecord,
      { publishedAt: null, nextAttemptAt: { $lte: now } },
      {
        disableIdentityMap: true,
        orderBy: { occurredAt: 'asc', id: 'asc' },
        limit,
        lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE,
      },
    );
    return records.map((record) => OutboxMessageMapper.toDomain(record));
  }

  async save(message: OutboxMessage): Promise<void> {
    await this.em.nativeUpdate(
      OutboxMessageRecord,
      { id: message.id },
      {
        attempts: message.attempts,
        nextAttemptAt: message.nextAttemptAt ?? null,
        publishedAt: message.publishedAt ?? null,
        lastError: message.lastError ?? null,
      },
    );
  }

  async stats(): Promise<OutboxStats> {
    const [row] = await this.em.execute<OutboxStatsRow[]>(
      `SELECT COUNT(*)::int AS pending, MIN(occurred_at) AS oldest FROM outbox_messages WHERE published_at IS NULL`,
    );
    return {
      pending: Number(row?.pending ?? 0),
      ...(row?.oldest ? { oldestPendingOccurredAt: new Date(row.oldest) } : {}),
    };
  }
}
