import { MikroORM } from '@mikro-orm/postgresql';
import { DeliveryAttempts } from '../../../application/ports/system';

const MAX_ERROR_LENGTH = 500;

const RECORD_FAILURE_SQL = `
  INSERT INTO consumer_message_attempts (consumer_name, message_id, attempts, last_error, updated_at)
  VALUES (?, ?, 1, ?, ?)
  ON CONFLICT (consumer_name, message_id) DO UPDATE
     SET attempts = consumer_message_attempts.attempts + 1,
         last_error = EXCLUDED.last_error,
         updated_at = EXCLUDED.updated_at
  RETURNING attempts`;

export class MikroOrmDeliveryAttempts implements DeliveryAttempts {
  constructor(private readonly orm: MikroORM) {}

  async recordFailure(consumerName: string, messageId: string, reason: string, at: Date): Promise<number> {
    const [row] = await this.orm.em
      .fork()
      .execute<{ attempts: number }[]>(RECORD_FAILURE_SQL, [
        consumerName,
        messageId,
        reason.slice(0, MAX_ERROR_LENGTH),
        at.toISOString(),
      ]);
    return Number(row?.attempts ?? 1);
  }

  async clear(consumerName: string, messageId: string): Promise<void> {
    await this.orm.em
      .fork()
      .execute(`DELETE FROM consumer_message_attempts WHERE consumer_name = ? AND message_id = ?`, [
        consumerName,
        messageId,
      ]);
  }
}
