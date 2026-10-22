import { Migration } from '@mikro-orm/migrations';

export class Migration20261022000000_consumer_message_attempts extends Migration {
  override name = 'Migration20261022000000_consumer_message_attempts';

  override async up(): Promise<void> {
    this.addSql(`
      CREATE TABLE consumer_message_attempts (
        consumer_name  varchar(128) NOT NULL,
        message_id     varchar(256) NOT NULL,
        attempts       integer      NOT NULL,
        last_error     varchar(500),
        updated_at     timestamptz  NOT NULL,
        CONSTRAINT consumer_message_attempts_pk PRIMARY KEY (consumer_name, message_id),
        CONSTRAINT consumer_message_attempts_positive_ck CHECK (attempts >= 1)
      );
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`DROP TABLE IF EXISTS consumer_message_attempts;`);
  }
}
