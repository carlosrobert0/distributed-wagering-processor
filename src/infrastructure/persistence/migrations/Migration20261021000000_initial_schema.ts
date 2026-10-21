import { Migration } from '@mikro-orm/migrations';

export class Migration20261021000000_initial_schema extends Migration {
  override name = 'Migration20261021000000_initial_schema';

  override async up(): Promise<void> {
    this.addSql(`
      CREATE FUNCTION reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION '% is append-only: % is not allowed', TG_TABLE_NAME, TG_OP
          USING ERRCODE = 'restrict_violation';
      END;
      $$;
    `);

    this.addSql(`
      CREATE TABLE wallets (
        id          uuid           PRIMARY KEY,
        player_id   varchar(128)   NOT NULL,
        currency    char(3)        NOT NULL,
        balance     numeric(20,2)  NOT NULL,
        version     integer        NOT NULL,
        created_at  timestamptz    NOT NULL,
        updated_at  timestamptz    NOT NULL,
        CONSTRAINT wallets_player_currency_uq     UNIQUE (player_id, currency),
        CONSTRAINT wallets_id_currency_uq         UNIQUE (id, currency),
        CONSTRAINT wallets_balance_non_negative_ck CHECK (balance >= 0),
        CONSTRAINT wallets_version_positive_ck    CHECK (version >= 1),
        CONSTRAINT wallets_currency_iso_ck        CHECK (currency ~ '^[A-Z]{3}$')
      );
    `);
    this.addSql(`
      CREATE TRIGGER wallets_no_delete BEFORE DELETE ON wallets
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    `);
    this.addSql(`
      CREATE TRIGGER wallets_no_truncate BEFORE TRUNCATE ON wallets
        FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation();
    `);

    this.addSql(`
      CREATE TABLE wager_transactions (
        id                                 uuid          PRIMARY KEY,
        provider_id                        varchar(128)  NOT NULL,
        external_transaction_id            varchar(128)  NOT NULL,
        idempotency_key                    varchar(255)  NOT NULL,
        payload_hash                       char(64)      NOT NULL,
        wallet_id                          uuid          NOT NULL REFERENCES wallets (id),
        player_id                          varchar(128)  NOT NULL,
        round_id                           varchar(128)  NOT NULL,
        game_id                            varchar(128)  NOT NULL,
        kind                               varchar(16)   NOT NULL,
        amount                             numeric(20,2) NOT NULL,
        currency                           char(3)       NOT NULL,
        reference_external_transaction_id  varchar(128),
        reference_transaction_id           uuid          REFERENCES wager_transactions (id),
        status                             varchar(24)   NOT NULL,
        failure_code                       varchar(64),
        balance_after                      numeric(20,2),
        reference_attempts                 integer       NOT NULL DEFAULT 0,
        next_reference_check_at            timestamptz,
        created_at                         timestamptz   NOT NULL,
        processed_at                       timestamptz,
        updated_at                         timestamptz   NOT NULL,

        CONSTRAINT wager_tx_provider_external_uq UNIQUE (provider_id, external_transaction_id),
        CONSTRAINT wager_tx_idempotency_key_uq   UNIQUE (idempotency_key),

        CONSTRAINT wager_tx_kind_ck   CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        CONSTRAINT wager_tx_status_ck CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        CONSTRAINT wager_tx_currency_ck CHECK (currency ~ '^[A-Z]{3}$'),
        CONSTRAINT wager_tx_amount_ck CHECK (amount > 0 OR (kind = 'LOSS' AND amount = 0)),
        CONSTRAINT wager_tx_reversal_requires_reference_ck
          CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
        CONSTRAINT wager_tx_no_reference_ck
          CHECK (kind NOT IN ('BET','OPENING') OR reference_external_transaction_id IS NULL),
        CONSTRAINT wager_tx_reference_resolved_ck
          CHECK (status <> 'PROCESSED' OR reference_external_transaction_id IS NULL OR reference_transaction_id IS NOT NULL),
        CONSTRAINT wager_tx_failure_code_ck
          CHECK ((status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)),
        CONSTRAINT wager_tx_terminal_timestamp_ck
          CHECK ((status IN ('PROCESSED','REJECTED','FAILED')) = (processed_at IS NOT NULL)),
        CONSTRAINT wager_tx_observed_balance_ck
          CHECK ((status IN ('PROCESSED','REJECTED')) = (balance_after IS NOT NULL)),
        CONSTRAINT wager_tx_observed_balance_non_negative_ck CHECK (balance_after >= 0),
        CONSTRAINT wager_tx_pending_schedule_ck
          CHECK ((status = 'PENDING_REFERENCE') = (next_reference_check_at IS NOT NULL)),
        CONSTRAINT wager_tx_attempts_ck CHECK (reference_attempts >= 0),
        CONSTRAINT wager_tx_opening_internal_ck CHECK ((kind = 'OPENING') = (provider_id = 'internal'))
      );
    `);
    this.addSql(`
      CREATE UNIQUE INDEX wager_tx_single_reversal_uq ON wager_transactions (reference_transaction_id)
        WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED';
    `);
    this.addSql(`
      CREATE INDEX wager_tx_pending_reference_due_ix ON wager_transactions (next_reference_check_at)
        WHERE status = 'PENDING_REFERENCE';
    `);
    this.addSql(`
      CREATE INDEX wager_tx_waiting_on_reference_ix
        ON wager_transactions (provider_id, reference_external_transaction_id)
        WHERE status = 'PENDING_REFERENCE';
    `);
    this.addSql(`CREATE INDEX wager_tx_wallet_created_ix ON wager_transactions (wallet_id, created_at);`);
    this.addSql(
      `CREATE INDEX wager_tx_reference_ix ON wager_transactions (reference_transaction_id) WHERE reference_transaction_id IS NOT NULL;`,
    );

    this.addSql(`
      CREATE FUNCTION wager_tx_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          RAISE EXCEPTION 'wager_transactions rows cannot be deleted' USING ERRCODE = 'restrict_violation';
        END IF;
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'wager transaction % is terminal (%) and cannot change', OLD.id, OLD.status
            USING ERRCODE = 'restrict_violation';
        END IF;
        IF (NEW.id, NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key, NEW.payload_hash,
            NEW.wallet_id, NEW.player_id, NEW.round_id, NEW.game_id, NEW.kind, NEW.amount, NEW.currency,
            NEW.reference_external_transaction_id, NEW.created_at)
           IS DISTINCT FROM
           (OLD.id, OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key, OLD.payload_hash,
            OLD.wallet_id, OLD.player_id, OLD.round_id, OLD.game_id, OLD.kind, OLD.amount, OLD.currency,
            OLD.reference_external_transaction_id, OLD.created_at) THEN
          RAISE EXCEPTION 'business fields of wager transaction % are immutable', OLD.id
            USING ERRCODE = 'restrict_violation';
        END IF;
        RETURN NEW;
      END;
      $$;
    `);
    this.addSql(`
      CREATE TRIGGER wager_tx_guard BEFORE UPDATE OR DELETE ON wager_transactions
        FOR EACH ROW EXECUTE FUNCTION wager_tx_guard();
    `);
    this.addSql(`
      CREATE TRIGGER wager_tx_no_truncate BEFORE TRUNCATE ON wager_transactions
        FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation();
    `);

    this.addSql(`
      CREATE TABLE wallet_ledger_entries (
        id              uuid          PRIMARY KEY,
        wallet_id       uuid          NOT NULL,
        transaction_id  uuid          NOT NULL REFERENCES wager_transactions (id),
        wallet_version  integer       NOT NULL,
        direction       varchar(6)    NOT NULL,
        amount          numeric(20,2) NOT NULL,
        currency        char(3)       NOT NULL,
        balance_before  numeric(20,2) NOT NULL,
        balance_after   numeric(20,2) NOT NULL,
        created_at      timestamptz   NOT NULL,
        CONSTRAINT ledger_wallet_currency_fk FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
        CONSTRAINT ledger_transaction_wallet_uq UNIQUE (transaction_id, wallet_id),
        CONSTRAINT ledger_wallet_version_uq     UNIQUE (wallet_id, wallet_version),
        CONSTRAINT ledger_direction_ck          CHECK (direction IN ('DEBIT','CREDIT')),
        CONSTRAINT ledger_amount_positive_ck    CHECK (amount > 0),
        CONSTRAINT ledger_balances_non_negative_ck CHECK (balance_before >= 0 AND balance_after >= 0),
        CONSTRAINT ledger_version_positive_ck   CHECK (wallet_version >= 1),
        CONSTRAINT ledger_arithmetic_ck
          CHECK (balance_after = balance_before + CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END)
      );
    `);
    this.addSql(`
      CREATE TRIGGER ledger_append_only BEFORE UPDATE OR DELETE ON wallet_ledger_entries
        FOR EACH ROW EXECUTE FUNCTION reject_mutation();
    `);
    this.addSql(`
      CREATE TRIGGER ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
        FOR EACH STATEMENT EXECUTE FUNCTION reject_mutation();
    `);

    this.addSql(`
      CREATE FUNCTION ledger_validate_entry() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE
        tx        record;
        previous  record;
      BEGIN
        SELECT wallet_id, amount, currency, status INTO tx FROM wager_transactions WHERE id = NEW.transaction_id;
        IF tx.wallet_id <> NEW.wallet_id OR tx.amount <> NEW.amount OR tx.currency <> NEW.currency THEN
          RAISE EXCEPTION 'ledger entry % does not match transaction %', NEW.id, NEW.transaction_id
            USING ERRCODE = 'check_violation';
        END IF;
        IF tx.status <> 'PROCESSED' THEN
          RAISE EXCEPTION 'only PROCESSED transactions produce ledger entries (transaction % is %)', NEW.transaction_id, tx.status
            USING ERRCODE = 'check_violation';
        END IF;

        SELECT wallet_version, balance_after INTO previous
          FROM wallet_ledger_entries
         WHERE wallet_id = NEW.wallet_id
         ORDER BY wallet_version DESC
         LIMIT 1;
        IF FOUND THEN
          IF NEW.wallet_version <> previous.wallet_version + 1 OR NEW.balance_before <> previous.balance_after THEN
            RAISE EXCEPTION 'ledger chain broken for wallet % at version %', NEW.wallet_id, NEW.wallet_version
              USING ERRCODE = 'check_violation';
          END IF;
        ELSIF NEW.balance_before <> 0 THEN
          RAISE EXCEPTION 'first ledger entry of wallet % must start from zero', NEW.wallet_id
            USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
      END;
      $$;
    `);
    this.addSql(`
      CREATE TRIGGER ledger_validate_entry BEFORE INSERT ON wallet_ledger_entries
        FOR EACH ROW EXECUTE FUNCTION ledger_validate_entry();
    `);

    this.addSql(`
      CREATE FUNCTION assert_wallet_matches_ledger(target uuid) RETURNS void LANGUAGE plpgsql AS $$
      DECLARE
        w           record;
        last_entry  record;
      BEGIN
        SELECT balance, version INTO w FROM wallets WHERE id = target;
        IF NOT FOUND THEN
          RETURN;
        END IF;
        SELECT balance_after, wallet_version INTO last_entry
          FROM wallet_ledger_entries
         WHERE wallet_id = target
         ORDER BY wallet_version DESC
         LIMIT 1;
        IF FOUND THEN
          IF w.balance <> last_entry.balance_after OR w.version <> last_entry.wallet_version THEN
            RAISE EXCEPTION 'wallet % (balance %, version %) diverges from its ledger (balance %, version %)',
              target, w.balance, w.version, last_entry.balance_after, last_entry.wallet_version
              USING ERRCODE = 'check_violation';
          END IF;
        ELSIF w.balance <> 0 OR w.version <> 1 THEN
          RAISE EXCEPTION 'wallet % has balance/version changes without ledger entries', target
            USING ERRCODE = 'check_violation';
        END IF;
      END;
      $$;
    `);
    this.addSql(`
      CREATE FUNCTION wallets_matches_ledger_trg() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        PERFORM assert_wallet_matches_ledger(NEW.id);
        RETURN NULL;
      END;
      $$;
    `);
    this.addSql(`
      CREATE FUNCTION ledger_matches_wallet_trg() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        PERFORM assert_wallet_matches_ledger(NEW.wallet_id);
        RETURN NULL;
      END;
      $$;
    `);
    this.addSql(`
      CREATE CONSTRAINT TRIGGER wallets_matches_ledger_ctr AFTER INSERT OR UPDATE ON wallets
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION wallets_matches_ledger_trg();
    `);
    this.addSql(`
      CREATE CONSTRAINT TRIGGER ledger_matches_wallet_ctr AFTER INSERT ON wallet_ledger_entries
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION ledger_matches_wallet_trg();
    `);

    this.addSql(`
      CREATE TABLE inbox_messages (
        consumer_name  varchar(128) NOT NULL,
        message_id     varchar(256) NOT NULL,
        payload_hash   char(64)     NOT NULL,
        received_at    timestamptz  NOT NULL,
        processed_at   timestamptz,
        CONSTRAINT inbox_messages_pk PRIMARY KEY (consumer_name, message_id)
      );
    `);

    this.addSql(`
      CREATE TABLE outbox_messages (
        id               uuid         PRIMARY KEY,
        aggregate_id     varchar(128) NOT NULL,
        partition_key    varchar(128) NOT NULL,
        event_type       varchar(64)  NOT NULL,
        payload          jsonb        NOT NULL,
        occurred_at      timestamptz  NOT NULL,
        attempts         integer      NOT NULL DEFAULT 0,
        next_attempt_at  timestamptz,
        published_at     timestamptz,
        last_error       varchar(500),
        CONSTRAINT outbox_attempts_ck CHECK (attempts >= 0),
        CONSTRAINT outbox_schedule_ck CHECK (published_at IS NOT NULL OR next_attempt_at IS NOT NULL)
      );
    `);
    this.addSql(`
      CREATE INDEX outbox_due_ix ON outbox_messages (next_attempt_at, occurred_at) WHERE published_at IS NULL;
    `);
    this.addSql(`
      CREATE FUNCTION outbox_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.id, NEW.aggregate_id, NEW.partition_key, NEW.event_type, NEW.payload, NEW.occurred_at)
           IS DISTINCT FROM (OLD.id, OLD.aggregate_id, OLD.partition_key, OLD.event_type, OLD.payload, OLD.occurred_at) THEN
          RAISE EXCEPTION 'outbox event % is immutable', OLD.id USING ERRCODE = 'restrict_violation';
        END IF;
        IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
          RAISE EXCEPTION 'outbox event % was already published', OLD.id USING ERRCODE = 'restrict_violation';
        END IF;
        RETURN NEW;
      END;
      $$;
    `);
    this.addSql(`
      CREATE TRIGGER outbox_guard BEFORE UPDATE ON outbox_messages
        FOR EACH ROW EXECUTE FUNCTION outbox_guard();
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`DROP TABLE IF EXISTS outbox_messages;`);
    this.addSql(`DROP FUNCTION IF EXISTS outbox_guard();`);
    this.addSql(`DROP TABLE IF EXISTS inbox_messages;`);
    this.addSql(`DROP TABLE IF EXISTS wallet_ledger_entries;`);
    this.addSql(`DROP TABLE IF EXISTS wager_transactions;`);
    this.addSql(`DROP TABLE IF EXISTS wallets;`);
    this.addSql(`DROP FUNCTION IF EXISTS ledger_matches_wallet_trg();`);
    this.addSql(`DROP FUNCTION IF EXISTS wallets_matches_ledger_trg();`);
    this.addSql(`DROP FUNCTION IF EXISTS assert_wallet_matches_ledger(uuid);`);
    this.addSql(`DROP FUNCTION IF EXISTS ledger_validate_entry();`);
    this.addSql(`DROP FUNCTION IF EXISTS wager_tx_guard();`);
    this.addSql(`DROP FUNCTION IF EXISTS reject_mutation();`);
  }
}
