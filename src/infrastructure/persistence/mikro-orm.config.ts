import { Migrator } from '@mikro-orm/migrations';
import { defineConfig, MikroORM } from '@mikro-orm/postgresql';
import { Migration20261021000000_initial_schema } from './migrations/Migration20261021000000_initial_schema';
import { Migration20261022000000_consumer_message_attempts } from './migrations/Migration20261022000000_consumer_message_attempts';
import { ENTITY_SCHEMAS } from './records';

export interface OrmOptions {
  url: string;
  poolMax: number;
  debug?: boolean;
}

export function createOrmConfig(options: OrmOptions) {
  return defineConfig({
    clientUrl: options.url,
    entities: ENTITY_SCHEMAS,
    pool: { min: 0, max: options.poolMax },
    extensions: [Migrator],
    migrations: {
      tableName: 'mikro_orm_migrations',
      transactional: true,
      allOrNothing: true,
      migrationsList: [
        { name: 'Migration20261021000000_initial_schema', class: Migration20261021000000_initial_schema },
        {
          name: 'Migration20261022000000_consumer_message_attempts',
          class: Migration20261022000000_consumer_message_attempts,
        },
      ],
    },
    ensureDatabase: false,
    allowGlobalContext: false,
    debug: options.debug ?? false,
  });
}

export async function createOrm(options: OrmOptions): Promise<MikroORM> {
  return MikroORM.init(createOrmConfig(options));
}
