import { loadConfig } from '../config/app-config';
import { createOrm } from './mikro-orm.config';

async function main(): Promise<void> {
  const direction = process.argv[2] ?? 'up';
  const config = loadConfig();
  const orm = await createOrm({ url: config.database.url, poolMax: 2 });
  try {
    const executed = direction === 'down' ? await orm.migrator.down({ to: 0 }) : await orm.migrator.up();
    const names = executed.map((migration) => migration.name).join(', ') || 'nothing to do';
    console.log(`migrations ${direction}: ${names}`);
  } finally {
    await orm.close(true);
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
