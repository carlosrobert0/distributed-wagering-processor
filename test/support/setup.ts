import { afterAll, setDefaultTimeout } from 'bun:test';
import { closeTestPools } from './database';
import { dropRunDatabases } from './environment';

setDefaultTimeout(180_000);

afterAll(async () => {
  await closeTestPools();
  await dropRunDatabases().catch(() => undefined);
});
