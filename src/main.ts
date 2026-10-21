import { createApp } from './bootstrap';
import { loadConfig } from './infrastructure/config/app-config';

async function main(): Promise<void> {
  const config = loadConfig();
  const app = await createApp(config);
  await app.listen(config.port, '0.0.0.0');
}

main().catch((error: unknown) => {
  console.error(JSON.stringify({ level: 'fatal', message: 'failed to start', error: String(error) }));
  process.exit(1);
});
