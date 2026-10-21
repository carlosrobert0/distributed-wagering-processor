import { AppLogger } from '../../application/ports/system';

export interface PollingWorkerOptions {
  name: string;
  intervalMs: number;
  tick: () => Promise<boolean>;
}

export class PollingWorker {
  private running = false;
  private loop?: Promise<void>;
  private wake?: () => void;

  constructor(
    private readonly options: PollingWorkerOptions,
    private readonly logger: AppLogger,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
    this.logger.info('worker started', { worker: this.options.name });
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.wake?.();
    await this.loop;
    this.logger.info('worker stopped', { worker: this.options.name });
  }

  private async run(): Promise<void> {
    while (this.running) {
      let moreWork = false;
      try {
        moreWork = await this.options.tick();
      } catch (error) {
        this.logger.error('worker tick failed', { worker: this.options.name }, error);
      }
      if (this.running && !moreWork) {
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, this.options.intervalMs);
          this.wake = () => {
            clearTimeout(timer);
            resolve();
          };
        });
      }
    }
  }
}
