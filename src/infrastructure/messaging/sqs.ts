import { GetQueueAttributesCommand, GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';

export interface SqsConnectionOptions {
  region: string;
  endpoint?: string;
  accessKeyId: string;
  secretAccessKey: string;
}

export function createSqsClient(options: SqsConnectionOptions): SQSClient {
  return new SQSClient({
    region: options.region,
    ...(options.endpoint ? { endpoint: options.endpoint } : {}),
    credentials: { accessKeyId: options.accessKeyId, secretAccessKey: options.secretAccessKey },
    maxAttempts: 3,
  });
}

export class SqsQueues {
  private readonly urls = new Map<string, Promise<string>>();

  constructor(readonly client: SQSClient) {}

  url(queueName: string): Promise<string> {
    let url = this.urls.get(queueName);
    if (!url) {
      url = this.client.send(new GetQueueUrlCommand({ QueueName: queueName })).then((result) => {
        if (!result.QueueUrl) throw new Error(`Queue ${queueName} not found`);
        return result.QueueUrl;
      });
      url.catch(() => this.urls.delete(queueName));
      this.urls.set(queueName, url);
    }
    return url;
  }

  async maxReceiveCount(queueName: string): Promise<number | undefined> {
    const result = await this.client.send(
      new GetQueueAttributesCommand({ QueueUrl: await this.url(queueName), AttributeNames: ['RedrivePolicy'] }),
    );
    const policy = result.Attributes?.RedrivePolicy;
    if (!policy) return undefined;
    const parsed = JSON.parse(policy) as { maxReceiveCount?: number | string };
    return parsed.maxReceiveCount !== undefined ? Number(parsed.maxReceiveCount) : undefined;
  }

  async ping(queueName: string): Promise<void> {
    await this.client.send(
      new GetQueueAttributesCommand({ QueueUrl: await this.url(queueName), AttributeNames: ['QueueArn'] }),
    );
  }
}
