import { NodeHttpHandler } from '@smithy/node-http-handler';
import { GetQueueAttributesCommand, GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';

export interface SqsConnectionOptions {
  region: string;
  endpoint?: string;
  accessKeyId: string;
  secretAccessKey: string;
}

export const SQS_CONNECTION_TIMEOUT_MS = 2_000;
export const SQS_REQUEST_TIMEOUT_MS = 5_000;

export function createSqsClient(options: SqsConnectionOptions): SQSClient {
  return new SQSClient({
    region: options.region,
    ...(options.endpoint ? { endpoint: options.endpoint } : {}),
    credentials: { accessKeyId: options.accessKeyId, secretAccessKey: options.secretAccessKey },
    maxAttempts: 3,
    requestHandler: new NodeHttpHandler({
      connectionTimeout: SQS_CONNECTION_TIMEOUT_MS,
      requestTimeout: SQS_REQUEST_TIMEOUT_MS,
      throwOnRequestTimeout: true,
    }),
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

  async ping(queueName: string): Promise<void> {
    await this.client.send(
      new GetQueueAttributesCommand({ QueueUrl: await this.url(queueName), AttributeNames: ['QueueArn'] }),
    );
  }
}
