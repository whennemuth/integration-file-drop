import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
import { IBucket } from "./Bucket"

export type ISubscriber = {
  notify: (bucket: IBucket, newKey: string) => Promise<void>
}

export class Subscriber implements ISubscriber {
  private subscriberLambdaArn: string;
  private lambdaClient: LambdaClient;

  constructor(subscriberLambdaArn: string, lambdaClient?: LambdaClient) {
    this.subscriberLambdaArn = subscriberLambdaArn;
    this.lambdaClient = lambdaClient || new LambdaClient({});
  }

  async notify(bucket: IBucket, newKey: string): Promise<void> {
    await this.invokeSubscriberLambda(bucket.getName(), newKey);
  }

  private invokeSubscriberLambda = async (bucketName: string, key: string): Promise<void> => {
    const s3Path = `s3://${bucketName}/${key}`;

    try {
      console.log(`Invoking subscriber Lambda: ${this.subscriberLambdaArn}`);
      
      const payload = {
        s3Path,
        bucket: bucketName,
        key,
        processingMetadata: {
          processedAt: new Date().toISOString(),
          processorVersion: '1.0.0'
        }
      };

      const command = new InvokeCommand({
        FunctionName: this.subscriberLambdaArn,
        InvocationType: 'Event', // Async invocation
        Payload: Buffer.from(JSON.stringify(payload))
      });

      await this.lambdaClient.send(command);
      console.log(`Successfully invoked subscriber Lambda: ${this.subscriberLambdaArn}`);
    } catch (error) {
      console.error(`Error invoking subscriber Lambda (${this.subscriberLambdaArn}):`, error);
      throw error;
    }
  }
}