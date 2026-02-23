import { S3Event, Context as LambdaContext } from 'aws-lambda';
import { BucketConfig } from '../../context/IContext';
import { Bucket } from './Bucket';
import { S3EventProcessor } from './S3EventProcessor';

const BUCKET_CONFIG: BucketConfig = JSON.parse(process.env.BUCKET_CONFIG || '{"subdirectories": []}');

/**
 * Lambda handler for processing S3 events
 */
export async function handler(event: S3Event, context: LambdaContext): Promise<void> {
  console.log('Received S3 event:', JSON.stringify(event, null, 2));

  for (const record of event.Records) {
    try {
      const bucket = new Bucket(BUCKET_CONFIG);
      const processor = new S3EventProcessor(record, bucket);
      const result = await processor.process();
      
      console.log('Processing result:', JSON.stringify(result, null, 2));
    } catch (error) {
      console.error('Error processing record:', error);
      // Continue processing other records even if one fails
    }
  }
}
