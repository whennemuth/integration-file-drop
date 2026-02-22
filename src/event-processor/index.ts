import { S3Event, S3EventRecord, Context as LambdaContext } from 'aws-lambda';
import { 
  S3Client, 
  CopyObjectCommand, 
  DeleteObjectCommand
} from '@aws-sdk/client-s3';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
import { BucketSubdirectory } from '../../context/IContext';

const s3Client = new S3Client({});
const lambdaClient = new LambdaClient({});

const BUCKET_CONFIG = JSON.parse(process.env.BUCKET_CONFIG || '{"subdirectories": []}');

/**
 * Lambda handler for processing S3 events
 */
export async function handler(event: S3Event, context: LambdaContext): Promise<void> {
  console.log('Received S3 event:', JSON.stringify(event, null, 2));

  for (const record of event.Records) {
    try {
      await processRecord(record);
    } catch (error) {
      console.error('Error processing record:', error);
      // Continue processing other records even if one fails
    }
  }
}

/**
 * Process a single S3 event record
 */
async function processRecord(record: S3EventRecord): Promise<void> {
  const bucket = record.s3.bucket.name;
  const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));

  console.log(`Processing object: s3://${bucket}/${key}`);

  // Find matching subdirectory configuration
  const subfolderConfig = findSubfolderConfig(key);
  if (!subfolderConfig) {
    console.log(`Object not in any configured subfolder. Logging and exiting.`);
    return;
  }

  console.log(`Matched subfolder: ${subfolderConfig.path}`);

  // Skip if file has already been processed (matches date-based naming pattern)
  // Pattern: {subfolder}/{prefix}-{ISO_TIMESTAMP}.json (e.g., full-2026-02-20T16:57:35.356Z.json)
  const filename = key.split('/').pop() || '';
  const dateBasedPattern = /^(full|delta|invalid-json)-\d{4}-\d{2}-\d{2}T[\d:.]+Z\.json$/;
  if (dateBasedPattern.test(filename)) {
    console.log(`File ${filename} has already been processed (matches date-based pattern). Skipping to avoid recursive loop.`);
    return;
  }

  // Rename file if it doesn't have .json extension
  let finalKey = key;
  if (!key.endsWith('.json')) {
    console.log(`File does not have .json extension. Renaming...`);
    const renamedKey = await renameToJsonExtension(bucket, key);
    if (!renamedKey) {
      console.error(`Failed to rename object: ${key}`);
      return;
    }
    finalKey = renamedKey;
  }

  // Rename with date-based convention
  const newKey = generateDateBasedFileName(subfolderConfig.path);
  console.log(`Renaming ${finalKey} to ${newKey}`);
  const renamed = await renameObject(bucket, finalKey, newKey);
  if (!renamed) {
    console.error(`Failed to rename object to date-based filename`);
    return;
  }

  // Note: Object expiration is handled by S3 bucket lifecycle rules (configured per subdirectory)

  // Invoke target Lambda function for this subfolder
  await invokeTargetLambda(bucket, newKey, subfolderConfig.targetLambdaArn);

  console.log(`Successfully processed: ${newKey}`);
}

/**
 * Find the matching subdirectory configuration for a given key
 */
function findSubfolderConfig(key: string): BucketSubdirectory | null {
  const subdirectories = BUCKET_CONFIG.subdirectories as BucketSubdirectory[];
  
  for (const subdir of subdirectories) {
    if (key.startsWith(`${subdir.path}/`)) {
      return subdir;
    }
  }
  
  return null;
}

/**
 * Rename object to have .json extension
 */
async function renameToJsonExtension(bucket: string, key: string): Promise<string | null> {
  const newKey = `${key}.json`;
  const success = await renameObject(bucket, key, newKey);
  return success ? newKey : null;
}

/**
 * Generate date-based filename for a specific subfolder
 * Extracts prefix from subfolder path (e.g., "data-full" -> "full-", "data-delta" -> "delta-")
 */
function generateDateBasedFileName(subfolderPath: string): string {
  const timestamp = new Date().toISOString();
  // Extract last part after hyphen as prefix (e.g., "data-full" -> "full")
  const prefix = subfolderPath.split('-').pop() || 'data';
  return `${subfolderPath}/${prefix}-${timestamp}.json`;
}

/**
 * Rename S3 object by copying and deleting original
 */
async function renameObject(bucket: string, oldKey: string, newKey: string): Promise<boolean> {
  try {
    // Copy to new key
    await s3Client.send(new CopyObjectCommand({
      Bucket: bucket,
      CopySource: `${bucket}/${oldKey}`,
      Key: newKey
    }));

    // Delete old key
    await s3Client.send(new DeleteObjectCommand({
      Bucket: bucket,
      Key: oldKey
    }));

    console.log(`Successfully renamed ${oldKey} to ${newKey}`);
    return true;
  } catch (error) {
    console.error(`Error renaming object from ${oldKey} to ${newKey}:`, error);
    return false;
  }
}

/**
 * Invoke the target Lambda function for this subfolder with S3 path
 */
async function invokeTargetLambda(bucket: string, key: string, targetArn: string): Promise<void> {
  const s3Path = `s3://${bucket}/${key}`;

  try {
    console.log(`Invoking target Lambda: ${targetArn}`);
    
    const payload = {
      s3Path,
      bucket,
      key,
      processingMetadata: {
        processedAt: new Date().toISOString(),
        processorVersion: '1.0.0'
      }
    };

    const command = new InvokeCommand({
      FunctionName: targetArn,
      InvocationType: 'Event', // Async invocation
      Payload: Buffer.from(JSON.stringify(payload))
    });

    await lambdaClient.send(command);
    console.log(`Successfully invoked target Lambda: ${targetArn}`);
  } catch (error) {
    console.error(`Error invoking target Lambda (${targetArn}):`, error);
    throw error;
  }
}
