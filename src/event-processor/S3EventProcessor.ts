import { S3EventRecord } from 'aws-lambda';
import { BucketConfig, BucketSubdirectory } from '../../context/IContext';
import { IBucket } from './Bucket';
import { ISubscriber, Subscriber } from './Subscriber';

export type ProcessingResult = {
  success: boolean;
  action: 'skipped-no-match' | 'skipped-already-processed' | 'renamed' | 'error-rename' | 'error-invoke';
  reason?: string;
  originalKey: string;
  newKey?: string;
  subscriberInvoked?: boolean;
  movedToErrors?: boolean;
}

export type ClockFunction = () => string;

/**
 * Processes S3 event records for file arrival handling
 * Responsibilities:
 * - Match files to configured subdirectories
 * - Detect and skip already-processed files (recursion prevention)
 * - Rename files with timestamp prefix
 * - Invoke subscriber Lambda functions
 * - Move failed invocation files to errors subfolder
 */
export class S3EventProcessor {
  private record: S3EventRecord;
  private bucket: IBucket;
  private clockFn: ClockFunction;

  /**
   * Strict ISO 8601 timestamp pattern for recursion detection
   * Format: YYYY-MM-DDTHH:mm:ss.SSSZ-
   * Example: 2026-02-22T10:30:00.000Z-filename.json
   */
  private static readonly TIMESTAMP_PREFIX_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z-/;

  constructor(
    record: S3EventRecord,
    bucket: IBucket,
    clockFn?: ClockFunction
  ) {
    this.record = record;
    this.bucket = bucket;
    this.clockFn = clockFn || (() => new Date().toISOString());
  }

  /**
   * Process the S3 event record
   */
  async process(): Promise<ProcessingResult> {
    const bucketName = this.record.s3.bucket.name;
    const key = decodeURIComponent(this.record.s3.object.key.replace(/\+/g, ' '));

    console.log(`Processing object: s3://${bucketName}/${key}`);

    const result: ProcessingResult = {
      success: false,
      action: 'error-rename',
      originalKey: key
    };

    // Find matching subdirectory configuration
    const subfolderConfig = this.findSubfolderConfig(key);
    if (!subfolderConfig) {
      console.log(`Object not in any configured subfolder. Logging and exiting.`);
      return {
        ...result,
        success: true,
        action: 'skipped-no-match',
        reason: 'File does not match any configured subdirectory'
      };
    }

    console.log(`Matched subfolder: ${subfolderConfig.path}`);

    // Skip if file has already been processed (recursion prevention)
    const filename = key.split('/').pop() || '';
    if (this.isAlreadyProcessed(filename)) {
      console.log(`File ${filename} has already been processed (matches timestamp prefix pattern). Skipping to avoid recursive loop.`);
      return {
        ...result,
        success: true,
        action: 'skipped-already-processed',
        reason: 'File already has timestamp prefix indicating previous processing'
      };
    }

    // For nested subdirectories, preserve the structure
    // Extract relative path within the configured subfolder
    const relativePath = key.substring(subfolderConfig.path.length + 1); // +1 for the '/'
    const pathParts = relativePath.split('/');
    const filenameOnly = pathParts[pathParts.length - 1];
    const nestedPath = pathParts.slice(0, -1).join('/'); // Get parent directories if any
    
    // Rename with timestamp prefix to preserve original filename completely
    const newKey = this.generateDateBasedFileName(subfolderConfig.path, nestedPath, filenameOnly);
    console.log(`Renaming ${key} to ${newKey}`);
    
    const renamed = await this.bucket.renameObject(key, newKey);
    if (!renamed) {
      console.error(`Failed to rename object to date-based filename`);
      return {
        ...result,
        action: 'error-rename',
        reason: 'Failed to rename object in S3'
      };
    }

    result.newKey = newKey;
    result.action = 'renamed';

    // Invoke subscriber Lambda function for this subfolder
    try {
      const subscriber = new Subscriber(subfolderConfig.subscriberLambdaArn);
      await subscriber.notify(this.bucket, newKey);
      result.subscriberInvoked = true;
      result.success = true;
      console.log(`Successfully processed: ${newKey}`);
    } catch (error) {
      console.error(`Error invoking subscriber Lambda. Moving file to errors subfolder.`, error);
      
      // Move to errors subfolder
      const movedToErrors = await this.bucket.moveToErrors(
        newKey,
        subfolderConfig.path,
        `Subscriber Lambda invocation failed: ${error}`
      );
      
      return {
        ...result,
        success: false,
        action: 'error-invoke',
        reason: `Subscriber Lambda invocation failed: ${error}`,
        subscriberInvoked: false,
        movedToErrors
      };
    }

    return result;
  }

  /**
   * Check if filename indicates it has already been processed
   * Uses strict ISO 8601 timestamp pattern followed by hyphen
   */
  private isAlreadyProcessed(filename: string): boolean {
    return S3EventProcessor.TIMESTAMP_PREFIX_PATTERN.test(filename);
  }

  /**
   * Find the matching subdirectory configuration for a given key
   */
  private findSubfolderConfig(key: string): BucketSubdirectory | null {
    const config = this.bucket.getConfig();
    const subdirectories = config.subdirectories;
    
    for (const subdir of subdirectories) {
      if (key.startsWith(`${subdir.path}/`)) {
        return subdir;
      }
    }
    
    return null;
  }

  /**
   * Generate timestamp-prefixed filename for a specific subfolder
   * Preserves the original filename completely by prefixing with ISO timestamp
   * Example: "data.json" becomes "2026-02-20T16:57:35.356Z-data.json"
   */
  private generateDateBasedFileName(subfolderPath: string, nestedPath: string, originalFilename: string): string {
    const timestamp = this.clockFn();
    if (nestedPath) {
      return `${subfolderPath}/${nestedPath}/${timestamp}-${originalFilename}`;
    }
    return `${subfolderPath}/${timestamp}-${originalFilename}`;
  }
}
