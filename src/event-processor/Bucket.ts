import { S3Client, CopyObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { BucketConfig } from "../../context/IContext";

export type IBucket = {
  getConfig: () => BucketConfig
  getName: () => string
  renameObject(key: string, newKey: string): Promise<boolean>
  moveToErrors(key: string, subfolderPath: string, reason: string): Promise<boolean>
}

export class Bucket implements IBucket {
  private config: BucketConfig;
  private s3Client: S3Client;

  constructor(config: BucketConfig, s3Client?: S3Client) {
    this.config = config;
    this.s3Client = s3Client || new S3Client({});
  }

  getConfig(): BucketConfig {
    return this.config;
  }

  getName(): string {
    return this.config.name!;
  }

  /**
   * Rename S3 object by copying and deleting original
   */
  async renameObject(key: string, newKey: string): Promise<boolean> {
    try {
      const bucketName = this.getName();
      
      // Copy to new key
      await this.s3Client.send(new CopyObjectCommand({
        Bucket: bucketName,
        CopySource: `${bucketName}/${key}`,
        Key: newKey
      }));

      // Delete old key
      await this.s3Client.send(new DeleteObjectCommand({
        Bucket: bucketName,
        Key: key
      }));

      console.log(`Successfully renamed ${key} to ${newKey}`);
      return true;
    } catch (error) {
      console.error(`Error renaming object from ${key} to ${newKey}:`, error);
      return false;
    }
  }

  /**
   * Move object to errors subfolder within the same parent folder
   * Uses timestamp-based naming with ISO date prefix
   */
  async moveToErrors(key: string, subfolderPath: string, reason: string): Promise<boolean> {
    try {
      const bucketName = this.getName();
      const timestamp = new Date().toISOString();
      const originalFilename = key.split('/').pop() || 'unknown';
      const errorKey = `${subfolderPath}/errors/${timestamp}-${originalFilename}`;
      
      // Copy to errors subfolder with timestamp-based name
      await this.s3Client.send(new CopyObjectCommand({
        Bucket: bucketName,
        CopySource: `${bucketName}/${key}`,
        Key: errorKey,
        TaggingDirective: 'REPLACE',
        Tagging: `error-reason=${encodeURIComponent(reason)}`
      }));

      // Delete original
      await this.s3Client.send(new DeleteObjectCommand({
        Bucket: bucketName,
        Key: key
      }));

      console.log(`Moved ${key} to errors subfolder: ${errorKey} (Reason: ${reason})`);
      return true;
    } catch (error) {
      console.error(`Error moving ${key} to errors subfolder:`, error);
      return false;
    }
  }
}