/**
 * Context interface for the File Drop CDK infrastructure.
 * Defines configuration for S3 bucket and Lambda processing pipeline.
 */
export interface IContext {
  /**
   * Unique identifier for the CDK stack
   */
  STACK_ID: string;

  /**
   * AWS account ID where resources will be deployed
   */
  ACCOUNT: string;

  /**
   * AWS region for deployment
   */
  REGION: string;

  /**
   * Resource tags for cost allocation and organization
   */
  TAGS: {
    Landscape: string;
    Service: string;
    Function: string;
    CostCenter?: string;
    Ticket?: string;
  };

  BUCKET: BucketConfig;

  /**
   * Lambda configuration
   */
  LAMBDA: {
    /**
     * Configuration for the event processor Lambda function that handles S3 events and invokes subscriber Lambdas
     */
    eventProcessor: LambdaConfig;

    /**
     * Optional configuration for a test subscriber Lambda function that can be created for testing purposes.
     * If not provided, no test subscriber Lambda will be created,
     */
    subscriberForTesting?: LambdaConfig;
  };
}

/**
 * S3 bucket configuration
 */
export type BucketConfig = {
  name?: string; // Base bucket name (actual bucket will have stack ID and landscape appended)
  subdirectories: BucketSubdirectory[];
  access?: {
    username: string;
    secretArn: string;
  }
}

export type LambdaConfig = {
  /**
   * Timeout in seconds for Lambda functions
   */
  timeoutSeconds?: number;

  /**
   * Memory allocation in MB for Lambda functions
   */
  memorySizeMb?: number;
}

export type BucketSubdirectory = {
  /**
   * Subdirectory where data files are expected to land
   * Example: "data-full"
   */
  path: string;

  /**
   * Number of days before objects are automatically deleted
   * Enforced by S3 bucket lifecycle rules
   */
  objectLifetimeDays: number;

  /**
   * Number of days before error objects are automatically deleted
   * Applies to files moved to {subfolder}/errors/ subdirectory
   * If omitted, inherits objectLifetimeDays value
   * Useful for retaining failed files longer for debugging
   */
  errorObjectLifetimeDays?: number;

  /**
   * Lambda function ARN to invoke after successful processing
   * Used by event processor to trigger the subscriber Lambda
   */
  subscriberLambdaArn: string;

  /**
   * Execution role ARN of the subscriber Lambda function
   * Used to grant S3 bucket read permissions via bucket policy
   * Example: "arn:aws:iam::123456789012:role/data-processor-role"
   */
  subscriberLambdaExecutionRoleArn: string;
}