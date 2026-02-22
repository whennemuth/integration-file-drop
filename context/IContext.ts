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

  /**
   * S3 bucket configuration
   */
  BUCKET: {
    name?: string; // Base bucket name (actual bucket will have stack ID and landscape appended)
    subdirectories: BucketSubdirectory[];
    access?: {
      username: string;
      secretArn: string;
    }
  };

  /**
   * Lambda configuration
   */
  LAMBDA: {
    /**
     * Timeout in seconds for Lambda functions
     */
    timeoutSeconds?: number;

    /**
     * Memory allocation in MB for Lambda functions
     */
    memorySizeMb?: number;
  };

  /**
   * When true, creates a test target Lambda function for testing the event processor
   * This Lambda logs the event payload and attempts to read the S3 object
   */
  CREATE_TEST_TARGET_LAMBDA?: boolean;
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
   * Lambda function ARN to invoke after successful processing
   * Used by event processor to trigger the target Lambda
   */
  targetLambdaArn: string;

  /**
   * Execution role ARN of the target Lambda function
   * Used to grant S3 bucket read permissions via bucket policy
   * Example: "arn:aws:iam::123456789012:role/data-processor-role"
   */
  targetLambdaExecutionRoleArn: string;
}