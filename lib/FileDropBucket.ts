import { RemovalPolicy, Duration } from 'aws-cdk-lib';
import { 
  Bucket, 
  BlockPublicAccess, 
  ObjectOwnership,
  EventType,
  BucketEncryption
} from 'aws-cdk-lib/aws-s3';
import { PolicyStatement, Effect, ArnPrincipal } from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';

export type DropoffBucketProps = {
  context: IContext;
};

export const getBucketName = (context: IContext): string => {
  const { STACK_ID, BUCKET: { name }, TAGS: { Landscape } } = context;
  if(name) {
    return `${name}-${Landscape}`;
  }
  return `${STACK_ID}-${Landscape}`;
}

/**
 * Creates an S3 bucket for receiving data files via async API responses.
 * 
 * Features:
 * - Automatically deletes objects when stack is destroyed
 * - Server-side encryption enabled
 * - Public access blocked
 * - Event notifications for object creation
 */
export class FileDropBucket extends Construct {
  public readonly bucket: Bucket;

  constructor(scope: Construct, id: string, props: DropoffBucketProps) {
    super(scope, id);

    const { context } = props;
    const bucketName = getBucketName(context);

    // Create S3 bucket for data file drop
    this.bucket = new Bucket(this, 'bucket', {
      bucketName,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      encryption: BucketEncryption.S3_MANAGED,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      versioned: false,
      enforceSSL: true,
      lifecycleRules: this.createLifecycleRules(context)
    });
  }

  /**
   * Create lifecycle rules for automatic object expiration
   * Each subdirectory gets its own rule with specific retention period
   * If errorObjectLifetimeDays is specified, creates separate rule for errors subfolder
   */
  private createLifecycleRules(context: IContext) {
    const rules: any[] = [];
    
    context.BUCKET.subdirectories.forEach((subdir, index) => {
      // Main subdirectory lifecycle rule  
      rules.push({
        id: `expire-${subdir.path}`,
        enabled: true,
        prefix: `${subdir.path}/`,
        expiration: Duration.days(subdir.objectLifetimeDays)
      });
      
      // Separate lifecycle rule for errors subdirectory if configured
      if (subdir.errorObjectLifetimeDays && subdir.errorObjectLifetimeDays !== subdir.objectLifetimeDays) {
        rules.push({
          id: `expire-${subdir.path}-errors`,
          enabled: true,
          prefix: `${subdir.path}/errors/`,
          expiration: Duration.days(subdir.errorObjectLifetimeDays)
        });
      }
    });
    
    return rules;
  }

  /**
   * Grant read access to a Lambda function via its execution role
   * @param executionRoleArn ARN of the Lambda function's execution role
   * @param index Index for unique policy statement ID
   */
  public grantReadToLambda(executionRoleArn: string, index: number): void {
    this.bucket.addToResourcePolicy(
      new PolicyStatement({
        sid: `AllowLambdaRead${index}`,
        effect: Effect.ALLOW,
        principals: [new ArnPrincipal(executionRoleArn)],
        actions: [
          's3:GetObject',
          's3:GetObjectVersion',
          's3:ListBucket'
        ],
        resources: [
          this.bucket.bucketArn,
          `${this.bucket.bucketArn}/*`
        ]
      })
    );
  }
}
