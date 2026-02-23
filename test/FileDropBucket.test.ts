import { App, Stack } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { BucketSubdirectory, IContext } from '../context/IContext';
import { FileDropBucket } from '../lib/FileDropBucket';

describe('FileDropBucket', () => {
  let app: App;
  let stack: Stack;
  let mockContext: IContext;

  beforeEach(() => {
    app = new App();
    stack = new Stack(app, 'TestStack');
  });

  /**
   * Helper to create a complete mock context with required properties
   */
  const createMockContext = (subdirectories: BucketSubdirectory[]): IContext => {
    return {
      STACK_ID: 'test-stack',
      ACCOUNT: '123456789012',
      REGION: 'us-east-2',
      TAGS: {
        Landscape: 'test',
        Application: 'file-drop',
        Service: 'integration',
        Function: 'file-processing'
      },
      BUCKET: {
        name: 'test-bucket',
        subdirectories
      },
      LAMBDA: {
        eventProcessor: {
          timeoutSeconds: 30
        }
      }
    } as IContext;
  };

  describe('Lifecycle Rules', () => {
    it('should create lifecycle rule with objectLifetimeDays for normal files', () => {
      // Arrange
      mockContext = createMockContext([
        {
          path: 'person-full',
          objectLifetimeDays: 7,
          subscriberLambdaArn: 'arn:aws:lambda:us-east-2:123456789012:function:subscriber',
          subscriberLambdaExecutionRoleArn: 'arn:aws:iam::123456789012:role/subscriber-role'
        }
      ]);

      // Act
      new FileDropBucket(stack, 'TestBucket', { context: mockContext });
      const template = Template.fromStack(stack);

      // Assert - Should have lifecycle rule for person-full/
      template.hasResourceProperties('AWS::S3::Bucket', {
        LifecycleConfiguration: {
          Rules: [
            {
              Id: 'expire-person-full',
              Status: 'Enabled',
              Prefix: 'person-full/',
              ExpirationInDays: 7
            }
          ]
        }
      });
    });

    it('should create separate lifecycle rule for errors with errorObjectLifetimeDays', () => {
      // Arrange
      mockContext = createMockContext([
        {
          path: 'person-full',
          objectLifetimeDays: 7,
          errorObjectLifetimeDays: 14,
          subscriberLambdaArn: 'arn:aws:lambda:us-east-2:123456789012:function:subscriber',
          subscriberLambdaExecutionRoleArn: 'arn:aws:iam::123456789012:role/subscriber-role'
        }
      ]);

      // Act
      new FileDropBucket(stack, 'TestBucket', { context: mockContext });
      const template = Template.fromStack(stack);

      // Assert - Should have TWO lifecycle rules
      template.hasResourceProperties('AWS::S3::Bucket', {
        LifecycleConfiguration: {
          Rules: [
            {
              Id: 'expire-person-full',
              Status: 'Enabled',
              Prefix: 'person-full/',
              ExpirationInDays: 7
            },
            {
              Id: 'expire-person-full-errors',
              Status: 'Enabled',
              Prefix: 'person-full/errors/',
              ExpirationInDays: 14
            }
          ]
        }
      });
    });

    it('should NOT create separate error rule when errorObjectLifetimeDays equals objectLifetimeDays', () => {
      // Arrange
      mockContext = createMockContext([
        {
          path: 'person-delta',
          objectLifetimeDays: 3,
          errorObjectLifetimeDays: 3, // Same as objectLifetimeDays
          subscriberLambdaArn: 'arn:aws:lambda:us-east-2:123456789012:function:subscriber',
          subscriberLambdaExecutionRoleArn: 'arn:aws:iam::123456789012:role/subscriber-role'
        }
      ]);

      // Act
      new FileDropBucket(stack, 'TestBucket', { context: mockContext });
      const template = Template.fromStack(stack);

      // Assert - Should have only ONE lifecycle rule (not separate for errors)
      template.hasResourceProperties('AWS::S3::Bucket', {
        LifecycleConfiguration: {
          Rules: [
            {
              Id: 'expire-person-delta',
              Status: 'Enabled',
              Prefix: 'person-delta/',
              ExpirationInDays: 3
            }
          ]
        }
      });
    });

    it('should NOT create separate error rule when errorObjectLifetimeDays is undefined', () => {
      // Arrange
      mockContext = createMockContext([
        {
          path: 'uploads',
          objectLifetimeDays: 5,
          // errorObjectLifetimeDays not defined
          subscriberLambdaArn: 'arn:aws:lambda:us-east-2:123456789012:function:subscriber',
          subscriberLambdaExecutionRoleArn: 'arn:aws:iam::123456789012:role/subscriber-role'
        }
      ]);

      // Act
      new FileDropBucket(stack, 'TestBucket', { context: mockContext });
      const template = Template.fromStack(stack);

      // Assert - Should have only ONE lifecycle rule
      template.hasResourceProperties('AWS::S3::Bucket', {
        LifecycleConfiguration: {
          Rules: [
            {
              Id: 'expire-uploads',
              Status: 'Enabled',
              Prefix: 'uploads/',
              ExpirationInDays: 5
            }
          ]
        }
      });
    });

    it('should create lifecycle rules for multiple subdirectories with different configurations', () => {
      // Arrange
      mockContext = createMockContext([
        {
          path: 'person-full',
          objectLifetimeDays: 7,
          errorObjectLifetimeDays: 14,
          subscriberLambdaArn: 'arn:aws:lambda:us-east-2:123456789012:function:subscriber-full',
          subscriberLambdaExecutionRoleArn: 'arn:aws:iam::123456789012:role/subscriber-role'
        },
        {
          path: 'person-delta',
          objectLifetimeDays: 3,
          // No errorObjectLifetimeDays
          subscriberLambdaArn: 'arn:aws:lambda:us-east-2:123456789012:function:subscriber-delta',
          subscriberLambdaExecutionRoleArn: 'arn:aws:iam::123456789012:role/subscriber-role'
        }
      ]);

      // Act
      new FileDropBucket(stack, 'TestBucket', { context: mockContext });
      const template = Template.fromStack(stack);

      // Assert - Should have THREE lifecycle rules total
      // 1. person-full/ (7 days)
      // 2. person-full/errors/ (14 days)
      // 3. person-delta/ (3 days)
      template.hasResourceProperties('AWS::S3::Bucket', {
        LifecycleConfiguration: {
          Rules: [
            {
              Id: 'expire-person-full',
              Status: 'Enabled',
              Prefix: 'person-full/',
              ExpirationInDays: 7
            },
            {
              Id: 'expire-person-full-errors',
              Status: 'Enabled',
              Prefix: 'person-full/errors/',
              ExpirationInDays: 14
            },
            {
              Id: 'expire-person-delta',
              Status: 'Enabled',
              Prefix: 'person-delta/',
              ExpirationInDays: 3
            }
          ]
        }
      });
    });

    it('should apply more specific errors/ prefix rule before general subfolder rule', () => {
      // Arrange - This tests S3 lifecycle rule precedence behavior
      mockContext = createMockContext([
        {
          path: 'data',
          objectLifetimeDays: 10,
          errorObjectLifetimeDays: 30, // Longer retention for errors
          subscriberLambdaArn: 'arn:aws:lambda:us-east-2:123456789012:function:subscriber',
          subscriberLambdaExecutionRoleArn: 'arn:aws:iam::123456789012:role/subscriber-role'
        }
      ]);

      // Act
      new FileDropBucket(stack, 'TestBucket', { context: mockContext });
      const template = Template.fromStack(stack);

      // Assert
      // Files in data/ should expire after 10 days
      // Files in data/errors/ should expire after 30 days (more specific prefix wins)
      template.hasResourceProperties('AWS::S3::Bucket', {
        LifecycleConfiguration: {
          Rules: [
            {
              Id: 'expire-data',
              Status: 'Enabled',
              Prefix: 'data/',
              ExpirationInDays: 10
            },
            {
              Id: 'expire-data-errors',
              Status: 'Enabled',
              Prefix: 'data/errors/',
              ExpirationInDays: 30
            }
          ]
        }
      });
    });
  });
});
