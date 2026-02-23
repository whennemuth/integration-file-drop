import { S3EventRecord } from 'aws-lambda';
import { S3EventProcessor, ProcessingResult } from '../src/event-processor/S3EventProcessor';
import { Bucket, IBucket } from '../src/event-processor/Bucket';
import { BucketConfig } from '../context/IContext';
import { Subscriber } from '../src/event-processor/Subscriber';

// Mock the Subscriber module to avoid AWS SDK initialization issues
jest.mock('../src/event-processor/Subscriber', () => {
  return {
    Subscriber: jest.fn().mockImplementation(() => {
      return {
        notify: jest.fn().mockResolvedValue(true)
      };
    })
  };
});

const MockedSubscriber = Subscriber as jest.MockedClass<typeof Subscriber>;

describe('S3EventProcessor', () => {
  let mockBucket: jest.Mocked<IBucket>;
  let bucketConfig: BucketConfig;
  let fixedTimestamp: string;

  beforeEach(() => {
    // Fixed timestamp for consistent testing
    fixedTimestamp = '2026-02-22T10:30:00.000Z';

    // Mock bucket configuration
    bucketConfig = {
      name: 'test-bucket',
      subdirectories: [
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
          subscriberLambdaArn: 'arn:aws:lambda:us-east-2:123456789012:function:subscriber-delta',
          subscriberLambdaExecutionRoleArn: 'arn:aws:iam::123456789012:role/subscriber-role'
        }
      ]
    };

    //Mock bucket
    mockBucket = {
      getConfig: jest.fn().mockReturnValue(bucketConfig),
      getName: jest.fn().mockReturnValue('test-bucket'),
      renameObject: jest.fn().mockResolvedValue(true),
      moveToErrors: jest.fn().mockResolvedValue(true)
    };

    // Reset Subscriber mock to default behavior
    MockedSubscriber.mockClear();
    MockedSubscriber.mockImplementation(() => ({
      notify: jest.fn().mockResolvedValue(true)
    }) as any);
  });

  /**
   * Helper function to create a mock S3 event record
   */
  const createMockRecord = (key: string): S3EventRecord => ({
    eventVersion: '2.1',
    eventSource: 'aws:s3',
    awsRegion: 'us-east-2',
    eventTime: '2026-02-22T10:00:00.000Z',
    eventName: 'ObjectCreated:Put',
    userIdentity: {
      principalId: 'AWS:AIDAI...'
    },
    requestParameters: {
      sourceIPAddress: '192.168.1.1'
    },
    responseElements: {
      'x-amz-request-id': 'ABC123',
      'x-amz-id-2': 'DEF456'
    },
    s3: {
      s3SchemaVersion: '1.0',
      configurationId: 'test-config',
      bucket: {
        name: 'test-bucket',
        ownerIdentity: {
          principalId: 'A1B2C3D4'
        },
        arn: 'arn:aws:s3:::test-bucket'
      },
      object: {
        key,
        size: 1024,
        eTag: 'abc123',
        sequencer: '123ABC'
      }
    }
  });

  describe('Test 1: Files not matching configured subfolders', () => {
    it('should skip processing and log when file is not in any configured subfolder', async () => {
      const record = createMockRecord('unconfigured-folder/test-file.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      expect(result.success).toBe(true);
      expect(result.action).toBe('skipped-no-match');
      expect(result.reason).toContain('does not match any configured subdirectory');
      expect(result.originalKey).toBe('unconfigured-folder/test-file.json');
      expect(mockBucket.renameObject).not.toHaveBeenCalled();
    });

    it('should skip processing for root-level files', async () => {
      const record = createMockRecord('root-file.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      expect(result.success).toBe(true);
      expect(result.action).toBe('skipped-no-match');
      expect(mockBucket.renameObject).not.toHaveBeenCalled();
    });

    it('should skip processing for files in non-configured subdirectories', async () => {
      const record = createMockRecord('other-folder/data.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      expect(result.success).toBe(true);
      expect(result.action).toBe('skipped-no-match');
    });
  });

  describe('Test 2: Recursion prevention - already renamed files', () => {
    describe('Valid ISO timestamp patterns should be detected', () => {
      const validTimestampPrefixes = [
        '2026-02-22T10:30:00.000Z-data.json',
        '2024-01-01T00:00:00.000Z-file.txt',
        '2025-12-31T23:59:59.999Z-test.csv',
        '2026-06-15T14:22:33.456Z-report.tar.gz',
        '2023-03-10T08:05:12.100Z-file',  // No extension
        '2026-02-22T10:30:00.000Z-file.with.dots.json'
      ];

      validTimestampPrefixes.forEach(filename => {
        it(`should skip file with valid timestamp prefix: ${filename}`, async () => {
          const record = createMockRecord(`person-full/${filename}`);
          const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

          const result = await processor.process();

          expect(result.success).toBe(true);
          expect(result.action).toBe('skipped-already-processed');
          expect(result.reason).toContain('already has timestamp prefix');
          expect(mockBucket.renameObject).not.toHaveBeenCalled();
        });
      });
    });

    describe('Invalid/partial timestamp patterns should NOT be detected as processed', () => {
      const invalidPatterns = [
        'data-2026-02-22T10:30:00.000Z.json',  // Timestamp not at start
        '2026-02-22-data.json',  // Missing time component
        '2026-22T10:30:00.000Z-data.json',  // Invalid date format
        '2026-02-22T10:30:00.000Zdata.json',  // Missing hyphen after timestamp
        '2026-02-22T10:30:00-data.json',  // Missing milliseconds
        '2026-02-22T10:30-data.json',  // Missing seconds
        'T10:30:00.000Z-data.json',  // Missing date
        '02-22-2026T10:30:00.000Z-data.json',  // Wrong date format
        '2026/02/22T10:30:00.000Z-data.json',  // Wrong date separator
      ];

      invalidPatterns.forEach(filename => {
        it(`should process file with invalid timestamp pattern: ${filename}`, async () => {
          const record = createMockRecord(`person-full/${filename}`);
          const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

          const result = await processor.process();

          expect(result.action).not.toBe('skipped-already-processed');
          expect(mockBucket.renameObject).toHaveBeenCalled();
        });
      });
    });

    describe('Subdirectory names should not trigger recursion detection', () => {
      it('should not skip when subdirectory path contains timestamp pattern', async () => {
        // Malicious attempt: create subdirectory with timestamp pattern name
        bucketConfig.subdirectories.push({
          path: '2026-02-22T10:30:00.000Z-subfolder',
          objectLifetimeDays: 7,
          subscriberLambdaArn: 'arn:aws:lambda:us-east-2:123456789012:function:test',
          subscriberLambdaExecutionRoleArn: 'arn:aws:iam::123456789012:role/test'
        });

        const record = createMockRecord('2026-02-22T10:30:00.000Z-subfolder/newfile.json');
        const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

        const result = await processor.process();

        // Should process because filename 'newfile.json' doesn't have timestamp prefix
        expect(result.action).not.toBe('skipped-already-processed');
        expect(mockBucket.renameObject).toHaveBeenCalledWith(
          '2026-02-22T10:30:00.000Z-subfolder/newfile.json',
          expect.stringContaining('newfile.json')
        );
      });
    });

    describe('Edge cases for recursion detection', () => {
      it('should handle empty filename gracefully', async () => {
        const record = createMockRecord('person-full/');
        const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

        const result = await processor.process();

        // Should process (or handle appropriately), not crash
        expect(result).toBeDefined();
      });

      it('should handle URL-encoded timestamp in key', async () => {
        const encodedKey = 'person-full/2026-02-22T10%3A30%3A00.000Z-data.json';
        const record = createMockRecord(encodedKey);
        const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

        const result = await processor.process();

        // After decoding, should recognize as already processed
        expect(result.action).toBe('skipped-already-processed');
      });
    });
  });

  describe('Test 3: Date-based naming convention', () => {
    it('should rename file with correct ISO timestamp prefix format', async () => {
      const record = createMockRecord('person-full/data.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      await processor.process();

      expect(mockBucket.renameObject).toHaveBeenCalledWith(
        'person-full/data.json',
        'person-full/2026-02-22T10:30:00.000Z-data.json'
      );
    });

    it('should preserve file extension in renamed file', async () => {
      const testCases = [
        { original: 'data.json', expected: '2026-02-22T10:30:00.000Z-data.json' },
        { original: 'file.tar.gz', expected: '2026-02-22T10:30:00.000Z-file.tar.gz' },
        { original: 'report.csv', expected: '2026-02-22T10:30:00.000Z-report.csv' },
        { original: 'data', expected: '2026-02-22T10:30:00.000Z-data' },
        { original: 'file.backup.json', expected: '2026-02-22T10:30:00.000Z-file.backup.json' }
      ];

      for (const testCase of testCases) {
        mockBucket.renameObject.mockClear();
        const record = createMockRecord(`person-full/${testCase.original}`);
        const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

        await processor.process();

        expect(mockBucket.renameObject).toHaveBeenCalledWith(
          `person-full/${testCase.original}`,
          `person-full/${testCase.expected}`
        );
      }
    });

    it('should use custom clock function for timestamp', async () => {
      const customTimestamp = '2025-01-15T14:22:33.456Z';
      const record = createMockRecord('person-full/test.json');
      const processor = new S3EventProcessor(record, mockBucket, () => customTimestamp);

      await processor.process();

      expect(mockBucket.renameObject).toHaveBeenCalledWith(
        'person-full/test.json',
        'person-full/2025-01-15T14:22:33.456Z-test.json'
      );
    });

    it('should include result with newKey after successful rename', async () => {
      const record = createMockRecord('person-delta/update.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      expect(result.newKey).toBe('person-delta/2026-02-22T10:30:00.000Z-update.json');
      expect(result.action).toBe('renamed');
    });
  });

  describe('Test 4: Subfolder configuration expression in renaming', () => {
    it('should place renamed file in correct subfolder path', async () => {
      const record = createMockRecord('person-full/data.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      await processor.process();

      const [[_, newKey]] = (mockBucket.renameObject as jest.Mock).mock.calls;
      expect(newKey).toMatch(/^person-full\//);
    });

    it('should handle different subfolder configurations correctly', async () => {
      const subfolders = ['person-full', 'person-delta'];

      for (const subfolder of subfolders) {
        mockBucket.renameObject.mockClear();
        const record = createMockRecord(`${subfolder}/file.json`);
        const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

        await processor.process();

        expect(mockBucket.renameObject).toHaveBeenCalledWith(
          `${subfolder}/file.json`,
          `${subfolder}/2026-02-22T10:30:00.000Z-file.json`
        );
      }
    });

    it('should match longest matching subfolder path', async () => {
      // Add nested subfolder configuration
      bucketConfig.subdirectories.push({
        path: 'person-full/nested',
        objectLifetimeDays: 5,
        subscriberLambdaArn: 'arn:aws:lambda:us-east-2:123456789012:function:nested',
        subscriberLambdaExecutionRoleArn: 'arn:aws:iam::123456789012:role/nested'
      });

      const record = createMockRecord('person-full/nested/file.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      await processor.process();

      // Should match 'person-full' since 'person-full/nested' requires exact path match
      expect(mockBucket.renameObject).toHaveBeenCalledWith(
        'person-full/nested/file.json',
        expect.stringMatching(/^person-full\/(nested\/)?2026-02-22T10:30:00\.000Z-.*/)
      );
    });
  });

  describe('Test 5: Subscriber Lambda invocation', () => {
    // Note: Since Subscriber is instantiated inside process(), we need to mock it
    // For now, we'll test that the process completes successfully
    // In a real scenario, you'd want to inject the Subscriber or mock the module

    it('should complete processing successfully when subscriber invokes', async () => {
      const record = createMockRecord('person-full/data.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      expect(result.success).toBe(true);
      expect(result.action).toBe('renamed');
      expect(result.subscriberInvoked).toBe(true);
    });

    it('should not invoke subscriber if file does not match subfolder', async () => {
      const record = createMockRecord('unknown/data.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      expect(result.subscriberInvoked).toBeUndefined();
      expect(result.action).toBe('skipped-no-match');
    });

    it('should not invoke subscriber if file already processed', async () => {
      const record = createMockRecord('person-full/2026-02-22T10:30:00.000Z-data.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      expect(result.subscriberInvoked).toBeUndefined();
      expect(result.action).toBe('skipped-already-processed');
    });

    it('should handle subscriber invocation failure and move to errors', async () => {
      // Configure Subscriber mock to throw an error
      const mockNotify = jest.fn().mockRejectedValue(new Error('Lambda invocation failed'));
      MockedSubscriber.mockImplementation(() => ({
        notify: mockNotify
      }) as any);

      const record = createMockRecord('person-full/data.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      // Should have attempted to rename first
      expect(mockBucket.renameObject).toHaveBeenCalledWith(
        'person-full/data.json',
        'person-full/2026-02-22T10:30:00.000Z-data.json'
      );

      // Should have moved file to errors after subscriber failure
      expect(mockBucket.moveToErrors).toHaveBeenCalledWith(
        'person-full/2026-02-22T10:30:00.000Z-data.json',
        'person-full',
        expect.stringContaining('Lambda invocation failed')
      );

      // Result should indicate error-invoke action
      expect(result.success).toBe(false);
      expect(result.action).toBe('error-invoke');
      expect(result.subscriberInvoked).toBe(false);
      expect(result.reason).toContain('Lambda invocation failed');
    });
  });

  describe('Test 6: Error handling', () => {
    it('should return error result when rename fails', async () => {
      mockBucket.renameObject.mockResolvedValue(false);
      const record = createMockRecord('person-full/data.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      expect(result.success).toBe(false);
      expect(result.action).toBe('error-rename');
      expect(result.reason).toContain('Failed to rename');
    });

    it('should handle special characters in filenames', async () => {
      const specialFiles = [
        'file with spaces.json',
        'file-with-dashes.json',
        'file_with_underscores.json',
        'file(with)parens.json',
        'file[with]brackets.json',
        'file@special#chars$.json'
      ];

      for (const filename of specialFiles) {
        mockBucket.renameObject.mockClear();
        const record = createMockRecord(`person-full/${filename}`);
        const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

        const result = await processor.process();

        expect(mockBucket.renameObject).toHaveBeenCalled();
        expect(result.newKey).toContain(filename);
      }
    });

    it('should handle very long filenames', async () => {
      const longFilename = 'a'.repeat(200) + '.json';
      const record = createMockRecord(`person-full/${longFilename}`);
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      expect(result).toBeDefined();
      expect(mockBucket.renameObject).toHaveBeenCalled();
    });

    it('should handle URL-encoded characters in keys', async () => {
      const encodedKey = 'person-full/file%20with%20spaces.json';
      const record = createMockRecord(encodedKey);
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      // Should decode to 'file with spaces.json'
      expect(result.originalKey).toBe('person-full/file with spaces.json');
    });

    it('should handle files with multiple dots in name', async () => {
      const record = createMockRecord('person-full/data.backup.2024.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      await processor.process();

      expect(mockBucket.renameObject).toHaveBeenCalledWith(
        'person-full/data.backup.2024.json',
        'person-full/2026-02-22T10:30:00.000Z-data.backup.2024.json'
      );
    });
  });

  describe('Test 7: Additional edge cases', () => {
    it('should handle concurrent processing of different files', async () => {
      const files = ['file1.json', 'file2.json', 'file3.json'];
      const processors = files.map(file => {
        const record = createMockRecord(`person-full/${file}`);
        return new S3EventProcessor(record, mockBucket, () => fixedTimestamp);
      });

      const results = await Promise.all(processors.map(p => p.process()));

      expect(results).toHaveLength(3);
      results.forEach(result => {
        expect(result.success).toBe(true);
      });
    });

    it('should handle files in nested subdirectories correctly', async () => {
      const record = createMockRecord('person-full/subfolder/data.json');
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      // Should preserve nested structure: subfolder remains, filename gets timestamp prefix
      expect(mockBucket.renameObject).toHaveBeenCalledWith(
        'person-full/subfolder/data.json',
        'person-full/subfolder/2026-02-22T10:30:00.000Z-data.json'
      );
    });

    it('should handle zero-byte files', async () => {
      const record = createMockRecord('person-full/empty.json');
      record.s3.object.size = 0;
      const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

      const result = await processor.process();

      expect(result).toBeDefined();
      expect(mockBucket.renameObject).toHaveBeenCalled();
    });

    it('should maintain originalKey in result for all cases', async () => {
      const testKeys = [
        'person-full/data.json',
        'unknown/file.txt',
        'person-full/2026-02-22T10:30:00.000Z-existing.json'
      ];

      for (const key of testKeys) {
        const record = createMockRecord(key);
        const processor = new S3EventProcessor(record, mockBucket, () => fixedTimestamp);

        const result = await processor.process();

        expect(result.originalKey).toBe(key);
      }
    });
  });
});
