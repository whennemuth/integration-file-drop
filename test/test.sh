#!/bin/bash

# Test script for S3 file-drop bucket
# Uploads test files to various locations to verify event processor behavior
# Usage: ./test.sh <scenario>
# Example: ./test.sh scenario1

BUCKET_NAME="huron-person-file-drop-dev"
REGION="us-east-2"

# Check if scenario argument is provided
if [ $# -eq 0 ]; then
  echo "=========================================="
  echo "S3 File Drop Bucket Test Scenarios"
  echo "=========================================="
  echo ""
  echo "Usage: ./test.sh <scenario>"
  echo ""
  echo "Available scenarios:"
  echo "  scenario1 - Upload JSON to bucket root (no processing expected)"
  echo "  scenario2 - Upload valid person data to person-full/ (triggers Lambda)"
  echo "  scenario3 - Upload valid delta data to person-delta/ (triggers Lambda)"
  echo ""
  echo "Example: ./test.sh scenario2"
  echo ""
  echo "Bucket: $BUCKET_NAME"
  echo "Region: $REGION"
  echo "=========================================="
  exit 1
fi

SCENARIO=$1

echo "=========================================="
echo "S3 File Drop Bucket Test Scenarios"
echo "Bucket: $BUCKET_NAME"
echo "Region: $REGION"
echo "Scenario: $SCENARIO"
echo "=========================================="
echo ""

case $SCENARIO in
  scenario1)
    # Scenario 1: Upload to bucket root (should NOT trigger event processor)
    echo "Scenario 1: Upload JSON to bucket root (no processing expected)"
    echo "---"
    echo '{"message":"Test file in root","timestamp":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}' | \
      aws s3 cp - "s3://$BUCKET_NAME/test-root-file.json" \
      --region "$REGION" \
      --content-type "application/json"

    if [ $? -eq 0 ]; then
      echo "✓ Successfully uploaded to root"
      echo "  Note: This file should NOT trigger any Lambda processing"
    else
      echo "✗ Failed to upload"
    fi
    ;;

  scenario2)
    # Scenario 2: Upload valid person data to person-full/ (should trigger subscriber Lambda)
    echo "Scenario 2: Upload valid person data to person-full/ (should trigger Lambda)"
    echo "---"
    cat <<'EOF' | aws s3 cp - "s3://$BUCKET_NAME/person-full/test-valid-person-data.json" \
      --region "$REGION" \
      --content-type "application/json"
{
  "requestId": "test-12345",
  "timestamp": "2026-02-20T12:00:00Z",
  "recordCount": 3,
  "rawData": [
    {
      "personid": "U12345678",
      "personBasic": {
        "firstName": "John",
        "lastName": "Doe",
        "email": "john.doe@example.edu"
      },
      "personAffiliation": {
        "primaryAffiliation": "EMPLOYEE",
        "department": "Computer Science"
      }
    },
    {
      "personid": "U87654321",
      "personBasic": {
        "firstName": "Jane",
        "lastName": "Smith",
        "email": "jane.smith@example.edu"
      },
      "personAffiliation": {
        "primaryAffiliation": "STUDENT",
        "department": "Engineering"
      }
    },
    {
      "personid": "U11223344",
      "personBasic": {
        "firstName": "Bob",
        "lastName": "Johnson",
        "email": "bob.johnson@example.edu"
      },
      "personAffiliation": {
        "primaryAffiliation": "FACULTY",
        "department": "Mathematics"
      }
    }
  ]
}
EOF

    if [ $? -eq 0 ]; then
      echo "✓ Successfully uploaded to person-full/"
      echo "  Expected outcome:"
      echo "    • Event processor validates JSON"
      echo "    • File renamed to full-{timestamp}.json"
      echo "    • Expiration tag set (7 days)"
      echo "    • Subscriber Lambda invoked with S3 path"
    else
      echo "✗ Failed to upload"
    fi
    ;;

  scenario3)
    # Scenario 3: Upload to person-delta/ with valid JSON
    echo "Scenario 3: Upload valid delta data to person-delta/"
    echo "---"
    cat <<'EOF' | aws s3 cp - "s3://$BUCKET_NAME/person-delta/test-delta-data.json" \
      --region "$REGION" \
      --content-type "application/json"
{
  "requestId": "delta-98765",
  "timestamp": "2026-02-20T12:30:00Z",
  "recordCount": 1,
  "deltaType": "UPDATE",
  "rawData": [
    {
      "personid": "U12345678",
      "personBasic": {
        "firstName": "John",
        "lastName": "Doe-Updated",
        "email": "john.doe.new@example.edu"
      },
      "personAffiliation": {
        "primaryAffiliation": "EMPLOYEE",
        "department": "Data Science"
      }
    }
  ]
}
EOF

    if [ $? -eq 0 ]; then
      echo "✓ Successfully uploaded to person-delta/"
      echo "  Expected outcome:"
      echo "    • Event processor validates JSON"
      echo "    • File renamed to delta-{timestamp}.json"
      echo "    • Expiration tag set (3 days)"
      echo "    • Subscriber Lambda invoked with S3 path"
    else
      echo "✗ Failed to upload"
    fi
    ;;

  *)
    echo "Error: Unknown scenario '$SCENARIO'"
    echo ""
    echo "Available scenarios:"
    echo "  scenario1 - Upload JSON to bucket root"
    echo "  scenario2 - Upload valid person data to person-full/"
    echo "  scenario3 - Upload valid delta data to person-delta/"
    echo ""
    echo "Example: ./test.sh scenario2"
    exit 1
    ;;
esac

echo ""
echo "=========================================="
echo "Test complete!"
echo ""
echo "To verify results:"
echo "  1. Check CloudWatch Logs for event processor Lambda"
echo "  2. Check CloudWatch Logs for subscriber Lambda (if LAMBDA.subscriberForTesting is configured)"
echo "  3. List bucket contents:"
echo "     aws s3 ls s3://$BUCKET_NAME/ --recursive --region $REGION"
echo "  4. Check for error files:"
echo "     aws s3 ls s3://$BUCKET_NAME/person-full/errors/ --region $REGION"
echo "     aws s3 ls s3://$BUCKET_NAME/person-delta/errors/ --region $REGION"
echo ""
echo "To clean up test files:"
echo "  aws s3 rm s3://$BUCKET_NAME/test-root-file.json --region $REGION"
echo "  aws s3 rm s3://$BUCKET_NAME/person-full/ --recursive --region $REGION"
echo "  aws s3 rm s3://$BUCKET_NAME/person-delta/ --recursive --region $REGION"
echo "=========================================="
