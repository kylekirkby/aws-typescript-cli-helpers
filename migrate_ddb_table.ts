import {
  AttributeValue,
  BatchWriteItemCommand,
  BatchWriteItemCommandInput,
  DynamoDBClient,
  ScanCommand,
  ScanCommandInput,
} from "@aws-sdk/client-dynamodb";
import { fromIni } from "@aws-sdk/credential-providers";

/**
 * Migrates data from one DynamoDB table to another.
 */
const migrateTableData = async (
  sourceTableName: string,
  destinationTableName: string
): Promise<void> => {
  const docClient = new DynamoDBClient({
    region: "us-east-1",
    credentials: fromIni({ profile: "PROFILE_NAME" }),
  });

  // Scan the old table to retrieve all items
  const scanParams: ScanCommandInput = {
    TableName: sourceTableName,
  };

  const scanCommand: ScanCommand = new ScanCommand(scanParams);

  const items = [];
  let lastEvaluatedKey: Record<string, AttributeValue> | undefined;

  do {
    const result = await docClient.send(scanCommand);
    items.push(...(result.Items ?? []));
    lastEvaluatedKey = result.LastEvaluatedKey;
    scanParams.ExclusiveStartKey = lastEvaluatedKey;
  } while (lastEvaluatedKey);

  // Batch write the items to the new table
  const batches: BatchWriteItemCommandInput[] = [];
  for (let i = 0; i < items.length; i += 25) {
    batches.push({
      RequestItems: {
        [destinationTableName]: items.slice(i, i + 25).map((item) => ({
          PutRequest: {
            Item: item,
          },
        })),
      },
    });
  }

  for (const batchParams of batches) {
    const batchCommand = new BatchWriteItemCommand(batchParams);
    await docClient.send(batchCommand);
  }
};

const tableMap: Record<string, string> = {
  "User-xxxxxxxxx-develop": "User-yyyyyyyyy-dev",
  "Email-xxxxxxxxx-develop": "Email-yyyyyyyyy-dev",
  "Form-xxxxxxxxx-develop": "Form-yyyyyyyyy-dev",
  "Notification-xxxxxxxxx-develop": "Notification-yyyyyyyyy-dev",
  "Plan-xxxxxxxxx-develop": "Plan-yyyyyyyyy-dev",
};

for (const [sourceTableName, destinationTableName] of Object.entries(
  tableMap
)) {
  console.log(`Migrating ${sourceTableName} to ${destinationTableName}`);
  await migrateTableData(sourceTableName, destinationTableName);
}
