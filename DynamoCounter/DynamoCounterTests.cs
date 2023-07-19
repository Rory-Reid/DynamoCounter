using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using DynamoCounter.Tests;

namespace DynamoCounter;

[Collection("dynamo")]
public class DynamoCounterExamples
{
    private readonly IAmazonDynamoDB dynamo = new AmazonDynamoDBClient(
        new BasicAWSCredentials("_", "_"),
        new AmazonDynamoDBConfig { ServiceURL = "http://localhost:8000" });

    public DynamoCounterExamples(DynamoFixture fixture)
    {
        fixture.KillContainerAfterTests = true; // Flip this if you want to inspect the database after the tests run
    }
    
    [Fact]
    public async Task Atomic_counter()
    {
        var tableName = await this.CreateTable(nameof(this.Atomic_counter));
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = "counter"},
                ["count_value"] = new() {N = "0"}
            }
        });

        var count = await this.dynamo.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
            UpdateExpression = "ADD #count :increment",
            ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> {[":increment"] = new() {N = "1"}},
            ReturnValues = ReturnValue.ALL_NEW
        });

        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = count.Attributes["count_value"].N},
                ["some_attribute"] = new() {S = "This item guaranteed to have unique PK! Woohoo!"}
            }
        });
    }

    [Fact]
    public async Task Atomic_counter_concurrent_updates()
    {
        var tableName = await this.CreateTable(nameof(this.Atomic_counter_concurrent_updates));
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = "counter"},
                ["count_value"] = new() {N = "0"}
            }
        });

        // Operation 1 starts, increments and gets the counter value
        var op1Count = await this.dynamo.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
            UpdateExpression = "ADD #count :increment",
            ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> {[":increment"] = new() {N = "1"}},
            ReturnValues = ReturnValue.ALL_NEW
        });

        // Operation 2 begins and ends, incrementing the counter and inserting a record
        // Note that the returned value will be different to the value operation 1 has
        var op2Count = await this.dynamo.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
            UpdateExpression = "ADD #count :increment",
            ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> {[":increment"] = new() {N = "1"}},
            ReturnValues = ReturnValue.ALL_NEW
        });
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = op2Count.Attributes["count_value"].N},
                ["some_attribute"] = new() {S = "This item was inserted by the second operation"}
            }
        });

        // Operation 1 completes, inserting the record with the original counter value
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = op1Count.Attributes["count_value"].N},
                ["some_attribute"] = new() {S = "This item was inserted by the first operation"}
            }
        });
    }

    [Fact]
    public async Task Optimistic_locking()
    {
        var tableName = await this.CreateTable(nameof(this.Optimistic_locking));
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = "counter"},
                ["count_value"] = new() {N = "1"}
            }
        });

        var currentCountItem = await this.dynamo.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new("counter")},
            AttributesToGet = new List<string> {"count_value"}
        });
        var currentCount = currentCountItem.Item["count_value"].N;

        await this.dynamo.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
            UpdateExpression = "ADD #count :increment",
            ConditionExpression = "#count = :current_count",
            ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":increment"] = new() {N = "1"},
                [":current_count"] = new() {N = currentCount}
            },
        });

        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = currentCount},
                ["some_attribute"] = new() {S = "This item guaranteed to have unique PK! Woohoo!"}
            }
        });
    }
    
    /// <summary>
    /// Essentially the same as <see cref="Optimistic_locking"/> but using a Put request vs an Update request to
    /// conditionally update the counter before inserting a new item.
    /// </summary>
    [Fact]
    public async Task Optimistic_locking_with_put_counter()
    {
        var tableName = await this.CreateTable(nameof(this.Optimistic_locking_with_put_counter));
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = "counter"},
                ["count_value"] = new() {N = "1"}
            }
        });

        var currentCountItem = await this.dynamo.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new("counter")},
            AttributesToGet = new List<string> {"count_value"}
        });
        var currentCount = currentCountItem.Item["count_value"].N;
        var currentCountNumber = int.Parse(currentCount);

        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = "counter"},
                ["count_value"] = new() {N = (currentCountNumber + 1).ToString()}
            },
            ConditionExpression = "#count = :currentCount", // Fails if someone else has updated count and "locked" the value of count
            ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> {[":currentCount"] = new() {N = currentCount}}
        });

        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = currentCount},
                ["some_attribute"] = new() {S = "This item guaranteed to have unique PK! Woohoo!"}
            }
        });
    }
    
    [Fact]
    public async Task Optimistic_locking_concurrent_updates()
    {
        var tableName = await this.CreateTable(nameof(this.Optimistic_locking_concurrent_updates));
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = "counter"},
                ["count_value"] = new() {N = "1"}
            }
        });

        // Operation 1 starts, increments and gets the counter value
        var op1CountItem = await this.dynamo.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new("counter")},
            AttributesToGet = new List<string> {"count_value"}
        });
        var op1Count = op1CountItem.Item["count_value"].N;
        var op1CountNumber = int.Parse(op1Count);

        // Operation 2 begins and ends, incrementing the counter and inserting a record.
        // Note that the returned value will be the same as operation 1, clashing, but because it writes first it wins
        var op2CountItem = await this.dynamo.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new("counter")},
            AttributesToGet = new List<string> {"count_value"}
        });
        var op2Count = op2CountItem.Item["count_value"].N;
        var op2CountNumber = int.Parse(op2Count);
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = "counter"},
                ["count_value"] = new() {N = (op2CountNumber + 1).ToString()}
            },
            ConditionExpression = "#count = :currentCount",
            ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> {[":currentCount"] = new() {N = op2Count}}
        });
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = op1Count},
                ["some_attribute"] = new() {S = "This item was inserted by the second operation"}
            }
        });
        
        // Operation 1 tries the first request - but it throws an exception!
        try
        {
            await this.dynamo.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["pk"] = new() {S = "counter"},
                    ["count_value"] = new() {N = (op1CountNumber + 1).ToString()}
                },
                ConditionExpression = "#count = :currentCount",
                ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {[":currentCount"] = new() {N = op1Count}}
            });
        }
        catch (ConditionalCheckFailedException)
        {
            // So, operation 2 swooped in and stole our ID. We have to scrap the current value and retry everything.
        }

        // Operation 1 retries everything, getting the count again and hoping for another optimistic lock on the value 
        op1CountItem = await this.dynamo.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new("counter")},
            AttributesToGet = new List<string> {"count_value"}
        });
        op1Count = op1CountItem.Item["count_value"].N;
        op1CountNumber = int.Parse(op1Count);
        
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = "counter"},
                ["count_value"] = new() {N = (op1CountNumber + 1).ToString()}
            },
            ConditionExpression = "#count = :currentCount",
            ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> {[":currentCount"] = new() {N = op1Count}}
        });

        // Because the above request is successful, we have exclusive use of the value we set! Let's use it.
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = op1Count},
                ["some_attribute"] = new() {S = "This item was inserted by the first operation"}
            }
        });
    }
    
    [Fact]
    public async Task Transactional_optimistic_locking()
    {
        var tableName = await this.CreateTable(nameof(this.Transactional_optimistic_locking));
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = "counter"},
                ["count_value"] = new() {N = "1"}
            }
        });

        var currentCountItem = await this.dynamo.GetItemAsync(new GetItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
                AttributesToGet = new List<string> {"count_value"}
            });
        var currentCount = currentCountItem.Item["count_value"].N;

        await this.dynamo.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems = new List<TransactWriteItem>
            {
                new()
                {
                    Update = new Update
                    {
                        Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
                        TableName = tableName,
                        UpdateExpression = "ADD #count :increment",
                        ConditionExpression = "#count = :currentCount",
                        ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            [":increment"] = new() {N = "1"},
                            [":currentCount"] = new() {N = currentCount}
                        }
                    }
                },
                new()
                {
                    Put = new Put
                    {
                        TableName = tableName,
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["pk"] = new() {S = currentCount},
                            ["some_attribute"] = new() {S = "This item guaranteed to have unique PK! Woohoo!"}
                        }
                    }
                }
            }
        });
    }
    
    [Fact]
    public async Task Transactional_optimistic_locking_concurrent_updates()
    {
        var tableName = await this.CreateTable(nameof(this.Transactional_optimistic_locking_concurrent_updates));
        await this.dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() {S = "counter"},
                ["count_value"] = new() {N = "1"}
            }
        });

        // Operation 1 begins, gets the count
        var op1CountItem = await this.dynamo.GetItemAsync(new GetItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
                AttributesToGet = new List<string> {"count_value"}
            });
        var op1Count = op1CountItem.Item["count_value"].N;

        // Operation 2 begins and ends, getting the count and executing the write
        var op2CountItem = await this.dynamo.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
            AttributesToGet = new List<string> {"count_value"}
        });
        var op2Count = op2CountItem.Item["count_value"].N;
        await this.dynamo.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems = new List<TransactWriteItem>
            {
                new()
                {
                    Update = new Update
                    {
                        Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
                        TableName = tableName,
                        UpdateExpression = "ADD #count :increment",
                        ConditionExpression = "#count = :currentCount",
                        ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            [":increment"] = new() {N = "1"},
                            [":currentCount"] = new() {N = op2Count}
                        }
                    }
                },
                new()
                {
                    Put = new Put
                    {
                        TableName = tableName,
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["pk"] = new() {S = op2Count},
                            ["some_attribute"] = new() {S = "This item was inserted by the second operation"}
                        }
                    }
                }
            }
        });
        
        // Operation 1 tries to write - and fails!
        try
        {
            await this.dynamo.TransactWriteItemsAsync(new TransactWriteItemsRequest
            {
                TransactItems = new List<TransactWriteItem>
                {
                    new()
                    {
                        Update = new Update
                        {
                            Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
                            TableName = tableName,
                            UpdateExpression = "ADD #count :increment",
                            ConditionExpression = "#count = :currentCount",
                            ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                [":increment"] = new() {N = "1"},
                                [":currentCount"] = new() {N = op1Count}
                            }
                        }
                    },
                    new()
                    {
                        Put = new Put
                        {
                            TableName = tableName,
                            Item = new Dictionary<string, AttributeValue>
                            {
                                ["pk"] = new() {S = op1Count},
                                ["some_attribute"] = new() {S = "This item was inserted by the first operation"}
                            }
                        }
                    }
                }
            });
        }
        catch (TransactionCanceledException e) when (e.CancellationReasons[0].Code is "ConditionalCheckFailed")
        {
            // The first write failed because the number clashed. We'll have to get again and retry
        }
        
        // Operation 1 retries everything, getting the count again and hoping for another optimistic lock on the value
        op1CountItem = await this.dynamo.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
            AttributesToGet = new List<string> {"count_value"}
        });
        op1Count = op1CountItem.Item["count_value"].N;
        await this.dynamo.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems = new List<TransactWriteItem>
            {
                new()
                {
                    Update = new Update
                    {
                        Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = "counter"}},
                        TableName = tableName,
                        UpdateExpression = "ADD #count :increment",
                        ConditionExpression = "#count = :currentCount",
                        ExpressionAttributeNames = new Dictionary<string, string> {["#count"] = "count_value"},
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            [":increment"] = new() {N = "1"},
                            [":currentCount"] = new() {N = op1Count}
                        }
                    }
                },
                new()
                {
                    Put = new Put
                    {
                        TableName = tableName,
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["pk"] = new() {S = op1Count},
                            ["some_attribute"] = new() {S = "This item was inserted by the first operation"}
                        }
                    }
                }
            }
        });

        Assert.NotEqual(op1Count, op2Count);
        await this.AssertExists(tableName, op1Count, "some_attribute", "This item was inserted by the first operation");
        await this.AssertExists(tableName, op2Count, "some_attribute", "This item was inserted by the second operation");
    }

    private async Task<string> CreateTable(string? name = null)
    {
        name ??= Guid.NewGuid().ToString("N");
        await this.dynamo.CreateTableAsync(new CreateTableRequest
        {
            TableName = name,
            KeySchema = new List<KeySchemaElement> { new("pk", KeyType.HASH) },
            AttributeDefinitions = new List<AttributeDefinition> { new("pk", ScalarAttributeType.S) },
            BillingMode = BillingMode.PAY_PER_REQUEST
        });

        return name;
    }

    private async Task AssertExists(string tableName, string id, string attributeName, string attributeValue)
    {
        var item = await this.dynamo.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> {["pk"] = new() {S = id}},
            AttributesToGet = new List<string> {attributeName},
            ConsistentRead = true
        });
        Assert.Equal(attributeValue, item.Item[attributeName].S);
    }
}