using Amazon.DynamoDBv2.DataModel;

namespace WordList.Processing.CheckBatches.Models;

[DynamoDBTable("batches")]
public class Batch
{
    [DynamoDBHashKey("id")]
    public string Id { get; set; }

    [DynamoDBProperty("openai_batch_id")]
    public string? OpenAIBatchId { get; set; }

    [DynamoDBProperty("status")]
    public string Status { get; set; } = "Unknown";

    [DynamoDBProperty("created_at")]
    public long CreatedAt { get; set; } = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds();

    [DynamoDBProperty("error_message")]
    public string? ErrorMessage { get; set; }

    [DynamoDBProperty("correlation_id")]
    public string? CorrelationId { get; set; }
}

