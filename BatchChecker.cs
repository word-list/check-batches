using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.SQS;
using Amazon.SQS.Model;
using WordList.Common.Json;
using WordList.Common.Messages;
using WordList.Common.OpenAI;
using WordList.Processing.CheckBatches.Models;

namespace WordList.Processing.CheckBatches;

public class BatchChecker
{
    private static DynamoDBContext s_db = new DynamoDBContextBuilder().Build();
    private static AmazonSQSClient s_sqs = new();

    private SemaphoreSlim _batchCheckingLimiter = new(4);
    private SemaphoreSlim _messageSendingLimiter = new(4);

    private OpenAIClient _openAI;

    protected ILambdaLogger Logger { get; init; }

    protected string BatchesTableName { get; set; }

    public int TryCount { get; init; } = 3;
    public string TargetQueueUrl { get; init; }

    public BatchChecker(ILambdaLogger logger, string targetQueueUrl)
    {
        var apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY")
            ?? throw new Exception("OPENAI_API_KEY must be set");

        _openAI = new(apiKey);

        Logger = logger;
        BatchesTableName = Environment.GetEnvironmentVariable("BATCHES_TABLE_NAME")
            ?? throw new Exception("BATCHES_TABLE_NAME must be set");

        TargetQueueUrl = targetQueueUrl;
    }

    private async Task<Batch[]> GetAvailableBatchesAsync()
    {
        Logger.LogInformation("Retrieving batches to check...");

        var query = new QueryOperationConfig
        {
            IndexName = "StatusIndex",
            KeyExpression = new Expression
            {
                ExpressionStatement = "#batch_status = :status",
                ExpressionAttributeValues = new() { { ":status", "Waiting" } },
                ExpressionAttributeNames = new() { { "#batch_status", "status" } }
            }
        };

        var result = await s_db.FromQueryAsync<Batch>(query, new FromQueryConfig { OverrideTableName = BatchesTableName }).GetRemainingAsync().ConfigureAwait(false);

        Logger.LogInformation($"Retrieved {result.Count} waiting batches");

        return [.. result];
    }

    private static bool IsCompletedStatus(string status)
        => status.Equals("completed", StringComparison.CurrentCultureIgnoreCase)
        || status.Equals("failed", StringComparison.CurrentCultureIgnoreCase);

    private async Task<Batch?> ShouldUpdateBatchAsync(Batch batch)
    {
        Logger.LogInformation($"[{batch.Id}] Waiting to check batch");

        await _batchCheckingLimiter.WaitAsync().ConfigureAwait(false);
        try
        {
            Logger.LogInformation($"[{batch.Id}] Starting to check batch");

            if (batch.OpenAIBatchId is null)
            {
                Logger.LogWarning($"[{batch.Id}] Batch has no associated OpenAIBatchId, aborting");
                return null;
            }

            var openAIBatch = await _openAI.GetBatchStatusAsync(batch.OpenAIBatchId);
            if (openAIBatch is null)
            {
                Logger.LogWarning($"[{batch.Id}] Retrieved no data from OpenAI API, aborting");
                return null;
            }

            if (!IsCompletedStatus(openAIBatch.Status))
            {
                Logger.LogInformation($"[{batch.Id}] Batch status of '{openAIBatch.Status}' is not a completed state, skipping");
                return null;
            }

            Logger.LogInformation($"[{batch.Id}] Batch status of '{openAIBatch.Status}' is a completed state, returning");
            return batch;
        }
        finally
        {
            _batchCheckingLimiter.Release();
        }
    }

    private string? TrySerialize(UpdateBatchMessage message)
    {
        try
        {
            return JsonHelpers.Serialize(message, LambdaFunctionJsonSerializerContext.Default.UpdateBatchMessage);
        }
        catch (Exception ex)
        {
            Logger.LogError($"[{message.BatchId} Failed to serialise message: {ex.Message}");
            return null;
        }
    }

    private async Task SendUpdateMessageBatchAsync(UpdateBatchMessage[] messages)
    {
        Logger.LogInformation($"Waiting to send a batch of {messages.Length} update message(s)");
        await _messageSendingLimiter.WaitAsync().ConfigureAwait(false);
        try
        {
            Logger.LogInformation($"Sending a batch of {messages.Length} update message(s)");

            var entryMap = messages
                .Select(TrySerialize)
                .Where(text => text is not null)
                .Select(text => new SendMessageBatchRequestEntry(Guid.NewGuid().ToString(), text))
                .ToDictionary(entry => entry.Id);

            for (var tryNumber = 1; tryNumber <= TryCount && entryMap.Count != 0; tryNumber++)
            {
                var batchRequest = new SendMessageBatchRequest(TargetQueueUrl, [.. entryMap.Values]);

                if (tryNumber > 1) await Task.Delay(250);

                Logger.LogInformation($"Try number {tryNumber}: Sending batch of {batchRequest.Entries.Count} message(s)");
                try
                {
                    var response = await s_sqs.SendMessageBatchAsync(batchRequest).ConfigureAwait(false);
                    foreach (var message in response.Successful)
                    {
                        entryMap.Remove(message.Id);
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogError($"Failed to send message batch of {batchRequest.Entries.Count} message(s): {ex.Message}");
                }
            }

            if (entryMap.Count > 0)
            {
                Logger.LogError($"Failed to send message batch of {entryMap.Count} message(s) after {TryCount} attempt(s)");
            }
        }
        finally
        {
            _messageSendingLimiter.Release();
        }
    }

    public async Task CheckBatchesAsync()
    {
        var batchesToCheck = await GetAvailableBatchesAsync().ConfigureAwait(false);

        var checkBatchTasks = batchesToCheck.Select(ShouldUpdateBatchAsync);

        Logger.LogInformation("Waiting for all checks to complete");
        var results = await Task.WhenAll(checkBatchTasks).ConfigureAwait(false);
        Logger.LogInformation("All tasks completed");

        var batches = results
            .OfType<Batch>()
            .Select(batch => new UpdateBatchMessage { BatchId = batch.Id })
            .ToArray();

        Logger.LogInformation($"Retrieved {batches.Length} batch(es) to update");

        var sendMessageTasks = batches.Chunk(10).Select(SendUpdateMessageBatchAsync);

        Logger.LogInformation("Waiting for all update messages to send");
        await Task.WhenAll(sendMessageTasks);
        Logger.LogInformation("All update messages sent");
    }
}