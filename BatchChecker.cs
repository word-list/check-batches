using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.SQS;
using Amazon.SQS.Model;
using WordList.Common.Json;
using WordList.Common.Logging;
using WordList.Common.Messaging;
using WordList.Common.Messaging.Messages;
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

    protected ILogger Log { get; init; }

    protected string BatchesTableName { get; set; }

    public int TryCount { get; init; } = 3;

    public BatchChecker(ILogger logger)
    {
        var apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY")
            ?? throw new Exception("OPENAI_API_KEY must be set");

        _openAI = new(apiKey);

        Log = logger;
        BatchesTableName = Environment.GetEnvironmentVariable("BATCHES_TABLE_NAME")
            ?? throw new Exception("BATCHES_TABLE_NAME must be set");
    }

    private async Task<Batch[]> GetAvailableBatchesAsync()
    {
        Log.Info("Retrieving batches to check...");

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

        var result = await s_db
            .FromQueryAsync<Batch>(query, new FromQueryConfig { OverrideTableName = BatchesTableName })
            .GetRemainingAsync()
            .ConfigureAwait(false);

        Log.Info($"Retrieved {result.Count} waiting batches");

        return [.. result];
    }

    private static bool IsCompletedStatus(string status)
        => status.Equals("completed", StringComparison.CurrentCultureIgnoreCase)
        || status.Equals("failed", StringComparison.CurrentCultureIgnoreCase);

    private async Task<Batch?> ShouldUpdateBatchAsync(Batch batch)
    {
        var log = Log.WithPrefix($"[{batch.Id}] ");

        log.Info($"Waiting to check batch");

        await _batchCheckingLimiter.WaitAsync().ConfigureAwait(false);
        try
        {
            log.Info($"Starting to check batch");

            if (batch.OpenAIBatchId is null)
            {
                log.Warning($"Batch has no associated OpenAIBatchId, aborting");
                return null;
            }

            var openAIBatch = await _openAI.GetBatchStatusAsync(batch.OpenAIBatchId);
            if (openAIBatch is null)
            {
                log.Warning($"Retrieved no data from OpenAI API, aborting");
                return null;
            }

            if (!IsCompletedStatus(openAIBatch.Status))
            {
                log.Info($"Batch status of '{openAIBatch.Status}' is not a completed state, skipping");
                return null;
            }

            log.Info($"Batch status of '{openAIBatch.Status}' is a completed state, returning");
            return batch;
        }
        finally
        {
            _batchCheckingLimiter.Release();
        }
    }

    public async Task CheckBatchesAsync()
    {
        var batchesToCheck = await GetAvailableBatchesAsync().ConfigureAwait(false);

        var checkBatchTasks = batchesToCheck.Select(ShouldUpdateBatchAsync);

        Log.Info("Waiting for all checks to complete");
        var results = await Task.WhenAll(checkBatchTasks).ConfigureAwait(false);
        Log.Info("All tasks completed");

        var batches = results
            .OfType<Batch>()
            .Select(batch => new UpdateBatchMessage { BatchId = batch.Id })
            .ToArray();

        Log.Info($"Retrieved {batches.Length} batch(es) to update");

        Log.Info($"Sending messages...");

        await MessageQueues.UpdateBatch.SendBatchedMessagesAsync(Log, batches).ConfigureAwait(false);

        Log.Info("All update messages sent");
    }
}