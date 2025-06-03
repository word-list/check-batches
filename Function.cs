using System.Text.Json.Serialization;
using Amazon.Lambda.Core;
using Amazon.Lambda.RuntimeSupport;
using Amazon.Lambda.Serialization.SystemTextJson;
using WordList.Processing.CheckBatches.Models;

namespace WordList.Processing.CheckBatches;

public class Function
{
    public static async Task<string> FunctionHandler(ILambdaContext context)
    {
        context.Logger.LogInformation("Entering CheckBatches FunctionHandler");

        var updateBatchQueueUrl = Environment.GetEnvironmentVariable("UPDATE_BATCH_QUEUE_URL")
            ?? throw new Exception("UPDATE_BATCH_QUEUE_URL must be set");

        var batchChecker = new BatchChecker(context.Logger, updateBatchQueueUrl);
        await batchChecker.CheckBatchesAsync().ConfigureAwait(false);

        context.Logger.LogInformation("Exiting CheckBatches FunctionHandler");

        return "ok";
    }

    public static async Task Main()
    {
        Func<ILambdaContext, Task<string>> handler = FunctionHandler;
        await LambdaBootstrapBuilder.Create(handler, new SourceGeneratorLambdaJsonSerializer<LambdaFunctionJsonSerializerContext>())
            .Build()
            .RunAsync();
    }
}

[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(Batch))]
public partial class LambdaFunctionJsonSerializerContext : JsonSerializerContext
{
}