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
        var log = new LambdaContextLogger(context);

        log.Info("Entering CheckBatches FunctionHandler");

        var batchChecker = new BatchChecker(log);
        await batchChecker.CheckBatchesAsync().ConfigureAwait(false);

        log.Info("Exiting CheckBatches FunctionHandler");

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