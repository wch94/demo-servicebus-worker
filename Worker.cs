using Azure.Messaging.ServiceBus;

namespace DemoServiceBusWorker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly ServiceBusClient _client;
    private readonly ServiceBusProcessor _processor;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;

        var connectionString = Environment.GetEnvironmentVariable("ServiceBusConnection");
        var queueName = "demoqueue";

        _client = new ServiceBusClient(connectionString);
        _processor = _client.CreateProcessor(queueName, new ServiceBusProcessorOptions());
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        _processor.ProcessMessageAsync += MessageHandler;
        _processor.ProcessErrorAsync += ErrorHandler;
        await _processor.StartProcessingAsync(cancellationToken);

        _logger.LogInformation("Started Service Bus processor.");
    }

    private async Task MessageHandler(ProcessMessageEventArgs args)
    {
        string body = args.Message.Body.ToString();
        _logger.LogInformation($"Received message: {body}");
        await args.CompleteMessageAsync(args.Message);
    }

    private Task ErrorHandler(ProcessErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Message handler error");
        return Task.CompletedTask;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await _processor.StopProcessingAsync(cancellationToken);
        await _processor.DisposeAsync();
        await _client.DisposeAsync();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.CompletedTask;
}
