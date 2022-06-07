using Confluent.Kafka;
using kafka_plugin.Features.Consumer;

namespace kafka_plugin_client.Features.Consumer;

class ConsumerWorker : BackgroundService
{
    private readonly ILogger<ConsumerWorker> _logger;
    private readonly IConfiguration _configuration;
    private readonly IKafkaConsumer _consumer;

    public ConsumerWorker(ILogger<ConsumerWorker> logger,
                          IConfiguration configuration,
                          IKafkaConsumer consumer)
    {
        _logger = logger;
        _configuration = configuration;
        _consumer = consumer;

        // Subscribe to the events of the consumer
        _consumer.OnLogEntry += Consumer_OnLogEntry;
        _consumer.OnMessageEntry += Consumer_OnMessageEntry;
    }

    private void Consumer_OnMessageEntry(string topic, string message)
    {
        _logger.LogInformation($"Received message {topic}: '{message}'");
    }

    private void Consumer_OnLogEntry(string topic, LogMessage logMessage)
    {
        _logger.LogInformation($"{topic} - '{logMessage.Message}'");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
        string topic = _configuration["kafka:Topic"];
        await _consumer.ConsumeAsync(topic, stoppingToken);
    }
}