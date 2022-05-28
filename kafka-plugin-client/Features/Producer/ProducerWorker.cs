using Confluent.Kafka;
using kafka_plugin.Features.Producer;

namespace kafka_plugin_client.Features.Producer;

public class ProducerWorker : BackgroundService
{
    private readonly ILogger<ProducerWorker> _logger;
    private readonly IConfiguration _configuration;
    private readonly IKafkaProducer _producer;

    public ProducerWorker(ILogger<ProducerWorker> logger,
                          IConfiguration configuration,
                          IKafkaProducer producer)
    {
        _logger = logger;
        _configuration = configuration;
        _producer = producer;

        // Subscribe to the events of the producer
        _producer.OnLogEntry += Producer_OnLogEntry;
    }

    private void Producer_OnLogEntry(string topic, LogMessage logMessage)
    {
        _logger.LogInformation($"{topic} - '{logMessage.Message}'");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (true)
        {
            var message = string.Format("Worker running at: {0}", DateTimeOffset.Now);
            await _producer.ProduceAsync(_configuration["kafka:Topic"], message, stoppingToken);
            await Task.Delay(2000, stoppingToken);
        }
    }
}
