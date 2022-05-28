using Confluent.Kafka;

namespace kafka_plugin.Features.Producer
{
    public class KafkaProducer : IKafkaProducer
    {
        public ProducerConfig Configuration { get; }
        public event IKafkaProducer.LogEntry? OnLogEntry;

        public KafkaProducer(ProducerConfig configuration)
        {
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public async Task<DeliveryResult<long, string>> ProduceAsync(string topic, string message, CancellationToken cancellationToken = default)
        {
            using (var producer = new ProducerBuilder<long, string>(Configuration)
                .SetKeySerializer(Serializers.Int64)
                .SetValueSerializer(Serializers.Utf8)
                .SetLogHandler((_, logMsg) => OnLogEntry?.Invoke(topic, logMsg))
                .Build())
            {
                try
                {
                    Message<long, string> messageData = new()
                    {
                        Key = DateTime.UtcNow.Ticks,
                        Value = message
                    };

                    return await producer.ProduceAsync(topic, messageData);
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }
    }
}