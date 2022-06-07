using Confluent.Kafka;

namespace kafka_plugin.Features.Consumer
{
    /// <summary>
    /// Consumidor de kafka
    /// </summary>
    public class KafkaConsumer : IKafkaConsumer
    {
        public ConsumerConfig Configuration { get; }
        public event IKafkaConsumer.LogEntry? OnLogEntry;
        public event IKafkaConsumer.MessageEntry? OnMessageEntry;

        /// <summary>
        /// Instancia de consumidor
        /// </summary>
        /// <param name="configuration">Configuración del consumidor kafka</param>
        public KafkaConsumer(ConsumerConfig configuration)
        {
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public async Task ConsumeAsync(string topic, CancellationToken cancellationToken) => await Task.Run(() =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    string message = Consume(topic, cancellationToken);
                    if (!string.IsNullOrEmpty(message))
                    {
                        OnMessageEntry?.Invoke(topic, message);
                    }
                }
                catch (ConsumeException e)
                {
                    //  Se detiene el consumo del topico si el error es fatal
                    if (e.Error.IsFatal)
                    {
                        var logMsg = new LogMessage("KafkaConsumer", SyslogLevel.Critical, "ConsumerAsync", e.Message);
                        OnLogEntry?.Invoke(topic, logMsg);
                        throw;
                    }
                    else
                    {
                        var logMsg = new LogMessage("KafkaConsumer", SyslogLevel.Error, "ConsumerAsync", e.Message);
                        OnLogEntry?.Invoke(topic, logMsg);
                    }
                }
                catch (Exception e)
                {
                    var logMsg = new LogMessage("KafkaConsumer", SyslogLevel.Critical, "ConsumerAsync", e.Message);
                    OnLogEntry?.Invoke(topic, logMsg);
                    throw;
                }
            }
        }, cancellationToken);

        public string Consume(string topic, CancellationToken cancellationToken = default)
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(Configuration)
                .SetValueDeserializer(Deserializers.Utf8)
                .SetLogHandler((_, logMsg) => OnLogEntry?.Invoke(topic, logMsg))
                .Build();

            string message = string.Empty;

            consumer.Subscribe(topic);
            var result = consumer.Consume(cancellationToken);
            if (result != null)
            {
                message = result.Message.Value;
            }
            consumer.Close();

            return message;
        }
    }
}
