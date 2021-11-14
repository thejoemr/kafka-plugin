using Confluent.Kafka;

namespace kafka_plugin
{
    public class Consumer
    {
        public ConsumerConfig ConsumerConfig { get; }
        public string Server { get; }

        public delegate void MessageReceived(string topic, string message);
        public delegate void Error(string topic, Exception e);
        public delegate void Disconect(string topic);

        public event MessageReceived ?OnMessageReceived;
        public event Error? OnError;
        public event Disconect? OnDisconect;

        public Consumer(string server, string groupId)
        {
            ConsumerConfig = new ConsumerConfig
            {
                BootstrapServers = server,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                MaxPollIntervalMs = 300000,
                GroupId = groupId,

                // Read messages from start if no commit exists.
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            Server = server;
        }

        public void StartReceivingMessages(string topic, CancellationToken stoppingToken = default)
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(ConsumerConfig)
                .SetValueDeserializer(Deserializers.Utf8)
                .SetErrorHandler((_, e) => OnError?.Invoke(topic, new Exception(e.Reason)))
                .Build();

            try
            {
                consumer.Subscribe(topic);

                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<Ignore, string> result = consumer.Consume(TimeSpan.FromMilliseconds(ConsumerConfig.MaxPollIntervalMs - 1000 ?? 250000));

                    string message = result.Message.Value;

                    consumer.Commit(result);
                    consumer.StoreOffset(result);

                    OnMessageReceived?.Invoke(topic, message);
                }
            }
            catch (Exception e)
            {
                OnError?.Invoke(topic, e);
                OnDisconect?.Invoke(topic);
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
