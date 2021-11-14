using Confluent.Kafka;

namespace kafka_plugin
{
    /// <summary>
    /// Expone la funcionalidad de un consumidor de colas de mensaje
    /// </summary>
    public class Consumer
    {
        /// <summary>
        /// Configuración del componente
        /// </summary>
        public ConsumerConfig ConsumerConfig { get; }
        /// <summary>
        /// Dirección del servidor kafka
        /// </summary>
        public string Server { get; }

        public delegate void MessageReceived(string topic, string message);
        public delegate void Error(string topic, Exception e);
        public delegate void Disconect(string topic);

        /// <summary>
        /// Se dispara cada vez que una <strong>Queue</strong> recibe un mensaje
        /// </summary>
        public event MessageReceived ?OnMessageReceived;
        /// <summary>
        /// Se dispara cada vez que una <strong>Queue</strong> genera un mensaje
        /// </summary>
        public event Error? OnError;
        /// <summary>
        /// Se dispara cada vez que una <strong>Queue</strong> se desconecta
        /// </summary>
        public event Disconect? OnDisconect;

        /// <summary>
        /// Crea un nuevo consumidor de colas de mensaje
        /// </summary>
        /// <param name="server">Url del servidor Kafka</param>
        /// <param name="groupId">Identificador del consumidor</param>
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

        /// <summary>
        /// Se suscribe a una <strong>Queue</strong> para recibir mensajes
        /// </summary>
        /// <param name="topic">Nombre de la Queue de mensajes</param>
        /// <param name="stoppingToken">Token de cancelación</param>
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
