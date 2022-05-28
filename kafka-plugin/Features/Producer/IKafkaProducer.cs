using Confluent.Kafka;

namespace kafka_plugin.Features.Producer
{
    public interface IKafkaProducer
    {
        /// <summary>
        /// Evento que captura una entrada de log proveniente del consumidor
        /// </summary>
        /// <param name="topic">Topic que se está leyendo</param>
        /// <param name="logMessage">Información de la entrada log</param>
        delegate void LogEntry(string topic, LogMessage logMessage);

        /// <summary>
        /// Evento que captura una entrada de log proveniente del consumidor
        /// </summary>
        event LogEntry OnLogEntry;

        /// <summary>
        /// Produce un mensaje en un topic
        /// </summary>
        /// <param name="topic">Topic de destino</param>
        /// <param name="message">Mensaje</param>
        /// <param name="cancellationToken">Toquen de cancelación de envio</param>
        /// <returns></returns>
        Task<DeliveryResult<long, string>> ProduceAsync(string topic, string message, CancellationToken cancellationToken = default);
    }
}