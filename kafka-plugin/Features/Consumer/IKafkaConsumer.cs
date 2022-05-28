using Confluent.Kafka;

namespace kafka_plugin.Features.Consumer
{
    /// <summary>
    /// Consumidor Kafka
    /// </summary>
    public interface IKafkaConsumer
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
        /// Evento que captura una entrada de mensaje proveniente del consumidor
        /// </summary>
        /// <param name="topic">Topic que se está leyendo</param>
        /// <param name="message">Mensaje recibido</param>
        delegate void MessageEntry(string topic, string message);

        /// <summary>
        /// Evento que captura una entrada de mensaje proveniente del consumidor
        /// </summary>
        event MessageEntry OnMessageEntry;

        /// <summary>
        /// Comienza a escuchar los eventos de un topic de forma asíncrona
        /// </summary>
        /// <param name="topic">Topic que se está leyendo</param>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <returns></returns>
        Task ConsumeAsync(string topic, CancellationToken cancellationToken = default);

        
        /// <summary>
        /// Comienza a escuchar los eventos de un topic
        /// </summary>
        /// <param name="topic">Topic que se está leyendo</param>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <returns></returns>
        string Consume(string topic, CancellationToken cancellationToken = default);
    }
}