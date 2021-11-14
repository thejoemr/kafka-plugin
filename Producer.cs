﻿using Confluent.Kafka;
using System.Net;

namespace kafka_plugin;
public class Producer
{
    public string Server { get; }
    public ProducerConfig ProducerConfig { get; }

    public delegate void DeliveryMessage(string topic, string message);
    public delegate void Error(string topic, Exception e);

    public event DeliveryMessage? OnDeliveryMessage;
    public event Error? OnError;

    public Producer(string server)
    {
        ProducerConfig = new ProducerConfig
        {
            BootstrapServers = server,
            EnableDeliveryReports = true,
            ClientId = Dns.GetHostName(),

            // retry settings:
            // Receive acknowledgement from all sync replicas
            Acks = Acks.All,
            // Number of times to retry before giving up
            MessageSendMaxRetries = 3,
            // Duration to retry before next attempt
            RetryBackoffMs = 1000,
            // Set to true if you don't want to reorder messages on retry
            EnableIdempotence = true
        };

        Server = server;
    }

    public async Task<bool> PostAsync(string topic, string message)
    {
        using var producer = new ProducerBuilder<long, string>(ProducerConfig)
            .SetKeySerializer(Serializers.Int64)
            .SetValueSerializer(Serializers.Utf8)
            .SetLogHandler((_, message) => OnDeliveryMessage?.Invoke(topic, $"Facility: {message.Facility}-{message.Level} Message: {message.Message}"))
            .SetErrorHandler((_, e) => OnError?.Invoke(topic, new Exception($"Error: {e.Reason}. Is Fatal: {e.IsFatal}")))
            .Build();

        try
        {
            Message<long, string> messageData = new()
            {
                Key = DateTime.UtcNow.Ticks,
                Value = message
            };

            DeliveryResult<long, string> deliveryReport = await producer.ProduceAsync(topic, messageData);

            OnDeliveryMessage?.Invoke(topic, $"Message sent (value: '{message}'). Delivery status: {deliveryReport.Status}");

            if (deliveryReport.Status != PersistenceStatus.Persisted)
            {
                // delivery might have failed after retries. This message requires manual processing.
                OnDeliveryMessage?.Invoke(topic, $"ERROR: Message not ack'd by all brokers (value: '{message}'). Delivery status: {deliveryReport.Status}");
            }

            return deliveryReport.Status == PersistenceStatus.Persisted;
        }
        catch (Exception e)
        {
            OnError?.Invoke(topic, e);
        }

        return false;
    }
}