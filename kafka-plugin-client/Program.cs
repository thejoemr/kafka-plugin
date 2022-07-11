using System.Net;
using Confluent.Kafka;
using kafka_plugin.Features.Consumer;
using kafka_plugin.Features.Producer;
using kafka_plugin_client.Infraestructure.Workers;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        IConfiguration configuration = hostContext.Configuration;

        // Agregamos el servicio productor
        services.AddSingleton<IKafkaProducer>((sp) => {
            var config = new ProducerConfig
            {
                BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BROKER"),
                ClientId = Dns.GetHostName(),
            };

            // Puedes hacer tu configuraci�n con un Diccionario y pasarlo en el constructor de un objecto [ProducerConfig]
            return new KafkaProducer(config);
        });

        // Agregamos el servicio consumidor
        services.AddSingleton<IKafkaConsumer>((sp) => {
            var config = new ConsumerConfig
            {
                BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BROKER"),
                GroupId = Environment.GetEnvironmentVariable("KAFKA_GROUPID"),
                ClientId = Dns.GetHostName(),
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            // Puedes hacer tu configuración con un Diccionario y pasarlo en el constructor de un objecto [ConsumerConfig]
            return new KafkaConsumer(config);
        });

        // Agregamos los workers de consumo y de producción
        services.AddHostedService<ConsumerWorker>();
        services.AddHostedService<ProducerWorker>();
    })
    .Build();

await host.RunAsync();
