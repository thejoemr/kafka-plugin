using System.Net;
using Confluent.Kafka;
using kafka_plugin.Features.Consumer;
using kafka_plugin.Features.Producer;
using kafka_plugin_client.Features.Consumer;
using kafka_plugin_client.Features.Producer;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        IConfiguration configuration = hostContext.Configuration;

        services.AddSingleton<IKafkaProducer>((sp) => {
            var config = new ProducerConfig
            {
                BootstrapServers = configuration["kafka:Brokers"],
                ClientId = Dns.GetHostName(),
            };

            // Puedes hacer tu configuraci�n con un Diccionario y pasarlo en el constructor de un objecto [ProducerConfig]
            return new KafkaProducer(config);
        });

        services.AddSingleton<IKafkaConsumer>((sp) => {
            var config = new ConsumerConfig
            {
                BootstrapServers = configuration["kafka:Brokers"],
                GroupId = configuration["kafka:GroupId"],
                ClientId = Dns.GetHostName(),
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            // Puedes hacer tu configuraci�n con un Diccionario y pasarlo en el constructor de un objecto [ProducerConfig]
            return new KafkaConsumer(config);
        });

        services.AddHostedService<ConsumerWorker>();
        services.AddHostedService<ProducerWorker>();
    })
    .Build();

await host.RunAsync();
