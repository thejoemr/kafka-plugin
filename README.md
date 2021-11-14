# kafka-plugin
## Plugin de integración con Apache Kafka
> Agregar referencia al espacio de nombres
```
using kafka_plugin;
```
1.   Configuración del consumidor
```
Consumer consumer = new("localhost:9092", "develop");
```
- Configuración de los eventos 
```
consumer.OnMessageReceived += OnMessageReceived;
consumer.OnError += OnConsumerError;
consumer.OnDisconect += OnDisconect;

async void OnMessageReceived(string topic, string message)
{
    Console.WriteLine($"Nuevo mensaje de '{topic}' => message: '{message}'");
    await Task.Delay(10);
}

async void OnConsumerError(string topic, Exception e)
{
    Console.WriteLine($"Error en '{topic}' => message: {e.Message}");
    await Task.Delay(10);
}

async void OnDisconect(string topic)
{
    Console.WriteLine($"Se detuvo la recepcion de mensajes de la queue '{topic}'");
    await Task.Delay(10);
}
```
- Ejecución de los consumidores en hilos independientes
```
Task.Factory.StartNew(() => consumer.StartReceivingMessages("test"));
Task.Factory.StartNew(() => consumer.StartReceivingMessages("develop"));
```
2.   Configuración del productor
```
Producer producer = new("localhost:9092");
```
- Configuración de los eventos
```
producer.OnDeliveryMessage += OnDeliveryMessage;
producer.OnError += OnProducerError;

void OnProducerError(string topic, Exception e)
{
    Console.WriteLine($"'{topic}' error de envio => message: '{e.Message}'");
}

void OnDeliveryMessage(string topic, string message)
{
    Console.WriteLine($"'{topic}' informe de envio => message: '{message}'");
}
```
- Ejecución de los productores en hilos independientes
```
DateTime now = DateTime.Now;
DateTime firstRun = now.AddMinutes(1);

TimeSpan timeToGo = (firstRun - now) <= TimeSpan.Zero ? TimeSpan.Zero : firstRun - now;

new Timer(async (x) =>
{
    await producer.PostAsync("test", $"{DateTime.Now} => hola test");
}, null, timeToGo, TimeSpan.FromSeconds(2));

new Timer(async (x) =>
{
    await producer.PostAsync("develop", $"{DateTime.Now} => hola develop");
}, null, timeToGo, TimeSpan.FromSeconds(2));
```
