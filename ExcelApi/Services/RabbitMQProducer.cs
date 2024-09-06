using RabbitMQ.Client;
using System.Text;

namespace ExcelApi.Services ;
public class RabbitMQProducer  {

    private readonly IConnectionFactory _connectionFactory;
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly RabbitMQConsumer _consumer;
    public int currQueueIndex;
    public RabbitMQProducer(IConnectionFactory connectionFactory, RabbitMQConsumer consumer)
    {
        _connectionFactory = connectionFactory;
        _connection = _connectionFactory.CreateConnection();
        _channel = _connection.CreateModel();
        _consumer = consumer;
        
        currQueueIndex = 0; 

        _channel.QueueDeclare(queue: "queue", 
                        durable: false,
                        exclusive: false,
                        arguments: null);
        
    }

    public void produce(string chunk) {{
        // var watch = new System.Diagnostics.Stopwatch();
        // watch.Start();

        var body = Encoding.UTF8.GetBytes(chunk);
        
        _channel.BasicPublish(exchange: "", routingKey: "queue", body: body);

        _consumer.Consume();

        // Console.WriteLine("producer");
        // watch.Stop();
        // Console.WriteLine($"Producer Execution Time: {watch.ElapsedMilliseconds} ms");
    }}

}