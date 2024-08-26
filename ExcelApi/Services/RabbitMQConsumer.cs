using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using MySqlConnector;
using System.Text;

namespace ExcelApi.Services;

public class RabbitMQConsumer
{
    private readonly IConfiguration _configuration;
    private readonly IConnectionFactory _connectionFactory;
    private readonly IConnection _connection;
    private readonly string? _sqlConnectionString;
    private readonly IModel _channel;

    public RabbitMQConsumer(IConfiguration configuration, IConnectionFactory connectionFactory)
    {
        _configuration = configuration;
        _sqlConnectionString = _configuration.GetConnectionString("Default");

        _connectionFactory = connectionFactory;
        _connection = _connectionFactory.CreateConnection();
        _channel = _connection.CreateModel();

        int numberOfQueues = 5;

        for (int i = 0; i < numberOfQueues; i++)
        {
            _channel.QueueDeclare(queue: $"queue{i}",
                            durable: false,
                            exclusive: false,
                            arguments: null);
        }
    }
    public void Consume(int queueNumber)
    {

        var watch = new System.Diagnostics.Stopwatch();
        watch.Start();

        //Set Event object which listen message from chanel which is sent by producer
        var consumer = new EventingBasicConsumer(_channel);

        consumer.Received +=  (model, eventArgs) =>
        {
            var body = eventArgs.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            InsertDataIntoDatabase(message);
        };
        //read the message
        _channel.BasicConsume(queue: $"queue{queueNumber}", autoAck: true, consumer: consumer);

        watch.Stop();
        // Console.WriteLine($"Consumer Execution Time: {watch.ElapsedMilliseconds} ms");
    }

    public  void InsertDataIntoDatabase(string query)
    {
        //inserting to db
        // Console.WriteLine("Inside db");
        // Console.WriteLine("a");

        Task.Run(() =>
        {
            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            using (var mySQLConnection = new MySqlConnection(_sqlConnectionString))
            {
                mySQLConnection.Open();
                using (var command = new MySqlCommand(query.ToString(), mySQLConnection))
                {
                    command.ExecuteNonQuery();
                }
                mySQLConnection.Close();
            }

            watch.Stop();
            // Console.WriteLine($"DB Execution Time: {watch.ElapsedMilliseconds} ms");

            // Console.WriteLine("end time " + ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds());

        });
    }
}
