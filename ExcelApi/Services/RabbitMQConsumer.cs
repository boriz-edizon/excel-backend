using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using MySqlConnector;
using System.Text;
using Microsoft.AspNetCore.SignalR;
using ExcelApi.Hubs;

namespace ExcelApi.Services;

public class RabbitMQConsumer
{
    private readonly IConfiguration _configuration;
    private readonly IConnectionFactory _connectionFactory;
    private readonly IConnection _connection;
    private readonly string? _sqlConnectionString;
    private readonly IModel _channel;
    private readonly IHubContext<ProgressHub> _hubContext;
    private int _chunksConsumed;  
    private int _totalChunks;  
    private float _percentConsumed;

    public RabbitMQConsumer(IConfiguration configuration, IConnectionFactory connectionFactory, IHubContext<ProgressHub> hubContext)
    {
        _configuration = configuration;
        _sqlConnectionString = _configuration.GetConnectionString("Default");

        _connectionFactory = connectionFactory;
        _connection = _connectionFactory.CreateConnection();
        _channel = _connection.CreateModel();

        _hubContext = hubContext;

        
        _channel.QueueDeclare(queue: "queue", durable: false,  exclusive: false, arguments: null);
    }

    // Resets the packet counter to zero
    public void ResetChunkCounter(int totalChunks)
    {
        _chunksConsumed = 0;
        _totalChunks = totalChunks;
    }

    public void Consume()
    {
        // var watch = new System.Diagnostics.Stopwatch();
        // watch.Start();

        //Set Event object which listen message from chanel which is sent by producer
        var consumer = new EventingBasicConsumer(_channel);

        consumer.Received +=  (model, eventArgs) =>
        {
            var body = eventArgs.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            InsertDataIntoDatabase(message);
            // Console.WriteLine("inside consume event");
        };
        //read the message
        _channel.BasicConsume(queue: "queue", autoAck: true, consumer: consumer);

        _chunksConsumed++;

        _percentConsumed = (float)_chunksConsumed / (float)_totalChunks;

        _hubContext.Clients.All.SendAsync("ReceiveUpdate", $"Packets received: {_percentConsumed}");


        // Console.WriteLine("consmer");
        // watch.Stop();
        // Console.WriteLine($"Consumer Execution Time: {watch.ElapsedMilliseconds} ms");
    }

    public  void InsertDataIntoDatabase(string query)
    {
        //inserting to db
        // Console.WriteLine("Inside db");
        // Console.WriteLine("a");

        Task.Run(() =>
        {
            // var watch = new System.Diagnostics.Stopwatch();
            // watch.Start();

            using (var mySQLConnection = new MySqlConnection(_sqlConnectionString))
            {
                mySQLConnection.Open();
                using (var command = new MySqlCommand(query.ToString(), mySQLConnection))
                {
                    command.ExecuteNonQuery();
                }
                mySQLConnection.Close();

                // Console.WriteLine("Sql Connection end");
            }

            // watch.Stop();
            // Console.WriteLine($"DB Execution Time: {watch.ElapsedMilliseconds} ms");

            // Console.WriteLine("end time " + ((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds());
        });
    }
}
