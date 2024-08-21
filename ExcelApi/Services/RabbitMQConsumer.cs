using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using MySqlConnector;
using System.Text;

namespace ExcelApi.Services ;

public class RabbitMQConsumer {
    private readonly IConfiguration _configuration;
    private readonly IConnectionFactory _connectionFactory;
    private readonly IConnection _connection;
    private readonly MySqlConnection _sqlConnection;

    private readonly IModel _channel;

    public RabbitMQConsumer(IConfiguration configuration, IConnectionFactory connectionFactory){
        _configuration = configuration;
        _sqlConnection = new MySqlConnection(_configuration.GetConnectionString("Default"));

        _connectionFactory = connectionFactory;
        _connection = _connectionFactory.CreateConnection();
        _channel = _connection.CreateModel();

        int numberOfQueues = 5;

        for (int i = 0; i < numberOfQueues; i++){
        _channel.QueueDeclare(queue: $"queue{i}", 
                        durable: false,
                        exclusive: false,
                        arguments: null);
        }    
    }
    public void Consume(int queueNumber) {
        //Set Event object which listen message from chanel which is sent by producer
        var consumer = new EventingBasicConsumer(_channel);

        consumer.Received += (model, eventArgs) => {
            var body = eventArgs.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            
        };
        //read the message
        _channel.BasicConsume(queue: $"queue{queueNumber}", autoAck: true, consumer: consumer);
    }

    public async Task InsertToDB(string query, int queueN){
        //inserting to db
        // Console.WriteLine("Inside db");
        // Console.WriteLine("a");


        using (var mySQLConnection = new MySqlConnection(dbConnString))
        {
            mySQLConnection.Open();
            using (var command = new MySqlCommand(query.ToString(), mySQLConnection))
            {
                command.ExecuteNonQuery();
            }
            mySQLConnection.Close();
        }
    });
}
