using ExcelApi.Services;
using ExcelApi.Hubs;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

//creating the connectionfactory for rabbitmq
builder.Services.AddSingleton<IConnectionFactory>(sp => {
    var factory = new ConnectionFactory { HostName = "localhost" };
    return factory;
});

builder.Services.AddSingleton<RabbitMQProducer>();
builder.Services.AddSingleton<RabbitMQConsumer>();

builder.Services.AddSignalR();

builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowSpecificOrigins",
        builder =>
        {
            builder.WithOrigins("http://127.0.0.1:5501") // Specify the frontend origin
                            .AllowAnyMethod()
                            .AllowAnyHeader()
                            .AllowCredentials(); // Required for SignalR with credentials
        });
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseCors("AllowSpecificOrigins");

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.MapHub<ProgressHub>("/progressHub");

app.Run();
