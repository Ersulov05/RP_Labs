using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Microsoft.Extensions.Configuration;

namespace EventsLogger;

class Program
{
    private const string EventsExchangeName = "valuator.events";
    private static string? _queueName;

    static async Task Main(string[] args)
    {        
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        string rabbitHost = config["RabbitMQ:HostName"] ?? throw new InvalidOperationException("RabbitMQ:HostName cannot be null");
        string rabbitUser = config["RabbitMQ:UserName"] ?? throw new InvalidOperationException("RabbitMQ:UserName cannot be null");
        string rabbitPassword = config["RabbitMQ:Password"] ?? throw new InvalidOperationException("RabbitMQ:Password cannot be null");

        ConnectionFactory factory = new ConnectionFactory
        {
            HostName = rabbitHost,
            UserName = rabbitUser,
            Password = rabbitPassword,
            AutomaticRecoveryEnabled = true
        };

        await using IConnection connection = await factory.CreateConnectionAsync();
        await using IChannel channel = await connection.CreateChannelAsync();

        await DeclareTopologyAsync(channel);
            
        await RunConsumer(channel);          
        await Task.Delay(-1);
    }

    private static async Task RunConsumer(IChannel channel)
    {
        AsyncEventingBasicConsumer consumer = new(channel);
        consumer.ReceivedAsync += (_, eventArgs) =>
        {
            Consume(eventArgs);
            return Task.CompletedTask;
        };
        
        await channel.BasicConsumeAsync(
            queue: _queueName!,
            autoAck: true,
            consumer: consumer
        );
    }

    private static void Consume(BasicDeliverEventArgs eventArgs)
    {
        string body = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
        
        try
        {
            using JsonDocument doc = JsonDocument.Parse(body);
            JsonElement root = doc.RootElement;
            
            string eventType = root.GetProperty("EventType").GetString() ?? throw new InvalidOperationException("EventType cannot be null");
            string entityId = root.GetProperty("EntityId").GetString() ?? throw new InvalidOperationException("EntityId cannot be null");
            
            Console.WriteLine($"=== Event Received ===");
            Console.WriteLine($"Event Type: {eventType}");
            Console.WriteLine($"Entity ID: {entityId}");
            
            if (eventType == "RankCalculated" && root.TryGetProperty("Rank", out JsonElement rankElement))
            {
                double rank = rankElement.GetDouble();
                Console.WriteLine($"Rank Value: {rank}");
            }
            else if (eventType == "SimilarityCalculated" && root.TryGetProperty("Similarity", out JsonElement similarityElement))
            {
                int similarity = similarityElement.GetInt32();
                Console.WriteLine($"Similarity Value: {similarity}");
            }
            
            Console.WriteLine($"Raw Message: {body}");
            Console.WriteLine("=====================");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing event: {ex.Message}");
            Console.WriteLine($"Raw message: {body}");
        }
    }

    private static async Task DeclareTopologyAsync(IChannel channel)
    {
        await channel.ExchangeDeclareAsync(
            exchange: EventsExchangeName,
            type: ExchangeType.Topic,
            durable: true);

        var queueDeclareResult = await channel.QueueDeclareAsync(
            queue: "",
            durable: false,
            exclusive: true,
            autoDelete: true);

        _queueName = queueDeclareResult.QueueName;
        
        await channel.QueueBindAsync(
            queue: _queueName,
            exchange: EventsExchangeName,
            routingKey: "event.*.calculated");
    }
}