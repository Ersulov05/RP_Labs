using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using StackExchange.Redis;
using Microsoft.Extensions.Configuration;

// TODO: Вынести конфиги, Обработка в consumer если обрыв redis пустой текст
namespace RankCalculator;

class Program
{
    private const string QueueName = "valuator.processing.rank";
    private const string EventsExchangeName = "valuator.events";
    private static IDatabase? _redis;

    static async Task Main(string[] args)
    {        
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var redisConnection = config["Redis:ConnectionString"] ?? throw new InvalidOperationException("RabbitMQ:HostName cannot be null");
        var rabbitHost = config["RabbitMQ:HostName"] ?? throw new InvalidOperationException("RabbitMQ:HostName cannot be null");
        var rabbitUser = config["RabbitMQ:UserName"] ?? throw new InvalidOperationException("RabbitMQ:HostName cannot be null");
        var rabbitPassword = config["RabbitMQ:Password"] ?? throw new InvalidOperationException("RabbitMQ:HostName cannot be null");

        Console.WriteLine("=== Configuration Values ===");
        Console.WriteLine($"Redis:ConnectionString = '{redisConnection}'");
        Console.WriteLine($"RabbitMQ:HostName = '{rabbitHost}'");
        Console.WriteLine($"RabbitMQ:UserName = '{rabbitUser}'");
        Console.WriteLine($"RabbitMQ:Password = '{rabbitPassword}'");

        _redis = ConnectionMultiplexer.Connect(redisConnection).GetDatabase();
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

    private static async Task PublishRankCalculatedEvent(IChannel channel, string id, double rank)
    {        
        var eventData = new
        {
            EventType = "RankCalculated",
            EntityId = id,
            Rank = rank,
        };
        
        byte[] messageData = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(eventData));
        
        await channel.BasicPublishAsync(
            exchange: EventsExchangeName,
            routingKey: "event.rank.calculated",
            mandatory: false,
            body: messageData);
    }

    private static async Task RunConsumer(IChannel channel)
    {
        AsyncEventingBasicConsumer consumer = new(channel);
        consumer.ReceivedAsync += (_, eventArgs) => ConsumeAsync(channel, eventArgs);
        
        await channel.BasicConsumeAsync(
            queue: QueueName,
            autoAck: false,
            consumer: consumer
        );
    }

    private static async Task ConsumeAsync(IChannel channel, BasicDeliverEventArgs eventArgs)
    {
        if (_redis == null)
        {
            await channel.BasicNackAsync(eventArgs.DeliveryTag, false, true);
            return;
        }

        string id = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
        string textKey = "TEXT-" + id;
        RedisValue redisValue = await _redis.StringGetAsync(textKey);
        string text = redisValue.ToString() ?? "";

        if (string.IsNullOrEmpty(text))
        {
            await channel.BasicAckAsync(eventArgs.DeliveryTag, false);
            return;
        }
    
        double rank = CalculateRank(text);
        string rankKey = "RANK-" + id;

        bool saved = await _redis.StringSetAsync(rankKey, rank.ToString());
        if (!saved)
        {
            await channel.BasicNackAsync(eventArgs.DeliveryTag, false, true);
            return;
        }

        await PublishRankCalculatedEvent(channel, id, rank);
        await channel.BasicAckAsync(eventArgs.DeliveryTag, false);
    }
    
    static double CalculateRank(string text)
    {
        if (string.IsNullOrEmpty(text)) return 0;
        
        int nonAlphabetic = 0;
        foreach (char c in text)
            if (!char.IsLetter(c)) nonAlphabetic++;
        
        return (double)nonAlphabetic / text.Length;
    }

    private static async Task DeclareTopologyAsync(IChannel channel)
    {
        await channel.ExchangeDeclareAsync(
            exchange: EventsExchangeName,
            type: ExchangeType.Topic,
            durable: true);

        await channel.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false
        );
    }
}