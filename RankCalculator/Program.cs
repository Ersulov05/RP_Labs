using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using StackExchange.Redis;
using Microsoft.Extensions.Configuration;
using Shard;

// TODO: Вынести конфиги, Обработка в consumer если обрыв redis пустой текст
namespace RankCalculator;

class Program
{
    private const string QueueName = "valuator.processing.rank";
    private const string EventsExchangeName = "valuator.events";
    private static IShardRedisService _shardRedis = null!;

    static async Task Main(string[] args)
    {        
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var dbMain = Environment.GetEnvironmentVariable("DB_MAIN") ?? "localhost:6379";
        var dbRu = Environment.GetEnvironmentVariable("DB_RU") ?? "localhost:6380";
        var dbEu = Environment.GetEnvironmentVariable("DB_EU") ?? "localhost:6381";
        var dbAsia = Environment.GetEnvironmentVariable("DB_ASIA") ?? "localhost:6382";

        var rabbitHost = config["RabbitMQ:HostName"] ?? throw new InvalidOperationException("RabbitMQ:HostName cannot be null");
        var rabbitUser = config["RabbitMQ:UserName"] ?? throw new InvalidOperationException("RabbitMQ:HostName cannot be null");
        var rabbitPassword = config["RabbitMQ:Password"] ?? throw new InvalidOperationException("RabbitMQ:HostName cannot be null");

        Console.WriteLine("=== Configuration Values ===");
        Console.WriteLine($"RabbitMQ:HostName = '{rabbitHost}'");
        Console.WriteLine($"RabbitMQ:UserName = '{rabbitUser}'");
        Console.WriteLine($"RabbitMQ:Password = '{rabbitPassword}'");

        var mainDb = ConnectionMultiplexer.Connect(dbMain).GetDatabase();
        _shardRedis = new ShardRedisService(dbRu, dbEu, dbAsia);

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
                 
        await RunConsumer(channel, mainDb);            
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

    private static async Task RunConsumer(IChannel channel, IDatabase mainDb)
    {
        AsyncEventingBasicConsumer consumer = new(channel);
        consumer.ReceivedAsync += (_, eventArgs) => ConsumeAsync(channel, eventArgs, mainDb);
        
        await channel.BasicConsumeAsync(
            queue: QueueName,
            autoAck: false,
            consumer: consumer
        );
    }

    private static async Task ConsumeAsync(IChannel channel, BasicDeliverEventArgs eventArgs, IDatabase mainDb)
    {
        TimeSpan interval = TimeSpan.FromSeconds(new Random().Next(3, 15));
        Console.WriteLine($"Waiting {interval}");
        await Task.Delay(interval);

        if (_shardRedis == null)
        {
            await channel.BasicNackAsync(eventArgs.DeliveryTag, false, true);
            return;
        }

        string id = Encoding.UTF8.GetString(eventArgs.Body.ToArray());

        string shardMapKey = $"SHARD-{id}";
        string region = mainDb.StringGet(shardMapKey);
        Console.WriteLine($"RankCalculator LOOKUP: {id}, {region.ToUpper()}");
        var regionDb = _shardRedis.GetDatabaseForRegion(region);

        string textKey = "TEXT-" + id;
        RedisValue redisValue = await regionDb.StringGetAsync(textKey);
        string text = redisValue.ToString() ?? "";

        if (string.IsNullOrEmpty(text))
        {
            await channel.BasicAckAsync(eventArgs.DeliveryTag, false);
            return;
        }
    
        double rank = CalculateRank(text);
        string rankKey = "RANK-" + id;

        bool saved = await regionDb.StringSetAsync(rankKey, rank.ToString());
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