using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using StackExchange.Redis;

namespace RankCalculator;

class Program
{
    private const string QueueName = "valuator.processing.rank";
    private static IDatabase _redis;

    public static async Task Main(string[] args)
    {        
        try
        {
            await ConnectToRedisAsync();
            ConnectionFactory factory = new ConnectionFactory
            {
                HostName = "rabbitmq-pa3",
                UserName = "guest",
                Password = "guest",
                AutomaticRecoveryEnabled = true
            };
            
            await using IConnection connection = await factory.CreateConnectionAsync();
            await using IChannel channel = await connection.CreateChannelAsync();
            
            await DeclareTopologyAsync(channel);
            await RunConsumerAsync(channel);
            await Task.Delay(-1);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Fatal error: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
            throw;
        }
    }

    private static async Task ConnectToRedisAsync()
    {
        _redis = ConnectionMultiplexer.Connect("redis:6379").GetDatabase();
        await Task.CompletedTask;
    }

    private static async Task DeclareTopologyAsync(IChannel channel)
    {
        await channel.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false
        );
        
        // Настройка: 1 сообщение за раз
        await channel.BasicQosAsync(0, 1, false);
    }

    private static async Task RunConsumerAsync(IChannel channel)
    {
        AsyncEventingBasicConsumer consumer = new(channel);
        consumer.ReceivedAsync += async (_, eventArgs) => 
            await ProcessMessageAsync(channel, eventArgs);
        
        await channel.BasicConsumeAsync(
            queue: QueueName,
            autoAck: false,
            consumer: consumer
        );
    }

    private static async Task ProcessMessageAsync(IChannel channel, BasicDeliverEventArgs eventArgs)
    {
        try
        {
            string message = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            
            var task = JsonSerializer.Deserialize<RankTaskMessage>(message);
            
            if (task != null && !string.IsNullOrEmpty(task.Id))
            {
                double rank = CalculateRank(task.Text ?? "");
                await SaveRankToRedisAsync(task.Id, rank);
            }
            
            await channel.BasicAckAsync(eventArgs.DeliveryTag, false);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing message: {ex.Message}");
            await channel.BasicNackAsync(eventArgs.DeliveryTag, false, false);
        }
    }

    private static async Task SaveRankToRedisAsync(string id, double rank)
    {
        await _redis.StringSetAsync($"RANK-{id}", rank.ToString());
    }

    private static double CalculateRank(string text)
    {
        if (string.IsNullOrEmpty(text)) return 0;
        
        int nonAlphabetic = 0;
        foreach (char c in text)
        {
            if (!char.IsLetter(c)) nonAlphabetic++;
        }
        
        return (double)nonAlphabetic / text.Length;
    }
}

class RankTaskMessage
{
    public string? Id { get; set; }
    public string? Text { get; set; }
}