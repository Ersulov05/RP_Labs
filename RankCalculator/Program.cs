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

    static async Task Main(string[] args)
    {        
        try
        {
            _redis = ConnectionMultiplexer.Connect("redis:6379").GetDatabase();
    
            var factory = new ConnectionFactory 
            { 
                HostName = "rabbitmq-pa3",
                UserName = "guest",
                Password = "guest",
                AutomaticRecoveryEnabled = true
            };
            var connection = await factory.CreateConnectionAsync();
            var channel = await connection.CreateChannelAsync();
            
            await channel.QueueDeclareAsync(QueueName, durable: true, exclusive: false, autoDelete: false);
            await channel.BasicQosAsync(0, 1, false);
            
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, ea) =>
            {
                try
                {
                    var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                    Console.WriteLine($"→ Received: {message}");
                    
                    var task = JsonSerializer.Deserialize<RankTaskMessage>(message);
                    
                    if (task != null && !string.IsNullOrEmpty(task.Id))
                    {
                        double rank = CalculateRank(task.Text ?? "");
                        await _redis.StringSetAsync($"RANK-{task.Id}", rank.ToString());
                    }
                    
                    await channel.BasicAckAsync(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    await channel.BasicNackAsync(ea.DeliveryTag, false, false);
                }
            };
            
            await channel.BasicConsumeAsync(QueueName, autoAck: false, consumer: consumer);            
            await Task.Delay(-1);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Fatal error: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
            throw;
        }
    }
    
    static double CalculateRank(string text)
    {
        if (string.IsNullOrEmpty(text)) return 0;
        
        int nonAlphabetic = 0;
        foreach (char c in text)
            if (!char.IsLetter(c)) nonAlphabetic++;
        
        return (double)nonAlphabetic / text.Length;
    }
}

class RankTaskMessage
{
    public string? Id { get; set; }
    public string? Text { get; set; }
}