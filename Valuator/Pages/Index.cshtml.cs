using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using RabbitMQ.Client;
using StackExchange.Redis;

using System.Text;
using System.Text.Json;

namespace Valuator.Pages;

public class IndexModel : PageModel
{
    private readonly ILogger<IndexModel> _logger;
    private readonly IDatabase _redisDb;
    private readonly IConnection _rabbitConnection;
    private const string ExchangeName = "valuator.processing.rank";
    private const string QueueName = "valuator.processing.rank";
    private const string EventsExchangeName = "valuator.events";

    public IndexModel(ILogger<IndexModel> logger, IConnectionMultiplexer redis, IConnection rabbitConnection)
    {
        _logger = logger;
        _redisDb = redis.GetDatabase();
        _rabbitConnection = rabbitConnection;
    }

    public void OnGet()
    {
    }

    public async Task<IActionResult> OnPost(string text)
    {
        try
        {
            _logger.LogDebug(text);

            string id = Guid.NewGuid().ToString();

            if (string.IsNullOrEmpty(text)) {
                return RedirectToPage();
            }

            // TODO: (pa1) посчитать similarity и сохранить в БД (Redis) по ключу similarityKey
            string similarityKey = "SIMILARITY-" + id;
            int similarity = CheckSimilarity(text);
            _redisDb.StringSet(similarityKey, similarity.ToString());
            await PublishSimilarityCalculatedEvent(id, similarity);

            // TODO: (pa1) сохранить в БД (Redis) text по ключу textKey
            string textKey = "TEXT-" + id;
            _redisDb.StringSet(textKey, text);

            // TODO: (pa1) посчитать rank и сохранить в БД (Redis) по ключу rankKey
            string rankKey = "RANK-" + id;
            _redisDb.StringSet(rankKey, "processing");
            await SendRankCalculationTask(id, text);      

            return Redirect($"summary?id={id}");
        }
        catch (Exception ex)
        {
            return RedirectToPage("Error", new { message = ex.Message });
        } 
    }

    private async Task PublishSimilarityCalculatedEvent(string id, int similarity)
    {
        using var channel = await _rabbitConnection.CreateChannelAsync();
        
        await channel.ExchangeDeclareAsync(
            exchange: EventsExchangeName,
            type: ExchangeType.Topic,
            durable: true);
        
        var eventData = new
        {
            EventType = "SimilarityCalculated",
            EntityId = id,
            Similarity = similarity,
        };
        
        byte[] messageData = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(eventData));
        
        await channel.BasicPublishAsync(
            exchange: EventsExchangeName,
            routingKey: "event.similarity.calculated",
            mandatory: false,
            body: messageData);
    }

    private async Task SendRankCalculationTask(string id, string text)
    {
        using var channel = await _rabbitConnection.CreateChannelAsync();
        
        await DeclareTopologyAsync(channel);
        
        byte[] messageData = Encoding.UTF8.GetBytes(id);
        
        await channel.BasicPublishAsync(
            exchange: ExchangeName,
            routingKey: "",
            mandatory: false,
            body: messageData);
    }

    private static async Task DeclareTopologyAsync(IChannel channel)
    {
        // Объявляем exchange типа Direct
        await channel.ExchangeDeclareAsync(
            exchange: ExchangeName,
            type: ExchangeType.Direct,
            durable: true);
        
        await channel.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false);
        
        // Привязываем очередь к exchange
        await channel.QueueBindAsync(
            queue: QueueName,
            exchange: ExchangeName,
            routingKey: "");
    }

    private int CheckSimilarity(string text)
    {
        var server = _redisDb.Multiplexer.GetServer("redis", 6379);
        var keys = server.Keys(pattern: "TEXT-*");
        
        foreach (var key in keys)
        {
            var storedValue = _redisDb.StringGet(key);
            if (storedValue.HasValue && storedValue.ToString() == text) {
                return 1;
            }                    
        }
        
        return 0;
    }
}
