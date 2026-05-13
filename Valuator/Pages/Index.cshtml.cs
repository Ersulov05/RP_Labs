using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using RabbitMQ.Client;
using StackExchange.Redis;
using Shard;

using System.Text;
using System.Text.Json;

namespace Valuator.Pages;

public class CountryInfo
{
    public string? Name { get; set; }
    public string? Region { get; set; }
}

public class IndexModel : PageModel
{
    private readonly ILogger<IndexModel> _logger;
    private readonly IDatabase _mainDb;
    private readonly IShardRedisService _shardedRedis;
    private readonly IConnection _rabbitConnection;
    private const string ExchangeName = "valuator.processing.rank";
    private const string QueueName = "valuator.processing.rank";
    private const string EventsExchangeName = "valuator.events";

    public List<CountryInfo> Countries { get; set; } = new()
    {
        new CountryInfo { Name = "Russia", Region = "RU" },
        new CountryInfo { Name = "France", Region = "EU" },
        new CountryInfo { Name = "Germany", Region = "EU" },
        new CountryInfo { Name = "UAE", Region = "ASIA" },
        new CountryInfo { Name = "India", Region = "ASIA" }
    };

    [BindProperty]
    public string? SelectedCountry { get; set; }

    public IndexModel(
        ILogger<IndexModel> logger, 
        IConnectionMultiplexer mainRedis, 
        IShardRedisService shardedRedis, 
        IConnection rabbitConnection
    )
    {
        _logger = logger;
        _mainDb = mainRedis.GetDatabase();
        _shardedRedis = shardedRedis;
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

            if (string.IsNullOrEmpty(text) || string.IsNullOrEmpty(SelectedCountry))
            {
                return RedirectToPage();
            }

            string region = _shardedRedis.GetRegionForCountry(SelectedCountry);
            _logger.LogInformation("Index LOOKUP: {Id}, {Region}", id, region.ToUpper());

            var shardMapKey = $"SHARD-{id}";
            await _mainDb.StringSetAsync(shardMapKey, region);
            var regionDb = _shardedRedis.GetDatabaseForRegion(region);

            // TODO: (pa1) посчитать similarity и сохранить в БД (Redis) по ключу similarityKey
            string similarityKey = "SIMILARITY-" + id;
            // int similarity = CheckSimilarity(text);
            int similarity = CheckRegionSimilarity(text, regionDb);
            regionDb.StringSet(similarityKey, similarity.ToString());
            await PublishSimilarityCalculatedEvent(id, similarity);

            // TODO: (pa1) сохранить в БД (Redis) text по ключу textKey
            string textKey = "TEXT-" + id;
            regionDb.StringSet(textKey, text);

            // TODO: (pa1) посчитать rank и сохранить в БД (Redis) по ключу rankKey
            string rankKey = "RANK-" + id;
            regionDb.StringSet(rankKey, "processing");
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
        var allDatabases = _shardedRedis.GetAllRegionalDatabases();

        foreach (var regionDb in allDatabases.Values)
        {
            int regionSimilarity = CheckRegionSimilarity(text, regionDb);
            if (regionSimilarity == 1)
            {
                return regionSimilarity;
            }
        }

        return 0;
    }

    private int CheckRegionSimilarity(string text, IDatabase regionDb)
    {
        var server = regionDb.Multiplexer.GetServer(regionDb.Multiplexer.GetEndPoints()[0]);
        var keys = server.Keys(pattern: "TEXT-*");
        
        foreach (var key in keys)
        {
            var storedValue = regionDb.StringGet(key);
            if (storedValue.HasValue && storedValue.ToString() == text) {
                return 1;
            }                    
        }
        
        return 0;
    }
}
