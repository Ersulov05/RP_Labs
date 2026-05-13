using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;
using Shard;


using StackExchange.Redis;

namespace Valuator.Pages;
public class SummaryModel : PageModel
{
    private readonly ILogger<SummaryModel> _logger;
    private readonly IDatabase _mainDb;
    private readonly IShardRedisService _shardedRedis;

    public SummaryModel(
        ILogger<SummaryModel> logger, 
        IConnectionMultiplexer mainRedis, 
        IShardRedisService shardedRedis)
    {
        _logger = logger;
        _mainDb = mainRedis.GetDatabase();
        _shardedRedis = shardedRedis;
        Text = "";
    }

    public string Text { get; set; }
    public string Rank { get; set; }
    public double Similarity { get; set; }
    public string Id { get; set; } 
    public string Region { get; set; }

    public void OnGet(string id)
    {
        _logger.LogDebug(id);
        Id = id;

        // TODO: (pa1) проинициализировать свойства Rank и Similarity значениями из БД (Redis)
        if (!string.IsNullOrEmpty(id))
        {
            string shardMapKey = $"SHARD-{id}";
            string region = _mainDb.StringGet(shardMapKey);
            _logger.LogInformation("Summary LOOKUP: {Id}, {Region}", id, region.ToUpper());
            
            if (string.IsNullOrEmpty(region))
            {
                Rank = "not found";
                Region = "not found";
                Similarity = 0;
                return;
            }

            Region = region;
            var regionDb = _shardedRedis.GetDatabaseForRegion(region);

            string rankKey = "RANK-" + id;
            string similarityKey = "SIMILARITY-" + id;
            string textKey = "TEXT-" + id;
            
            var rankValue = regionDb.StringGet(rankKey);
            var similarityValue = regionDb.StringGet(similarityKey);
            var textValue = regionDb.StringGet(textKey);

            if (textValue.HasValue)
                Text = textValue.ToString();
            
            if (rankValue.HasValue)
                Rank = rankValue.ToString();
            else
                Rank = "not value";
            
            if (similarityValue.HasValue && double.TryParse(similarityValue, out double similarity))
                Similarity = similarity;
            else
                Similarity = 0;
        }    
    }
}
