using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace Valuator.Pages;
public class SummaryModel : PageModel
{
    private readonly ILogger<SummaryModel> _logger;
    private readonly IDatabase _redisDb;

    public SummaryModel(ILogger<SummaryModel> logger, IConnectionMultiplexer redis)
    {
        _logger = logger;
        _redisDb = redis.GetDatabase();
        Text = "";
    }

    public string Text { get; set; }
    public double Rank { get; set; }
    public double Similarity { get; set; }

    public void OnGet(string id)
    {
        _logger.LogDebug(id);

        // TODO: (pa1) проинициализировать свойства Rank и Similarity значениями из БД (Redis)
        if (!string.IsNullOrEmpty(id))
        {
            string rankKey = "RANK-" + id;
            string similarityKey = "SIMILARITY-" + id;
            string textKey = "TEXT-" + id;
            
            var rankValue = _redisDb.StringGet(rankKey);
            var similarityValue = _redisDb.StringGet(similarityKey);
            var textValue = _redisDb.StringGet(textKey);

            if (textValue.HasValue)
                Text = textValue.ToString();
            
            if (rankValue.HasValue && double.TryParse(rankValue, out double rank))
                Rank = rank;
            else
                Rank = 0;
            
            if (similarityValue.HasValue && double.TryParse(similarityValue, out double similarity))
                Similarity = similarity;
            else
                Similarity = 0;
        }    
    }
}
