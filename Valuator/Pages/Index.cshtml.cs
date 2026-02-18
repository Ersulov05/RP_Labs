using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

using StackExchange.Redis;

namespace Valuator.Pages;

public class IndexModel : PageModel
{
    private readonly ILogger<IndexModel> _logger;
    private readonly IDatabase _redisDb;

    public IndexModel(ILogger<IndexModel> logger, IConnectionMultiplexer redis)
    {
        _logger = logger;
        _redisDb = redis.GetDatabase();
    }

    public void OnGet()
    {
    }

    public IActionResult OnPost(string text)
    {
        try
        {
            _logger.LogDebug(text);

            string id = Guid.NewGuid().ToString();

            if (string.IsNullOrEmpty(text)) {
                return RedirectToPage();
            }

            string similarityKey = "SIMILARITY-" + id;
            // TODO: (pa1) посчитать similarity и сохранить в БД (Redis) по ключу similarityKey
            int similarity = CheckSimilarity(text);
            _redisDb.StringSet(similarityKey, similarity.ToString());

            string textKey = "TEXT-" + id;
            // TODO: (pa1) сохранить в БД (Redis) text по ключу textKey
            _redisDb.StringSet(textKey, text);

            string rankKey = "RANK-" + id;
            // TODO: (pa1) посчитать rank и сохранить в БД (Redis) по ключу rankKey
            double rank = CalculateRank(text);
            _redisDb.StringSet(rankKey, rank.ToString());        

            return Redirect($"summary?id={id}");
        }
        catch (Exception ex)
        {
            return RedirectToPage("Error", new { message = ex.Message });
        } 
    }

    private double CalculateRank(string text)
    {
        int totalChars = text.Length;
        int nonAlphabeticChars = 0;

        foreach (char c in text)
        {
            if (!char.IsLetter(c))
                nonAlphabeticChars++;
        }
        
        return totalChars > 0 
            ? (double)nonAlphabeticChars / totalChars 
            : 0;
    }

    private int CheckSimilarity(string text)
    {
        var server = _redisDb.Multiplexer.GetServer("localhost", 6379);
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
