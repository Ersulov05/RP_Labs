using StackExchange.Redis;

namespace Shard;

public interface IShardRedisService
{
    IDatabase GetDatabaseForRegion(string region);
    IDatabase GetDatabaseForCountry(string country);
    string GetRegionForCountry(string country);

    IReadOnlyDictionary<string, IDatabase> GetAllRegionalDatabases();
}

public class ShardRedisService : IShardRedisService
{
    private readonly Dictionary<string, IConnectionMultiplexer> _regionConnections;
    private readonly Dictionary<string, string> _countryToRegion;

    public ShardRedisService(string dbRu, string dbEu, string dbAsia)
    {
        _regionConnections = new Dictionary<string, IConnectionMultiplexer>
        {
            ["RU"] = ConnectionMultiplexer.Connect(dbRu),
            ["EU"] = ConnectionMultiplexer.Connect(dbEu),
            ["ASIA"] = ConnectionMultiplexer.Connect(dbAsia)
        };

        _countryToRegion = new Dictionary<string, string>
        {
            ["Russia"] = "RU",
            ["France"] = "EU",
            ["Germany"] = "EU",
            ["UAE"] = "ASIA",
            ["India"] = "ASIA"
        };
    }

    public IDatabase GetDatabaseForRegion(string region)
    {
        if (_regionConnections.TryGetValue(region, out var connection))
            return connection.GetDatabase();
        
        throw new ArgumentException($"Unknown region: {region}");
    }

    public IDatabase GetDatabaseForCountry(string country)
    {
        var region = GetRegionForCountry(country);
        return GetDatabaseForRegion(region);
    }

    public string GetRegionForCountry(string country)
    {
        if (_countryToRegion.TryGetValue(country, out var region))
            return region;
        
        throw new ArgumentException($"Unknown country: {country}");
    }

    public IReadOnlyDictionary<string, IDatabase> GetAllRegionalDatabases()
    {
        return _regionConnections.ToDictionary(
            kvp => kvp.Key, 
            kvp => kvp.Value.GetDatabase()
        );
    }
}