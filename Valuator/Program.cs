using StackExchange.Redis;
using RabbitMQ.Client;  
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Configuration;
using Valuator.Hubs;
using Valuator.Services;
using Shard;

namespace Valuator;

public class Program
{
    public static async Task Main(string[] args)
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var dbMain = Environment.GetEnvironmentVariable("DB_MAIN") ?? "localhost:6379";
        var dbRu = Environment.GetEnvironmentVariable("DB_RU") ?? "localhost:6380";
        var dbEu = Environment.GetEnvironmentVariable("DB_EU") ?? "localhost:6381";
        var dbAsia = Environment.GetEnvironmentVariable("DB_ASIA") ?? "localhost:6382";

        var rabbitHost = config["RabbitMQ:HostName"];
        var rabbitUser = config["RabbitMQ:UserName"];
        var rabbitPassword = config["RabbitMQ:Password"];

        var builder = WebApplication.CreateBuilder(args);

        // Add services to the container.
        builder.Services.AddRazorPages();

        builder.Services
            .AddDataProtection()
            .PersistKeysToFileSystem(new DirectoryInfo("/keys"))
            .SetApplicationName("ValuatorApp");

        builder.Services.AddSingleton<IConnectionMultiplexer>(sp => 
            ConnectionMultiplexer.Connect(dbMain));

        builder.Services.AddSingleton<IShardRedisService>(sp => 
            new ShardRedisService(dbRu, dbEu, dbAsia));


        var factory = new ConnectionFactory
        {
            HostName = rabbitHost,
            UserName = rabbitUser,
            Password = rabbitPassword,
            AutomaticRecoveryEnabled = true
        };
        
        var connection = await factory.CreateConnectionAsync();
        builder.Services.AddSingleton<IConnection>(connection);

        builder.Services.AddSignalR();
        builder.Services.AddHostedService<RankNotificationService>();

        var app = builder.Build();

        // Configure the HTTP request pipeline.
        if (!app.Environment.IsDevelopment())
        {
            app.UseExceptionHandler("/Error");
        }
        app.UseStaticFiles();

        app.UseRouting();

        app.UseAuthorization();

        app.MapRazorPages();
        app.MapHub<RankHub>("/rankHub"); 

        app.Run();
    }
}