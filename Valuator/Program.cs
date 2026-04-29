using StackExchange.Redis;
using RabbitMQ.Client;  
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Configuration;
using Valuator.Hubs;
using Valuator.Services;

namespace Valuator;

public class Program
{
    public static async Task Main(string[] args)
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var redisConnection = config["Redis:ConnectionString"];
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
            ConnectionMultiplexer.Connect(redisConnection));

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