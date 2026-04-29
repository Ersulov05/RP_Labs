using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Valuator.Hubs;

namespace Valuator.Services;

public class RankNotificationService : BackgroundService
{
    private readonly IConnection _rabbitConnection;
    private readonly IHubContext<RankHub> _hubContext;
    private readonly ILogger<RankNotificationService> _logger;
    private const string EventsExchangeName = "valuator.events";
    private string _queueName = string.Empty;

    public RankNotificationService(
        IConnection rabbitConnection,
        IHubContext<RankHub> hubContext,
        ILogger<RankNotificationService> logger)
    {
        _rabbitConnection = rabbitConnection;
        _hubContext = hubContext;
        _logger = logger;
    }

    private async Task DeclareTopologyAsync(IChannel channel)
    {
        await channel.ExchangeDeclareAsync(
            exchange: EventsExchangeName,
            type: ExchangeType.Topic,
            durable: true);
        
        var queueDeclareResult = await channel.QueueDeclareAsync(
            queue: "",
            durable: false,
            exclusive: true,
            autoDelete: true);

        _queueName = queueDeclareResult.QueueName;
        
        await channel.QueueBindAsync(
            queue: queueDeclareResult.QueueName,
            exchange: EventsExchangeName,
            routingKey: "event.rank.calculated");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var channel = await _rabbitConnection.CreateChannelAsync();
        await DeclareTopologyAsync(channel);
        
        _logger.LogInformation("RankNotificationService started, listening for RankCalculated events");
        
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, eventArgs) => ConsumeAsync(channel, eventArgs);
        
        await channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);
        
        await Task.Delay(-1, stoppingToken);
    }

    private async Task ConsumeAsync(IChannel channel, BasicDeliverEventArgs eventArgs)
    {
        try
        {
            var body = eventArgs.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var eventData = JsonSerializer.Deserialize<Dictionary<string, object>>(message);
            
            if (eventData != null && eventData.TryGetValue("EventType", out var eventTypeObj))
            {
                var eventType = eventTypeObj.ToString();
                if (eventType == "RankCalculated")
                {
                    var entityId = eventData["EntityId"].ToString();
                    var rank = eventData["Rank"].ToString();
                    
                    _logger.LogInformation($"Received RankCalculated event for ID: {entityId}, Rank: {rank}");
                    
                    if (!string.IsNullOrEmpty(entityId))
                    {
                        await _hubContext.Clients.Group($"rank-{entityId}").SendAsync("RankReady", rank);
                    }
                }
            }
            
            await channel.BasicAckAsync(eventArgs.DeliveryTag, false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing RankCalculated event");
            await channel.BasicNackAsync(eventArgs.DeliveryTag, false, true);
        }
    }
}