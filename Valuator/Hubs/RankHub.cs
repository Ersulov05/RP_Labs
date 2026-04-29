using Microsoft.AspNetCore.SignalR;

namespace Valuator.Hubs;

public class RankHub : Hub
{
    private readonly ILogger<RankHub> _logger;
    
    public RankHub(ILogger<RankHub> logger)
    {
        _logger = logger;
    }

    public override async Task OnConnectedAsync()
    {
        _logger.LogInformation($"Client connected: {Context.ConnectionId}");
        await base.OnConnectedAsync();
    }
    
    public async Task SubscribeToRankUpdates(string id)
    {
        _logger.LogInformation($"Client subscribed to rank updates for ID: {id}");
        await Groups.AddToGroupAsync(Context.ConnectionId, $"rank-{id}");
    }
    
    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation($"Client disconnected: {Context.ConnectionId}");
        await base.OnDisconnectedAsync(exception);
    }
}