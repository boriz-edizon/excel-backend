using Microsoft.AspNetCore.SignalR;

namespace ExcelApi.Hubs
{
    public class ProgressHub : Hub
    {
        public async Task SendMessage(string user, string message)
        {
            await Clients.All.SendAsync("ReceiveMessage", user, message);
        }
    }
}