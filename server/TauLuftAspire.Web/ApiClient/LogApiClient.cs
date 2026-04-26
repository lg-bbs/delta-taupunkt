using TauLuftAspire.Model.Entity;

namespace TauLuftAspire.Web.ApiClient;

public class LogApiClient(HttpClient httpClient)
{
    public async Task<List<LogEntry>> GetNewLogsAsync()
    {
        return await httpClient.GetFromJsonAsync<List<LogEntry>>("/api/logs/new") ?? [];
    }

    public async Task<List<LogEntry>> GetAllLogsAsync()
    {
        return await httpClient.GetFromJsonAsync<List<LogEntry>>("/api/logs/") ?? [];
    }
}