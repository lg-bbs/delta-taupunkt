using TauLuftAspire.Model.Entity;
using TauLuftAspire.Model.Enum;

namespace TauLuftAspire.Web.ApiClient;

public class LogApiClient(HttpClient httpClient)
{
    public async Task<List<LogEntry>> GetNewLogsAsync()
    {
        return await httpClient.GetFromJsonAsync<List<LogEntry>>("/api/logs/new") ?? [];
    }

    public async Task<List<LogEntry>> GetAllLogsAsync(int minSeverity)
    {
        return await httpClient.GetFromJsonAsync<List<LogEntry>>($"/api/logs?minSeverity={minSeverity}") ?? [];
    }

    public async Task PostAllReadAsync()
    {
        await httpClient.PostAsync("/api/logs/read", null);
    }

    public async Task PostInsertTestLogs()
    {
        await httpClient.PostAsync("/api/logs/test", null);
    }
}