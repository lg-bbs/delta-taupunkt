using Aspire.Hosting.Docker.Resources.ServiceNodes.Swarm;
using TauLuftAspire.Model.Entity;
using static System.Net.WebRequestMethods;

namespace TauLuftAspire.Web.ApiClient;

public class ConfigApiClient(HttpClient httpClient)
{
    public async Task<Config> GetConfigAsync()
    {
        return await httpClient.GetFromJsonAsync<Config>($"/api/config") ?? new Config();
    }

    public async Task PostConfigAsync(Config config)
    {
        await httpClient.PostAsJsonAsync("api/config", config);
    }
}