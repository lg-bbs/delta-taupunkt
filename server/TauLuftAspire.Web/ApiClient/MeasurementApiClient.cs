using TauLuftAspire.Model.Entity;

namespace TauLuftAspire.Web.ApiClient;

public class MeasurementApiClient(HttpClient httpClient)
{
    public async Task<List<Measurement>> GetMeasurementsAsync(DateTime from, DateTime to)
    {
        return await httpClient.GetFromJsonAsync<List<Measurement>>($"/api/measurements?from={from.ToUniversalTime():s}&to={to.ToUniversalTime():s}") ?? [];
    }

    public async Task<Measurement?> GetCurrentMeasurement()
    {
        return await httpClient.GetFromJsonAsync<Measurement?>($"/api/measurements/current");
    }

    public async Task PostInsertTestMeasurementsAsync()
    {
        await httpClient.PostAsync($"/api/measurements/test", null);
    }

    public async Task DeleteAll()
    {
        await httpClient.DeleteAsync($"/api/measurements");
    }
}