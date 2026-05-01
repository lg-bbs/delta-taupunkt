using Microsoft.EntityFrameworkCore;
using TauLuftAspire.ApiService.Database;
using TauLuftAspire.Model.Entity;

namespace TauLuftAspire.ApiService.Endpoints;

public static class MeasurementEndpoints
{
    public static void MapMeasurementEndpoints(this IEndpointRouteBuilder routes)
    {
        var group = routes.MapGroup("/api/measurements").WithTags("Measurements");
        group.MapGet("/", GetAllMeasurements);
        group.MapGet("/insert", InsertMeasurement);
        group.MapGet("/insert/test", InsertTestMeasurements);
    }

    private static async Task<List<Measurement>> GetAllMeasurements(TauLuftDbContext db, DateTime from, DateTime to, int limit = 100)
    {
        var fromUtc = DateTime.SpecifyKind(from, DateTimeKind.Utc);
        var toUtc = DateTime.SpecifyKind(to, DateTimeKind.Utc);

        return await db.Measurement
            .OrderByDescending(e => e.Timestamp)
            .Where(e => e.Timestamp >= fromUtc && e.Timestamp <= toUtc)
            .Take(limit)
            .ToListAsync();
    }

    private static async Task InsertMeasurement(TauLuftDbContext db, double insideTemp, double insideHum, double insideDew, double outsideTemp, double outsideHum, double outsideDew, bool isFanRunning)
    {
        db.Measurement.Add(new Measurement
        {
            Timestamp = DateTime.UtcNow,
            Inside = new SingleMeasurement 
            { 
                Temperature = insideTemp, 
                Humidity = insideHum, 
                DewPoint = insideDew
            },
            Outside = new SingleMeasurement 
            { 
                Temperature = outsideTemp, 
                Humidity = outsideHum, 
                DewPoint = outsideDew 
            },
            IsFanRunning = isFanRunning
        });
        await db.SaveChangesAsync();
    }

    private static async Task InsertTestMeasurements(TauLuftDbContext db)
    {
        db.Measurement.AddRange(GenerateLargeBatchTestData());
        await db.SaveChangesAsync();
    }

    private static List<Measurement> GenerateLargeBatchTestData()
    {
        // Generiert Daten für die letzten 7 Tage alle 4 Stunden
        var list = new List<Measurement>();
        var start = DateTime.UtcNow.AddDays(-7);

        for (int i = 0; i < 50; i++)
        {
            var time = start.AddHours(i * 4);
            var tempIn = 20.0 + Math.Sin(i * 0.5); // Leichtes Schwingen
            var humIn = 55.0 + Math.Cos(i * 0.5) * 5;

            var tempOut = 10.0 + Math.Sin(i * 0.3) * 8;
            var humOut = 70.0 + Math.Cos(i * 0.2) * 20;

            list.Add(new Measurement
            {
                Timestamp = time,
                Inside = new SingleMeasurement { Temperature = Math.Round(tempIn, 1), Humidity = Math.Round(humIn, 1), DewPoint = Math.Round(tempIn - 8, 1) },
                Outside = new SingleMeasurement { Temperature = Math.Round(tempOut, 1), Humidity = Math.Round(humOut, 1), DewPoint = Math.Round(tempOut - 5, 1) },
                IsFanRunning = (tempIn - 8) > (tempOut - 5) // Einfache Logik für Testdaten
            });
        }

        return list;
    }
}
