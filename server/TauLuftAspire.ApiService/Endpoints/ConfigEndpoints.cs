using Microsoft.EntityFrameworkCore;
using TauLuftAspire.ApiService.Database;
using TauLuftAspire.Model.Entity;

namespace TauLuftAspire.ApiService.Endpoints;

public static class ConfigEndpoints
{
    public static void MapConfigEndpoints(this IEndpointRouteBuilder routes)
    {
        var group = routes.MapGroup("/api/config").WithTags("Config");
        group.MapGet("/", GetConfig);
        group.MapPost("/", PostConfig);
    }

    private static async Task<Config> GetConfig(TauLuftDbContext db)
    {
        var config = await db.Config.FirstOrDefaultAsync();
        if (config == null)
        {
            config = new Config();
            db.Config.Add(config);
            await db.SaveChangesAsync();
        }
        return config;
    }

    private static async Task<IResult> PostConfig(TauLuftDbContext db, Config config)
    {
        var existingConfig = await GetConfig(db);
        existingConfig.Interval = config.Interval;
        existingConfig.TargetDelta = config.TargetDelta;
        existingConfig.Hysrerese = config.Hysrerese;
        existingConfig.MinTempInside = config.MinTempInside;
        existingConfig.MinTempOutside = config.MinTempOutside;
        existingConfig.TempInsideOffset = config.TempInsideOffset;
        existingConfig.HumInsideOffset = config.HumInsideOffset;
        existingConfig.TempOutsideOffset = config.TempOutsideOffset;
        existingConfig.HumOutsideOffset = config.HumOutsideOffset;
        await db.SaveChangesAsync();

        return Results.NoContent();
    }
}
