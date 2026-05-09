using Microsoft.EntityFrameworkCore;
using TauLuftAspire.ApiService.Database;
using TauLuftAspire.ApiService.Endpoints;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.Services.AddProblemDetails();
builder.Services.AddOpenApi();

builder.AddNpgsqlDbContext<TauLuftDbContext>("tauluftdb");

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<TauLuftDbContext>();
    await db.Database.MigrateAsync();
}

app.UseExceptionHandler();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapDefaultEndpoints();
app.MapLogEndpoints();
app.MapMeasurementEndpoints();
app.MapConfigEndpoints();

app.Run();