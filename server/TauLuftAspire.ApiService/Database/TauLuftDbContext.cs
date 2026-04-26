using Microsoft.EntityFrameworkCore;
using TauLuftAspire.Model.Entity;

namespace TauLuftAspire.ApiService.Database;

public class TauLuftDbContext : DbContext
{
    public TauLuftDbContext(DbContextOptions<TauLuftDbContext> options) : base(options) { }

    public DbSet<LogEntry> LogEntry => Set<LogEntry>();
    public DbSet<Measurement> Measurement => Set<Measurement>();
}
