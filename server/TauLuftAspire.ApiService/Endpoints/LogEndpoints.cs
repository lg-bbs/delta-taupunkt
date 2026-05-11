using Microsoft.EntityFrameworkCore;
using TauLuftAspire.ApiService.Database;
using TauLuftAspire.Model.Entity;
using TauLuftAspire.Model.Enum;

namespace TauLuftAspire.ApiService.Endpoints;

public static class LogEndpoints
{
    public static void MapLogEndpoints(this IEndpointRouteBuilder routes)
    {
        var group = routes.MapGroup("/api/logs").WithTags("Logs");
        group.MapGet("/", GetAllLogs);
        group.MapGet("/new", GetNewLogs);
        group.MapPost("/", InsertLog);
        group.MapPost("/test", InsertTestLogs);
        group.MapPost("/read", LogsRead);
        group.MapDelete("/", DeleteAll);
    }

    private static async Task<IResult> DeleteAll(TauLuftDbContext db)
    {
        await db.LogEntry.ExecuteDeleteAsync();
        return Results.Ok();
    }

    private static async Task<List<LogEntry>> GetAllLogs(TauLuftDbContext db, DateTime from, DateTime to, int minSeverity = (int)LogSeverity.Info)
    {
        var fromUtc = DateTime.SpecifyKind(from, DateTimeKind.Utc);
        var toUtc = DateTime.SpecifyKind(to, DateTimeKind.Utc);

        return await db.LogEntry
            .Where(e => e.Severity >= (LogSeverity)minSeverity && e.Timestamp >= fromUtc && e.Timestamp <= toUtc)
            .OrderByDescending(e => e.Timestamp)
            .ToListAsync();
    }

    private static async Task<List<LogEntry>> GetNewLogs(TauLuftDbContext db)
    {
        return await db.LogEntry
            .OrderByDescending(e => e.Timestamp)
            .Where(e => !e.HaveRead && e.Severity >= LogSeverity.Warning)
            .ToListAsync();
    }

    private static async Task LogsRead(TauLuftDbContext db)
    {
        await db.LogEntry.Where(e => !e.HaveRead).ForEachAsync(e => e.HaveRead = true);
        await db.SaveChangesAsync();
    }

    private static async Task InsertLog(TauLuftDbContext db, string message, int severity = (int)LogSeverity.Info, string source = "System", string details = "")
    {
        db.LogEntry.Add(new LogEntry
        {
            Timestamp = DateTime.UtcNow,
            Source = source,
            Message = message,
            Details = details,
            Severity = (LogSeverity)severity,
            HaveRead = false
        });
        await db.SaveChangesAsync();
    }

    private static async Task InsertTestLogs(TauLuftDbContext db)
    {
        db.LogEntry.AddRange(LogMock());
        await db.SaveChangesAsync();
    }

    private static List<LogEntry> LogMock() => [
        // --- TAG 1: SYSTEMSTART & INITIALISIERUNG (vor 48h) ---
        new("System Boot", "Raspberry Pi Kernel 5.10 gestartet. Initialisiere Module...", "System", LogSeverity.Info, DateTime.UtcNow.AddHours(-48)),
        new("Datenbank Verbindung", "SQLite DB 'sensor_data.db' erfolgreich verbunden (WAL-Mode).", "Core", LogSeverity.Debug, DateTime.UtcNow.AddHours(-47.9)),
        new("GPIO Init", "Pins 17, 27, 22 als Output konfiguriert. I2C Bus aktiv.", "GPIO", LogSeverity.Debug, DateTime.UtcNow.AddHours(-47.8)),
        new("Netzwerk", "WLAN verbunden mit SSID 'IoT-Netz'. IP: 192.168.178.45", "Network", LogSeverity.Info, DateTime.UtcNow.AddHours(-47.7)),
        new("Dienst Start", "Hintergrund-Worker für Sensorik gestartet.", "Worker", LogSeverity.Info, DateTime.UtcNow.AddHours(-47.6)),

        // --- TAG 1: NORMALBETRIEB (vor 30-40h) ---
        new("Messung Init", "Erste Referenzwerte: Innen 20°C / 55%, Außen 12°C / 80%.", "Sensorik", LogSeverity.Info, DateTime.UtcNow.AddHours(-40)),
        new("Taupunkt-Logik", "Delta TP: 3.5K. Schwelle überschritten. Starte Lüfter.", "Logic", LogSeverity.Info, DateTime.UtcNow.AddHours(-39.5)),
        new("Motor Status", "PWM Signal auf 80% gesetzt. Drehzahl 1200 RPM.", "Motor", LogSeverity.Debug, DateTime.UtcNow.AddHours(-39.5)),
        new("Display Update", "LCD 20x4 aktualisiert. Seite: Übersicht.", "Display", LogSeverity.Debug, DateTime.UtcNow.AddHours(-38)),
        new("Systemstatus OK", "Alle Dienste laufen im Normalbereich. Auslastung Pi: 12%.", "Pi", LogSeverity.Debug, DateTime.UtcNow.AddHours(-30)),

        // --- TAG 2: KLEINE STÖRUNGEN (vor 24h) ---
        new("Latenz Warnung", "Schreibzugriff auf SD-Karte dauerte ungewöhnlich lang (800ms).", "Storage", LogSeverity.Warning, DateTime.UtcNow.AddHours(-24)),
        new("Verbindung verloren", "Wetter-API TimeOut nach 5000ms.", "Network", LogSeverity.Warning, DateTime.UtcNow.AddHours(-23.5)),
        new("Verbindung wiederhergestellt", "Wetter-API wieder erreichbar.", "Network", LogSeverity.Info, DateTime.UtcNow.AddHours(-23.4)),
        new("Lüfter Stop", "Taupunkt-Differenz zu gering (< 2K). Schalte ab.", "Logic", LogSeverity.Info, DateTime.UtcNow.AddHours(-20)),
        new("Config Änderung", "Benutzer 'Admin' hat Hysterese von 0.5 auf 1.0 geändert.", "Auth", LogSeverity.Warning, DateTime.UtcNow.AddHours(-18)),

        // --- TAG 2: DER SENSOR-AUSFALL (Der "Zwischenfall" zum Testen von Error-Filtern) ---
        new("I2C Lesefehler", "Keine Antwort von Adresse 0x76 (BME280 Außen).", "GPIO", LogSeverity.Error, DateTime.UtcNow.AddHours(-12), false),
        new("Retry Logik", "Versuch 1/3: Sensor Reset initiiert...", "Core", LogSeverity.Warning, DateTime.UtcNow.AddHours(-11.9), false),
        new("I2C Lesefehler", "Weiterhin keine Antwort von Sensor Außen.", "GPIO", LogSeverity.Error, DateTime.UtcNow.AddHours(-11.8), false),
        new("Notabschaltung", "Sicherheits-Logik: Lüftung deaktiviert wegen fehlender Außenwerte.", "Safety", LogSeverity.Critical, DateTime.UtcNow.AddHours(-11.8), false),
        new("Hardware Watchdog", "Sensor-Bus wird neu gestartet (Power Cycle).", "System", LogSeverity.Warning, DateTime.UtcNow.AddHours(-11.7), false),
        new("Recovery", "Sensor Außen wieder online. Werte plausibel.", "Sensorik", LogSeverity.Info, DateTime.UtcNow.AddHours(-11.5), false),

        // --- HEUTE: AKTUELLE ENTWICKLUNG (letzte 6 Stunden) ---
        new("Wartung", "Automatisches DB-Vakuum durchgeführt. Größe: 4MB.", "Storage", LogSeverity.Debug, DateTime.UtcNow.AddHours(-6), false),
        new("Hohe Feuchtigkeit", "Innenraumfeuchte kritisch: 68%. Lüftung max.", "Logic", LogSeverity.Warning, DateTime.UtcNow.AddHours(-5), false),
        new("Motor Boost", "Lüfter auf 100% (Boost Mode) gestellt.", "Motor", LogSeverity.Info, DateTime.UtcNow.AddHours(-5), false),
        new("Taupunkt-Berechnung", "Delta: 4.2K. Belüftung wird fortgesetzt.", "Core", LogSeverity.Debug, DateTime.UtcNow.AddHours(-4), false),
        new("Nachtmodus", "Zeitplan aktiv: 22:00 Uhr. Begrenze Drehzahl auf 30%.", "Logic", LogSeverity.Info, DateTime.UtcNow.AddHours(-2), false),
        
        // --- JETZT (Die neuesten Einträge) ---
        new("Filter Check", "Betriebsstunden > 1000h. Bitte Filter prüfen.", "Maintenance", LogSeverity.Warning, DateTime.UtcNow.AddMinutes(-45), false),
        new("API Zugriff", "GET /api/status von IP 192.168.178.20", "Web", LogSeverity.Debug, DateTime.UtcNow.AddMinutes(-10), false),
        new("Temperatur Innen", "Messwert: 21.4°C (stabil).", "Sensorik", LogSeverity.Info, DateTime.UtcNow.AddMinutes(-5), false),
        new("Heartbeat", "System läuft seit 48h 0m stabil.", "System", LogSeverity.Debug, DateTime.UtcNow.AddMinutes(-1), false)
    ];
}
