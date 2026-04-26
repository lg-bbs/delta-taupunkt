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
        group.MapGet("/insert", InsertLog);
    }

    private static async Task<List<LogEntry>> GetAllLogs(TauLuftDbContext db, int limit = 100)
    {
        return db.LogEntry
            .OrderByDescending(e => e.Timestamp)
            .Take(limit)
            .ToList();
        //return LogMock()
        //    .Take(limit)
        //    .ToList();
    }

    private static async Task<List<LogEntry>> GetNewLogs(TauLuftDbContext db)
    {
        return LogMock()
            .Where(e => !e.HaveRead && e.Severity >= LogSeverity.Warning)
            .ToList();
    }

    private static async Task InsertLog(TauLuftDbContext db, string message)
    {
        db.LogEntry.Add(new LogEntry
        {
            Timestamp = DateTime.UtcNow,
            Source = "System",
            Message = message,
            Details = "",
            Severity = LogSeverity.Info,
            HaveRead = false
        });
        await db.SaveChangesAsync();
    }

    private static List<LogEntry> LogMock() => [
        // --- TAG 1: SYSTEMSTART & INITIALISIERUNG (vor 48h) ---
        new(0, "System Boot", "Raspberry Pi Kernel 5.10 gestartet. Initialisiere Module...", "System", LogSeverity.Info, DateTime.Now.AddHours(-48)),
        new(1, "Datenbank Verbindung", "SQLite DB 'sensor_data.db' erfolgreich verbunden (WAL-Mode).", "Core", LogSeverity.Debug, DateTime.Now.AddHours(-47.9)),
        new(2, "GPIO Init", "Pins 17, 27, 22 als Output konfiguriert. I2C Bus aktiv.", "GPIO", LogSeverity.Debug, DateTime.Now.AddHours(-47.8)),
        new(3, "Netzwerk", "WLAN verbunden mit SSID 'IoT-Netz'. IP: 192.168.178.45", "Network", LogSeverity.Info, DateTime.Now.AddHours(-47.7)),
        new(4, "Dienst Start", "Hintergrund-Worker für Sensorik gestartet.", "Worker", LogSeverity.Info, DateTime.Now.AddHours(-47.6)),

        // --- TAG 1: NORMALBETRIEB (vor 30-40h) ---
        new(5, "Messung Init", "Erste Referenzwerte: Innen 20°C / 55%, Außen 12°C / 80%.", "Sensorik", LogSeverity.Info, DateTime.Now.AddHours(-40)),
        new(6, "Taupunkt-Logik", "Delta TP: 3.5K. Schwelle überschritten. Starte Lüfter.", "Logic", LogSeverity.Info, DateTime.Now.AddHours(-39.5)),
        new(7, "Motor Status", "PWM Signal auf 80% gesetzt. Drehzahl 1200 RPM.", "Motor", LogSeverity.Debug, DateTime.Now.AddHours(-39.5)),
        new(8, "Display Update", "LCD 20x4 aktualisiert. Seite: Übersicht.", "Display", LogSeverity.Debug, DateTime.Now.AddHours(-38)),
        new(9, "Systemstatus OK", "Alle Dienste laufen im Normalbereich. Auslastung Pi: 12%.", "Pi", LogSeverity.Debug, DateTime.Now.AddHours(-30)),

        // --- TAG 2: KLEINE STÖRUNGEN (vor 24h) ---
        new(10, "Latenz Warnung", "Schreibzugriff auf SD-Karte dauerte ungewöhnlich lang (800ms).", "Storage", LogSeverity.Warning, DateTime.Now.AddHours(-24)),
        new(11, "Verbindung verloren", "Wetter-API TimeOut nach 5000ms.", "Network", LogSeverity.Warning, DateTime.Now.AddHours(-23.5)),
        new(12, "Verbindung wiederhergestellt", "Wetter-API wieder erreichbar.", "Network", LogSeverity.Info, DateTime.Now.AddHours(-23.4)),
        new(13, "Lüfter Stop", "Taupunkt-Differenz zu gering (< 2K). Schalte ab.", "Logic", LogSeverity.Info, DateTime.Now.AddHours(-20)),
        new(14, "Config Änderung", "Benutzer 'Admin' hat Hysterese von 0.5 auf 1.0 geändert.", "Auth", LogSeverity.Warning, DateTime.Now.AddHours(-18)),

        // --- TAG 2: DER SENSOR-AUSFALL (Der "Zwischenfall" zum Testen von Error-Filtern) ---
        new(15, "I2C Lesefehler", "Keine Antwort von Adresse 0x76 (BME280 Außen).", "GPIO", LogSeverity.Error, DateTime.Now.AddHours(-12), false),
        new(16, "Retry Logik", "Versuch 1/3: Sensor Reset initiiert...", "Core", LogSeverity.Warning, DateTime.Now.AddHours(-11.9), false),
        new(17, "I2C Lesefehler", "Weiterhin keine Antwort von Sensor Außen.", "GPIO", LogSeverity.Error, DateTime.Now.AddHours(-11.8), false),
        new(18, "Notabschaltung", "Sicherheits-Logik: Lüftung deaktiviert wegen fehlender Außenwerte.", "Safety", LogSeverity.Critical, DateTime.Now.AddHours(-11.8), false),
        new(19, "Hardware Watchdog", "Sensor-Bus wird neu gestartet (Power Cycle).", "System", LogSeverity.Warning, DateTime.Now.AddHours(-11.7), false),
        new(20, "Recovery", "Sensor Außen wieder online. Werte plausibel.", "Sensorik", LogSeverity.Info, DateTime.Now.AddHours(-11.5), false),

        // --- HEUTE: AKTUELLE ENTWICKLUNG (letzte 6 Stunden) ---
        new(21, "Wartung", "Automatisches DB-Vakuum durchgeführt. Größe: 4MB.", "Storage", LogSeverity.Debug, DateTime.Now.AddHours(-6), false),
        new(22, "Hohe Feuchtigkeit", "Innenraumfeuchte kritisch: 68%. Lüftung max.", "Logic", LogSeverity.Warning, DateTime.Now.AddHours(-5), false),
        new(23, "Motor Boost", "Lüfter auf 100% (Boost Mode) gestellt.", "Motor", LogSeverity.Info, DateTime.Now.AddHours(-5), false),
        new(24, "Taupunkt-Berechnung", "Delta: 4.2K. Belüftung wird fortgesetzt.", "Core", LogSeverity.Debug, DateTime.Now.AddHours(-4), false),
        new(25, "Nachtmodus", "Zeitplan aktiv: 22:00 Uhr. Begrenze Drehzahl auf 30%.", "Logic", LogSeverity.Info, DateTime.Now.AddHours(-2), false),
        
        // --- JETZT (Die neuesten Einträge) ---
        new(26, "Filter Check", "Betriebsstunden > 1000h. Bitte Filter prüfen.", "Maintenance", LogSeverity.Warning, DateTime.Now.AddMinutes(-45), false),
        new(27, "API Zugriff", "GET /api/status von IP 192.168.178.20", "Web", LogSeverity.Debug, DateTime.Now.AddMinutes(-10), false),
        new(28, "Temperatur Innen", "Messwert: 21.4°C (stabil).", "Sensorik", LogSeverity.Info, DateTime.Now.AddMinutes(-5), false),
        new(29, "Heartbeat", "System läuft seit 48h 0m stabil.", "System", LogSeverity.Debug, DateTime.Now.AddMinutes(-1), false)
    ];
}
