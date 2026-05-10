using TauLuftAspire.Model.Enum;

namespace TauLuftAspire.Model.Entity;

public class LogEntry
{
    public int Id { get; set; }
    public LogSeverity Severity { get; set; } = LogSeverity.Unknown;
    public DateTime Timestamp { get; set; }
    public string? Source { get; set; }
    public string? Message { get; set; }
    public string? Details { get; set; }
    public bool HaveRead { get; set; }

    public DateTime LocalTimestamp => Timestamp.ToLocalTime();

    public bool IsSystem => !IsDht && !IsLcd && !IsFan && !IsInsideDht && !IsOutsideDht;
    public bool IsLcd => Source == "LCD";
    public bool IsFan => Source == "Fan";
    public bool IsInsideDht => Source == "InDHT";
    public bool IsOutsideDht => Source == "OutDHT";
    public bool IsDht => Source == "DHT";

    public LogEntry(string message, string details, string source, LogSeverity severity, DateTime timestamp, bool read = true)
    {
        Severity = severity;
        Timestamp = timestamp;
        Source = source;
        Message = message;
        Details = details;
        HaveRead = read;
    }

    public LogEntry() { }
}