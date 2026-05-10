namespace TauLuftAspire.Model.Entity;

public class Measurement
{
    public int Id { get; set; }
    public DateTime Timestamp { get; set; }
    public SingleMeasurement Inside { get; set; }
    public SingleMeasurement Outside { get; set; }
    public bool IsFanRunning { get; set; }

    public DateTime LocalTimestamp => Timestamp.ToLocalTime();

    public double DeltaTp => Inside.DewPoint - Outside.DewPoint;
}
