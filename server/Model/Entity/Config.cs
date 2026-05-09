namespace TauLuftAspire.Model.Entity;

public class Config
{
    public int Id { get; set; }
    public int Interval { get; set; } = 3;
    public double TargetDelta { get; set; } = 5;
    public double Hysrerese { get; set; } = 1;
    public double MinTempInside { get; set; } = 10;
    public double MinTempOutside { get; set; } = -10;
    public double TempInsideOffset { get; set; } = 0;
    public double HumInsideOffset { get; set; } = 0;
    public double TempOutsideOffset { get; set; } = 0;
    public double HumOutsideOffset { get; set; } = 0;
}
