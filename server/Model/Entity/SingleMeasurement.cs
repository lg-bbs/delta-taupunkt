using System.ComponentModel.DataAnnotations.Schema;

namespace TauLuftAspire.Model.Entity;

[ComplexType]
public class SingleMeasurement
{
    public double Temperature { get; set; }
    public double Humidity { get; set; }
    public double DewPoint { get; set; }
}
