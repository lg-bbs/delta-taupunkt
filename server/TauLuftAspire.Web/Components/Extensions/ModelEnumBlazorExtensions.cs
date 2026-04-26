using MudBlazor;
using TauLuftAspire.Model.Enum;

namespace TauLuftAspire.Web.Components.Extensions;

public static class LogSeverityBlazorExtensions
{
    extension(LogSeverity severity)
    {
        public Color MudColor => severity switch
        {
            LogSeverity.Info => Color.Info,
            LogSeverity.Success => Color.Success,
            LogSeverity.Warning => Color.Warning,
            LogSeverity.Error => Color.Error,
            LogSeverity.Critical => Color.Error,
            _ => Color.Default,
        };

        public string MudIcon => severity switch
        {
            LogSeverity.Debug => Icons.Material.Filled.BugReport,
            LogSeverity.Info => Icons.Material.Filled.Info,
            LogSeverity.Success => Icons.Material.Filled.CheckCircle,
            LogSeverity.Warning => Icons.Material.Filled.Warning,
            LogSeverity.Error => Icons.Material.Filled.ReportGmailerrorred,
            LogSeverity.Critical => Icons.Material.Filled.ReportGmailerrorred,
            _ => Icons.Material.Filled.HelpOutline
        };
    }
}
