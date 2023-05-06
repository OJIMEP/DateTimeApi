using Microsoft.Extensions.Configuration;

namespace DateTime.Application
{
    public static class AppSettings
    {
        public static string? Environment { get; private set; }

        public static void Initialize(IConfiguration configuration)
        {
            Environment = configuration.GetValue<string>("loggerEnv");
        }
    }
}
