using DateTimeService.Application.Logging;
using Microsoft.Extensions.Logging;

namespace DateTimeService.Api
{
    public static class LoggerExtensions
    {
        public static void LogElastic(this ILogger logger, ElasticLogElement logElement)
        {
            logger.LogInformation("{@Message}", logElement);
        }

        public static void LogElastic(this ILogger logger, string message)
        {
            var logElement = new ElasticLogElement
            {
                Status = LogStatus.Error,
                ErrorDescription = message
            };

            logger.LogElastic(logElement);
        }

        public static void LogElastic(this ILogger logger, string message, Exception ex)
        {
            var logElement = new ElasticLogElement
            {
                Status = LogStatus.Error,
                ErrorDescription = $"{message}: {ex.Message}"
            };

            logger.LogElastic(logElement);
        }
    }
}
