using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;

namespace DateTimeService.Application.Database.DatabaseManagement
{
    public class DatabaseErrorTracker
    {
        private readonly IMemoryCache _cache;
        private readonly IReadableDatabase _readableDatabaseService;
        private readonly IConfiguration _configuration;
        private readonly int _maxErrorsPerMinute;

        public DatabaseErrorTracker(IMemoryCache cache, IReadableDatabase readableDatabaseService, IConfiguration configuration)
        {
            _cache = cache;
            _readableDatabaseService = readableDatabaseService;
            _configuration = configuration;
            _maxErrorsPerMinute = _configuration.GetValue<int>("ErrorsCountForLowPriority");
        }

        // Увеличиваем счётчик ошибок для базы
        public void ReportError(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                return;
            }

            // Получаем текущее количество ошибок
            var errorCount = _cache.GetOrCreate(connectionString, entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(1); // Сброс через 1 минуту
                return 0;
            });

            errorCount++;
            
            // Если превышен лимит — устанавливаем пониженный приоритет
            if (errorCount >= _maxErrorsPerMinute)
            {
                _readableDatabaseService.SetPriorityCoefficient(connectionString, 0.5);
                errorCount = 0;
            }

            _cache.Set(connectionString, errorCount);
        }
    }
}
