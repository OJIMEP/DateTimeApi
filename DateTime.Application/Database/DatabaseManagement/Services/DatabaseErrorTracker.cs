using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using System.Collections.Concurrent;

namespace DateTimeService.Application.Database.DatabaseManagement
{
    public class DatabaseErrorTracker
    {
        private readonly IMemoryCache _cache;
        private readonly IReadableDatabase _readableDatabaseService;
        private readonly IConfiguration _configuration;
        private readonly int _maxErrorsPerMinute;
        private readonly TimeSpan _windowSize = TimeSpan.FromMinutes(1);

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

            if (_maxErrorsPerMinute == 0)
            {
                return;
            }

           var errors = _cache.GetOrCreate(connectionString, entry =>
            {
                entry.SlidingExpiration = _windowSize;
                return new ConcurrentQueue<DateTime>();
            });

            var now = DateTime.UtcNow;
            var cutoffTime = now - _windowSize;
            int maxIterations = Math.Min(_maxErrorsPerMinute * 2, errors.Count); // Защита от бесконечного цикла
            while (maxIterations-- > 0 && errors.TryPeek(out var timestamp) && timestamp < cutoffTime)
            {
                errors.TryDequeue(out _);
            }

            errors.Enqueue(now);

            // Если превышен лимит — устанавливаем пониженный приоритет
            if (errors.Count >= _maxErrorsPerMinute)
            {
                _readableDatabaseService.SetPriorityCoefficient(connectionString, 0.5);
            }
        }
    }
}
