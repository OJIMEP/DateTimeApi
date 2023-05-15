using DateTimeService.Application.Cache;
using DateTimeService.Application.Models;
using Microsoft.AspNetCore.Http;
using StackExchange.Redis;
using System.Diagnostics;

namespace DateTimeService.Application.Repositories
{
    public class RedisRepository
    {
        private readonly RedisSettings _settings;
        private readonly IConnectionMultiplexer _redis;
        private readonly IHttpContextAccessor _contextAccessor;

        public RedisRepository(RedisSettings settings, IConnectionMultiplexer redis, IHttpContextAccessor contextAccessor)
        {
            _settings = settings;
            _redis = redis;
            _contextAccessor = contextAccessor;
        }

        public async Task SaveAvailableDateResultToCache(AvailableDateResult result, string cityId)
        {
            if (!RedisEnabled())
            {
                return;
            }

            // Время жизни ключей
            var expiry = TimeSpan.FromSeconds(_settings.LifeTime);
            var db = _redis.GetDatabase((int)_settings.Database);

            // Запись пар ключ-значение в Redis
            foreach (var item in result.Data)
            {
                var key = $"{item.Key}-{cityId}";
                await db.SetRecord(key, item.Value, expiry);
            }
        }

        public async Task<AvailableDateResult> GetAvailableDateResultGromCashe(List<CodeItemQuery> queryItems, string cityId)
        {
            var result = new AvailableDateResult();

            if (!RedisEnabled())
            {
                return result;
            }

            var watch = Stopwatch.StartNew();

            var db = _redis.GetDatabase((int)_settings.Database);

            var redisKeys = queryItems
                .Select(item => (RedisKey)$"{item.CacheKey}-{cityId}")
                .ToArray();

            var values = await db.GetRecords<AvailableDateElementResult>(redisKeys);

            for (int i = 0; i < queryItems.Count; i++)
            {
                var value = values[i];
                if (value is not null)
                {
                    var key = queryItems[i].CacheKey;
                    result.Data.Add(key, value);
                }
            }

            watch.Stop();

            _contextAccessor.HttpContext.Items["TimeGettingFromCache"] = watch.ElapsedMilliseconds;

            return result;
        }

        private bool RedisEnabled()
        {
            return _settings.Enabled && _redis.IsConnected;
        }
    }
}
