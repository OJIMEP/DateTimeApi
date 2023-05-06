using StackExchange.Redis;
using System.Text.Json;

namespace DateTime.Application.Cache
{
    public static class DatabaseExtensions
    {
        public static async Task SetRecord<T>(this IDatabase cache, string recordId, T record, TimeSpan? absoluteExpireTime = null)
        {
            var jsonData = JsonSerializer.Serialize(record);
            await cache.StringSetAsync(recordId, jsonData, absoluteExpireTime);
        }

        public static async Task<T> GetRecord<T>(this IDatabase cache, string recordId)
        {
            var jsonData = await cache.StringGetAsync(recordId);

            if (jsonData.IsNull)
            {
                return default;
            }

            return JsonSerializer.Deserialize<T>(jsonData);
        }

        public static async Task<T[]> GetRecords<T>(this IDatabase cache, RedisKey[] redisKeys)
        {
            var values = await cache.StringGetAsync(redisKeys);

            var result = new T[values.Length];

            for (int i = 0; i < values.Length; i++)
            {
                var value = values[i];
                if (value.HasValue)
                {
                    var record = JsonSerializer.Deserialize<T>(value);
                    result[i] = record;
                }
            }

            return result;
        }
    }
}
