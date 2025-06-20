using DateTimeService.Application.Models;
using Microsoft.AspNetCore.Http;

namespace DateTimeService.Application.Repositories
{
    public class DateTimeRepository : IDateTimeRepository
    {
        private readonly RedisRepository _redisRepository;
        private readonly IHttpContextAccessor _contextAccessor;
        private readonly IDatabaseRepository _databaseRepository;

        public DateTimeRepository(IDatabaseRepository databaseRepository, RedisRepository redisRepository, IHttpContextAccessor contextAccessor)
        {
            _contextAccessor = contextAccessor;
            _databaseRepository = databaseRepository;
            _redisRepository = redisRepository;
        }

        public async Task<AvailableDateResult> GetAvailableDateAsync(AvailableDateQuery query, CancellationToken token = default)
        {
            // разбиваем запрос на товары с количеством и без количества, так как для них разные SQL запросы
            AvailableDateQuery.SplitByQuantity(query, out AvailableDateQuery queryWithQuantity, out AvailableDateQuery queryWithoutQuantity);

            AvailableDateResult result = new();

            _contextAccessor.HttpContext.Items["TotalItems"] = query.Codes.Count;

            if (queryWithoutQuantity.Codes.Count > 0)
            {
                var dataFromCache = await _redisRepository.GetAvailableDateResultGromCashe(queryWithoutQuantity.Codes, query.CityId);

                _contextAccessor.HttpContext.Items["FromCache"] = dataFromCache.Data.Count;

                foreach (var item in dataFromCache.Data)
                {
                    result.Data.TryAdd(item.Key, item.Value);
                }

                if (result.Data.Count == query.Codes.Count)
                {
                    return result;
                }

                DeleteCachedDataFromInputData(queryWithoutQuantity.Codes, dataFromCache);
            }

            List<AvailableDateQuery> queryList = new();

            queryList.AddRange(AvailableDateQuery.SplitByCodes(queryWithQuantity));
            queryList.AddRange(AvailableDateQuery.SplitByCodes(queryWithoutQuantity));

            // для получившегося списка запросов запускаем параллельное получение данных
            Task<AvailableDateResult>[] tasksArray = queryList.Select(subquery => Task.Run(() => _databaseRepository.GetAvailableDates(subquery, token))).ToArray();

            var results = await Task.WhenAll(tasksArray);

            foreach (var taskResult in results)
            {
                foreach (var item in taskResult.Data)
                {
                    result.Data.TryAdd(item.Key, item.Value);
                }

                if (!taskResult.WithQuantity)
                {
                    await _redisRepository.SaveAvailableDateResultToCache(taskResult, query.CityId);
                }
            }
            
            return result;
        }

        public async Task<IntervalListResult> GetIntervalListAsync(IntervalListQuery query, CancellationToken token = default)
        {
            IntervalListResult result = await _databaseRepository.GetIntervalList(query, token);

            return result;
        }

        public async Task<AvailableDeliveryTypesResult> GetAvailableDeliveryTypesAsync(AvailableDeliveryTypesQuery query, CancellationToken token = default)
        {
            var result = new AvailableDeliveryTypesResult();

            Task<DeliveryTypeAvailabilityResult> taskSelf;
            Task<DeliveryTypeAvailabilityResult> taskCourier;
            Task<DeliveryTypeAvailabilityResult> taskYourTime;
           
            taskSelf = Task.Run(() => _databaseRepository.GetDeliveryTypeAvailability(query, Constants.Self, token));
            taskCourier = Task.Run(() => _databaseRepository.GetDeliveryTypeAvailability(query, Constants.CourierDelivery, token));
            taskYourTime = Task.Run(() => _databaseRepository.GetDeliveryTypeAvailability(query, Constants.YourTimeDelivery, token));

            // ожидаем завершения всех задач
            var results = await Task.WhenAll(taskSelf, taskCourier, taskYourTime);

            foreach (var taskResult in results)
            {
                var deliveryType = taskResult.deliveryType;

                if (deliveryType == Constants.Self) { result.Self = taskResult.available; }
                if (deliveryType == Constants.CourierDelivery) { result.Courier = taskResult.available; }
                if (deliveryType == Constants.YourTimeDelivery) { result.YourTime = taskResult.available; }
            }

            lock (_contextAccessor.HttpContext.Items)
            {
                _contextAccessor.HttpContext.Items["TimeDatabaseConnection"] = Convert.ToInt64(results.Max(obj => obj.sqlConnectionTime));
                _contextAccessor.HttpContext.Items["TimeSqlExecution"] = Convert.ToInt64(results.Max(obj => obj.sqlExecutionTime));
            }

            return result;
        }

        private static void DeleteCachedDataFromInputData(List<CodeItemQuery> queryItems, AvailableDateResult dataFromCache)
        {
            if (!dataFromCache.Data.Any())
            {
                return;
            }

            for (int i = queryItems.Count - 1; i >= 0; i--)
            {
                var item = queryItems[i];
                var keyField = item.CacheKey;

                if (dataFromCache.Data.ContainsKey(keyField))
                {
                    queryItems.RemoveAt(i);
                }
            }
        }
    }
}
