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
            query.CheckQuantity = query.CheckQuantity && query.Codes.Any(x => x.Quantity > 1);
            AvailableDateResult result = new();

            _contextAccessor.HttpContext.Items["TotalItems"] = query.Codes.Count;

            if (!query.CheckQuantity)
            {
                var dataFromCache = await _redisRepository.GetAvailableDateResultGromCashe(query.Codes, query.CityId);

                _contextAccessor.HttpContext.Items["FromCache"] = dataFromCache.Data.Count;

                foreach (var item in dataFromCache.Data)
                {
                    result.Data.Add(item.Key, item.Value);
                }

                if (result.Data.Count == query.Codes.Count)
                {
                    return result;
                }

                DeleteCachedDataFromInputData(query.Codes, dataFromCache);
            }

            var newDates = await _databaseRepository.GetAvailableDate(query, token);

            foreach (var item in newDates.Data)
            {
                result.Data.Add(item.Key, item.Value);
            }

            if (!query.CheckQuantity)
            {
                await _redisRepository.SaveAvailableDateResultToCache(newDates, query.CityId);
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
            for (int i = queryItems.Count - 1; i >= 0; i--)
            {
                var item = queryItems[i];
                var keyField = item.Code is not null ? item.Code : item.Article;

                if (dataFromCache.Data.ContainsKey(keyField))
                {
                    queryItems.RemoveAt(i);
                }
            }
        }
    }
}
