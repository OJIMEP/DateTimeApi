using Dapper;
using DateTimeService.Application.Cache;
using DateTimeService.Application.Database;
using DateTimeService.Application.Models;
using DateTimeService.Application.Queries;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Diagnostics;

namespace DateTimeService.Application.Repositories
{
    public class DateTimeRepository : IDateTimeRepository
    {
        private readonly IConfiguration _configuration;
        private readonly IDbConnectionFactory _dbConnectionFactory;
        private readonly IGeoZones _geoZones;
        private readonly RedisSettings _redisSettings;
        private readonly IConnectionMultiplexer _redis;

        public DateTimeRepository(IDbConnectionFactory dbConnectionFactory, RedisSettings redisSettings, IConnectionMultiplexer redis, 
            IConfiguration configuration, IGeoZones geoZones)
        {
            _dbConnectionFactory = dbConnectionFactory;
            _redisSettings = redisSettings;
            _redis = redis;
            _configuration = configuration;
            _geoZones = geoZones;
        }

        public async Task<AvailableDateResult> GetAvailableDateAsync(AvailableDateQuery query, CancellationToken token = default)
        {
            query.CheckQuantity = query.CheckQuantity && query.Codes.Any(x => x.Quantity > 1);
            AvailableDateResult result = new();

            if (!query.CheckQuantity)
            {
                var dataFromCache = await GetFromCache(query.Codes, query.CityId);

                //_contextAccessor.HttpContext.Items["FromCache"] = dataFromCache.Data.Count;

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

            var newDates = await GetAvailableDateFromDatabase(query, token);

            foreach (var item in newDates.Data)
            {
                result.Data.Add(item.Key, item.Value);
            }

            if (!query.CheckQuantity)
            {
                await SaveToCache(newDates, query.CityId);
            }
            
            return result;
        }

        public async Task<IntervalListResult> GetIntervalListAsync(IntervalListQuery query, CancellationToken token = default)
        {
            DbConnection dbConnection = await GetDbConnection(token: token);

            bool adressExists;
            string zoneId;
            Stopwatch watch = Stopwatch.StartNew();

            (adressExists, zoneId) = await _geoZones.CheckAddressGeozone(query, dbConnection.Connection);

            watch.Stop();
            //_contextAccessor.HttpContext.Items["TimeLocationExecution"] = watch.ElapsedMilliseconds;

            if (!adressExists && zoneId == "")
            {
                throw new ValidationException("Адрес и геозона не найдены!");
            }

            IntervalListResult result;

            result = await GetIntervalListFromDatabase(query, dbConnection, zoneId, token);

            return result;
        }

        public async Task<AvailableDeliveryTypesResult> GetAvailableDeliveryTypesAsync(AvailableDeliveryTypesQuery query, CancellationToken token = default)
        {
            var result = new AvailableDeliveryTypesResult();

            Task<DeliveryTypeAvailabilityResult> taskSelf;
            Task<DeliveryTypeAvailabilityResult> taskCourier;
            Task<DeliveryTypeAvailabilityResult> taskYourTime;
           
            taskSelf = Task.Run(() => GetDeliveryTypeAvailabilityFromDatabase(query, Constants.Self, token));
            taskCourier = Task.Run(() => GetDeliveryTypeAvailabilityFromDatabase(query, Constants.CourierDelivery, token));
            taskYourTime = Task.Run(() => GetDeliveryTypeAvailabilityFromDatabase(query, Constants.YourTimeDelivery, token));

            // ожидаем завершения всех задач
            var results = await Task.WhenAll(taskSelf, taskCourier, taskYourTime);

            foreach (var taskResult in results)
            {
                var deliveryType = taskResult.deliveryType;

                if (deliveryType == Constants.Self) { result.Self = taskResult.available; }
                if (deliveryType == Constants.CourierDelivery) { result.Courier = taskResult.available; }
                if (deliveryType == Constants.YourTimeDelivery) { result.YourTime = taskResult.available; }
            }

            //_contextAccessor.HttpContext.Items["DatabaseConnection"] = string.Join(",", results.Select(obj => obj.connection));
            //_contextAccessor.HttpContext.Items["LoadBalancingExecution"] = Convert.ToInt64(results.Max(obj => obj.loadBalancingTime));
            //_contextAccessor.HttpContext.Items["TimeSqlExecutionFact"] = Convert.ToInt64(results.Max(obj => obj.sqlExecutionTime));

            return result;
        }

        public async Task<AvailableDateResult> GetAvailableDateFromDatabase(AvailableDateQuery query, CancellationToken token = default)
        {
            DbConnection dbConnection = await GetDbConnection(token: token);

            SqlConnection connection = dbConnection.Connection;

            var globalParameters = await GetGlobalParameters(connection);

            var queryParameters = new DynamicParameters();

            string queryText = AvailableDateQueryText(query, globalParameters, queryParameters, dbConnection);

            Stopwatch watch = Stopwatch.StartNew();

            var dbResult = await connection.QueryAsync<AvailableDateRecord>(
                new CommandDefinition(queryText, queryParameters, cancellationToken: token)
            );

            watch.Stop();
            //_contextAccessor.HttpContext.Items["TimeSqlExecutionFact"] = watch.ElapsedMilliseconds;

            foreach (var record in dbResult)
            {
                record.Courier.AddYears(-2000);
                record.Self.AddYears(-2000);
            };

            var resultDict = new AvailableDateResult();

            try
            {
                foreach (var codeItem in query.Codes)
                {
                    var resultElement = new AvailableDateElementResult
                    {
                        Code = codeItem.Article,
                        SalesCode = codeItem.SalesCode,
                        Courier = null,
                        Self = null
                    };

                    AvailableDateRecord? dbRecord;

                    if (String.IsNullOrEmpty(codeItem.Code))
                    {
                        dbRecord = dbResult.FirstOrDefault(x => x.Article == codeItem.Article);
                    }
                    else
                    {
                        dbRecord = dbResult.FirstOrDefault(x => x.Code == codeItem.Code);
                    }

                    if (dbRecord is not null)
                    {
                        resultElement.Courier = query.DeliveryTypes.Contains("courier") && dbRecord.Courier.Year != 3999
                            ? dbRecord.Courier.ToString("yyyy-MM-ddTHH:mm:ss")
                            : null;
                        resultElement.Self = query.DeliveryTypes.Contains("self") && dbRecord.Self.Year != 3999
                            ? dbRecord.Self.ToString("yyyy-MM-ddTHH:mm:ss")
                            : null;
                    }

                    if (String.IsNullOrEmpty(codeItem.Code))
                    {
                        resultDict.Data.Add(codeItem.Article, resultElement);
                    }
                    else
                    {
                        resultDict.Data.Add($"{codeItem.Article}_{codeItem.SalesCode}", resultElement);
                    }
                }
            }
            catch (ArgumentException ex)
            {
                throw new Exception("Duplicated keys in dictionary");
            }
            catch (Exception ex)
            {
                throw;
            }

            return resultDict;
        }
     
        private async Task<IntervalListResult> GetIntervalListFromDatabase(IntervalListQuery query, DbConnection dbConnection, string zoneId, CancellationToken token = default)
        {
            var result = new IntervalListResult();

            var globalParameters = await GetGlobalParameters(dbConnection.Connection);

            var queryParameters = new DynamicParameters();

            string queryText = IntervalListQueryText(query, globalParameters, queryParameters, dbConnection, zoneId);

            Stopwatch watch = Stopwatch.StartNew();

            try
            {
                var results = await dbConnection.Connection.QueryAsync<IntervalListRecord>(
                    new CommandDefinition(queryText, queryParameters, cancellationToken: token)
                );

                foreach (var element in results)
                {
                    var begin = element.StartDate.AddMonths(-24000);
                    var end = element.EndDate.AddMonths(-24000);
                    var bonus = element.Bonus == 1;

                    result.Data.Add(new IntervalListElementResult
                    {
                        Begin = begin,
                        End = end,
                        Bonus = bonus
                    });
                }
            }
            catch (Exception ex)
            {
                throw;
            }

            watch.Stop();
            //_contextAccessor.HttpContext.Items["TimeSqlExecutionFact"] = watch.ElapsedMilliseconds;

            return result;
        }

        private async Task<DeliveryTypeAvailabilityResult>? GetDeliveryTypeAvailabilityFromDatabase(AvailableDeliveryTypesQuery query, string deliveryType, CancellationToken token)
        {
            DbConnection dbConnection = await GetDbConnection(false, token);

            var globalParameters = await GetGlobalParameters(dbConnection.Connection, token);

            var queryParameters = new DynamicParameters();

            string queryText = AvailableDeliveryTypesQueryText(query, globalParameters, queryParameters, deliveryType, dbConnection.DatabaseType);

            bool deliveryTypeAvailable;

            var watch = Stopwatch.StartNew();

            try
            {
                var result = await dbConnection.Connection.QueryAsync<int>(
                    new CommandDefinition(queryText, queryParameters, cancellationToken: token));

                deliveryTypeAvailable = result != null && result.Any();
            }
            catch (Exception)
            {
                throw;
            }

            watch.Stop();

            return new DeliveryTypeAvailabilityResult
            {
                deliveryType = deliveryType,
                available = deliveryTypeAvailable,
                loadBalancingTime = dbConnection.ConnectTimeInMilliseconds,
                sqlExecutionTime = watch.ElapsedMilliseconds,
                connection = dbConnection.ConnectionWithoutCredentials
            };
        }

        private string AvailableDateQueryText(AvailableDateQuery query, List<GlobalParameter> globalParameters, DynamicParameters queryParameters, DbConnection dbConnection)
        {
            List<string> pickups = new();

            var queryTextBegin = TextFillGoodsTable(query, queryParameters, true, pickups);

            List<string> queryParts = new()
            {
                query.CheckQuantity == true ? AvailableDateQueries.AvailableDateWithCount1 : AvailableDateQueries.AvailableDate1,
                dbConnection.UseAggregations == true ? AvailableDateQueries.AvailableDate2MinimumWarehousesCustom : AvailableDateQueries.AvailableDate2MinimumWarehousesBasic,
                query.CheckQuantity == true ? AvailableDateQueries.AvailableDateWithCount3 : AvailableDateQueries.AvailableDate3,
                AvailableDateQueries.AvailableDate4SourcesWithPrices,
                query.CheckQuantity == true ? AvailableDateQueries.AvailableDateWithCount5 : AvailableDateQueries.AvailableDate5,
                dbConnection.UseAggregations == true ? AvailableDateQueries.AvailableDate6IntervalsCustom : AvailableDateQueries.AvailableDate6IntervalsBasic,
                AvailableDateQueries.AvailableDate7,
                dbConnection.UseAggregations == true ? AvailableDateQueries.AvailableDate8DeliveryPowerCustom : AvailableDateQueries.AvailableDate8DeliveryPowerBasic,
                AvailableDateQueries.AvailableDate9
            };

            var queryText = String.Join("", queryParts);

            List<string> pickupParameters = new();
            foreach (var pickupPoint in pickups)
            {
                var parameterString = string.Format("@PickupPointAll{0}", pickups.IndexOf(pickupPoint));
                pickupParameters.Add(parameterString);
                queryParameters.Add(parameterString, pickupPoint);
            }
            if (pickupParameters.Count == 0)
            {
                pickupParameters.Add("NULL");
            }

            var DateMove = DateTime.Now.AddMonths(24000);

            queryParameters.Add("@P_CityCode", query.CityId);
            queryParameters.Add("@P_DateTimeNow", DateMove);
            queryParameters.Add("@P_DateTimePeriodBegin", DateMove.Date);
            queryParameters.Add("@P_DateTimePeriodEnd", DateMove.Date.AddDays(globalParameters.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1));
            queryParameters.Add("@P_TimeNow", new DateTime(2001, 1, 1, DateMove.Hour, DateMove.Minute, DateMove.Second));
            queryParameters.Add("@P_EmptyDate", new DateTime(2001, 1, 1, 0, 0, 0));
            queryParameters.Add("@P_MaxDate", new DateTime(5999, 11, 11, 0, 0, 0));
            queryParameters.Add("@P_DaysToShow", 7);
            queryParameters.Add("@P_ApplyShifting", (int)globalParameters.GetValue("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров"));
            queryParameters.Add("@P_DaysToShift", (int)globalParameters.GetValue("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров"));

            if (query.CheckQuantity)
            {
                queryParameters.Add("@P_StockPriority", (int)globalParameters.GetValue("ПриоритизироватьСток_64854"));
            }

            string dateTimeNowOptimizeString = _configuration.GetValue<bool>("optimizeDateTimeNowEveryHour")
                ? DateMove.ToString("yyyy-MM-ddTHH:00:00")
            : DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            var pickupWorkingHoursJoinType = _configuration.GetValue<string>("pickupWorkingHoursJoinType");

            string useIndexHint = _configuration.GetValue<string>("useIndexHintWarehouseDates");// @", INDEX([_InfoRg23830_Custom2])";
            if (dbConnection.DatabaseType != DatabaseType.ReplicaTables || dbConnection.UseAggregations)
            {
                useIndexHint = "";
            }

            queryText = queryTextBegin + string.Format(queryText, string.Join(",", pickupParameters),
                dateTimeNowOptimizeString,
                DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                DateMove.Date.AddDays(globalParameters.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                globalParameters.GetValue("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                globalParameters.GetValue("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                pickupWorkingHoursJoinType,
                useIndexHint);

            if (_configuration.GetValue<bool>("disableKeepFixedPlan"))
            {
                queryText = queryText.Replace(", KEEPFIXED PLAN", "");
            }

            return queryText;
        }

        private string IntervalListQueryText(IntervalListQuery query, List<GlobalParameter> globalParameters, DynamicParameters queryParameters, DbConnection dbConnection, string zoneId)
        {
            string queryText = IntervalListQueries.IntervalList;
            
            var queryTextBegin = TextFillGoodsTable(query, queryParameters);

            var yourTimeDelivery = false;

            if (query.DeliveryType == Constants.YourTimeDelivery)
            {
                query.DeliveryType = Constants.CourierDelivery;
                yourTimeDelivery = true;
            }

            var DateMove = DateTime.Now.AddMonths(24000);

            queryParameters.Add("@P_AdressCode", query.AddressId);
            queryParameters.Add("@PickupPoint1", query.PickupPoint);
            queryParameters.Add("@P_Credit", query.Payment == "partly_pay" ? 1 : 0);
            queryParameters.Add("@P_Floor", (double)(query.Floor != null ? query.Floor : globalParameters.GetValue("Логистика_ЭтажПоУмолчанию")));
            queryParameters.Add("@P_DaysToShow", 7);
            queryParameters.Add("@P_DateTimeNow", DateMove);
            queryParameters.Add("@P_DateTimePeriodBegin", DateMove.Date);
            queryParameters.Add("@P_DateTimePeriodEnd", DateMove.Date.AddDays(globalParameters.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1));
            queryParameters.Add("@P_TimeNow", new DateTime(2001, 1, 1, DateMove.Hour, DateMove.Minute, DateMove.Second));
            queryParameters.Add("@P_EmptyDate", new DateTime(2001, 1, 1, 0, 0, 0));
            queryParameters.Add("@P_MaxDate", new DateTime(5999, 11, 11, 0, 0, 0));
            queryParameters.Add("@P_GeoCode", zoneId);
            queryParameters.Add("@P_OrderDate", query.OrderDate.AddMonths(24000));
            queryParameters.Add("@P_OrderNumber", query.OrderNumber);
            queryParameters.Add("@P_ApplyShifting", (int)globalParameters.GetValue("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров"));
            queryParameters.Add("@P_DaysToShift", (int)globalParameters.GetValue("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров"));
            queryParameters.Add("@P_StockPriority", (int)globalParameters.GetValue("ПриоритизироватьСток_64854"));
            queryParameters.Add("@P_YourTimeDelivery", yourTimeDelivery ? 1 : 0);

            string dateTimeNowOptimizeString = _configuration.GetValue<bool>("optimizeDateTimeNowEveryHour")
                ? DateMove.ToString("yyyy-MM-ddTHH:00:00")
                : DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            queryText = queryTextBegin + string.Format(queryText,
                "",
                dateTimeNowOptimizeString,
                DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                DateMove.Date.AddDays(globalParameters.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                globalParameters.GetValue("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                globalParameters.GetValue("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                dbConnection.DatabaseType == DatabaseType.ReplicaTables ? _configuration.GetValue<string>("useIndexHintWarehouseDates") : ""); // index hint

            if (_configuration.GetValue<bool>("disableKeepFixedPlan"))
            {
                queryText = queryText.Replace(", KEEPFIXED PLAN", "");
            }

            return queryText;
        }

        private string AvailableDeliveryTypesQueryText(AvailableDeliveryTypesQuery query, List<GlobalParameter> globalParameters, 
            DynamicParameters queryParameters, string deliveryType, DatabaseType databaseType)
        {
            string queryText = AvailableDeliveryTypesQueries.AvailableDelivery;
            
            var queryTextBegin = TextFillGoodsTable(query, queryParameters);

            var DateMove = DateTime.Now.AddMonths(24000);

            queryParameters.Add("@P_CityCode", query.CityId);
            queryParameters.Add("@P_Floor", (double)(globalParameters.GetValue("Логистика_ЭтажПоУмолчанию")));
            queryParameters.Add("@P_DaysToShow", 7);
            queryParameters.Add("@P_DateTimeNow", DateMove);
            queryParameters.Add("@P_DateTimePeriodBegin", DateMove.Date);
            queryParameters.Add("@P_DateTimePeriodEnd", DateMove.Date.AddDays(7 - 1));
            queryParameters.Add("@P_TimeNow", new DateTime(2001, 1, 1, DateMove.Hour, DateMove.Minute, DateMove.Second));
            queryParameters.Add("@P_EmptyDate", new DateTime(2001, 1, 1, 0, 0, 0));
            queryParameters.Add("@P_MaxDate", new DateTime(5999, 11, 11, 0, 0, 0));
            queryParameters.Add("@P_ApplyShifting", (int)globalParameters.GetValue("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров"));
            queryParameters.Add("@P_DaysToShift", (int)globalParameters.GetValue("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров"));
            queryParameters.Add("@P_StockPriority", (int)globalParameters.GetValue("ПриоритизироватьСток_64854"));
            queryParameters.Add("@P_YourTimeDelivery", deliveryType == Constants.YourTimeDelivery ? 1 : 0);
            queryParameters.Add("@P_IsDelivery", deliveryType == Constants.Self ? 0 : 1);
            queryParameters.Add("@P_IsPickup", deliveryType == Constants.Self ? 1 : 0);

            string pickupPointsString = string.Join(", ", query.PickupPoints
                .Select((value, index) =>
                {
                    string parameterName = $"@PickupPoint{index}";
                    queryParameters.Add(parameterName, value);
                    return parameterName;
                }));

            string dateTimeNowOptimizeString = _configuration.GetValue<bool>("optimizeDateTimeNowEveryHour")
                ? DateMove.ToString("yyyy-MM-ddTHH:00:00")
                : DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            queryText = queryTextBegin + string.Format(queryText,
                dateTimeNowOptimizeString,
                DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                DateMove.Date.AddDays(7 - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                globalParameters.GetValue("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                globalParameters.GetValue("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                databaseType == DatabaseType.ReplicaTables ? _configuration.GetValue<string>("useIndexHintWarehouseDates") : "", // index hint
                pickupPointsString);

            if (_configuration.GetValue<bool>("disableKeepFixedPlan"))
            {
                queryText = queryText.Replace(", KEEPFIXED PLAN", "");
            }

            return queryText;
        }

        public static string TextFillGoodsTable(AvailableDateQuery query, DynamicParameters queryParameters, bool optimizeRowsCount, List<string> PickupsList)
        {

            var resultString = CommonQueries.TableGoodsRawCreate;

            var insertRowsLimit = 900;

            query.Codes = query.Codes.Where(x =>
            {
                if (query.CheckQuantity)
                {
                    return x.Quantity > 0;
                }
                else return true;
            }).ToList();

            var maxCodes = query.Codes.Count;

            foreach (var codesElem in query.Codes)
            {
                foreach (var item in codesElem.PickupPoints)
                {
                    if (!PickupsList.Contains(item))
                    {
                        PickupsList.Add(item);
                    }
                }
            }

            if (query.Codes.Count > 2) maxCodes = 10;
            if (query.Codes.Count > 10) maxCodes = 30;
            if (query.Codes.Count > 30) maxCodes = 60;
            if (query.Codes.Count > 60) maxCodes = 100;
            if (query.Codes.Count > maxCodes || !optimizeRowsCount) maxCodes = query.Codes.Count;

            var parameters = new List<string>();

            for (int index = 0; index < maxCodes; index++)
            {
                CodeItemQuery item;

                if (index < query.Codes.Count)
                {
                    item = query.Codes[index];
                }
                else
                {
                    item = query.Codes[^1];
                }

                var article = $"@Article{index}";
                var code = $"@Code{index}";
                var quantity = $"@Quantity{index}";

                queryParameters.Add(article, item.Article);
                queryParameters.Add(code, string.IsNullOrEmpty(item.Code) ? null : item.Code);
                queryParameters.Add(quantity, item.Quantity);

                var parameterString = $"({article}, {code}, NULL, {quantity})";

                parameters.Add(parameterString);

                if (parameters.Count == insertRowsLimit)
                {
                    resultString += string.Format(CommonQueries.TableGoodsRawInsert, string.Join(", ", parameters));

                    parameters.Clear();
                }

                if (item.PickupPoints.Count() > 0)
                {
                    var pickupPoint = $"@PickupPoint{index}";

                    queryParameters.Add(pickupPoint, string.Join(",", item.PickupPoints));

                    var parameterStringPickup = $"({article}, {code}, {pickupPoint}, {quantity})";
                    parameters.Add(parameterStringPickup);
                }
            }

            if (parameters.Count > 0)
            {
                resultString += string.Format(CommonQueries.TableGoodsRawInsert, string.Join(", ", parameters));

                parameters.Clear();
            }

            return resultString;
        }

        private static string TextFillGoodsTable(IntervalListQuery query, DynamicParameters queryParameters)
        {
            var resultString = CommonQueries.TableGoodsRawCreate;

            var parameters = query.OrderItems.Select((item, index) =>
            {
                var article = $"@Article{index}";
                var code = $"@Code{index}";
                var quantity = $"@Quantity{index}";

                queryParameters.Add(article, item.Article);
                queryParameters.Add(code, string.IsNullOrEmpty(item.Code) ? null : item.Code);
                queryParameters.Add(quantity, item.Quantity);

                return $"({article}, {code}, NULL, {quantity})";
            }).ToList();

            if (parameters.Count > 0)
            {
                resultString += string.Format(CommonQueries.TableGoodsRawInsert, string.Join(", ", parameters));
            }

            return resultString;
        }

        private static string TextFillGoodsTable(AvailableDeliveryTypesQuery query, DynamicParameters queryParameters)
        {
            var resultString = AvailableDeliveryTypesQueries.GoodsRawCreate;

            var parameters = query.OrderItems.Select((item, index) =>
            {
                var article = $"@Article{index}";
                var code = $"@Code{index}";
                var quantity = $"@Quantity{index}";

                queryParameters.Add(article, item.Article);
                queryParameters.Add(code, string.IsNullOrEmpty(item.Code) ? null : item.Code);
                queryParameters.Add(quantity, item.Quantity);

                return $"({article}, {code}, {quantity})";
            }).ToList();

            if (parameters.Count > 0)
            {
                resultString += string.Format(AvailableDeliveryTypesQueries.GoodsRawInsert, string.Join(", ", parameters));
            }

            return resultString;
        }

        private async Task<DbConnection> GetDbConnection(bool logging = true, CancellationToken token = default)
        {
            DbConnection dbConnection;

            try
            {
                dbConnection = await _dbConnectionFactory.CreateConnectionAsync(token);
                if (logging)
                {
                    //_contextAccessor.HttpContext.Items["DatabaseConnection"] = dbConnection.ConnectionWithoutCredentials;
                    //_contextAccessor.HttpContext.Items["LoadBalancingExecution"] = dbConnection.ConnectTimeInMilliseconds;
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }

            if (dbConnection.Connection == null)
            {
                throw new Exception("Не найдено доступное соединение к БД");
            }

            return dbConnection;
        }

        private async Task<List<GlobalParameter>> GetGlobalParameters(SqlConnection connection, CancellationToken token = default)
        {
            var watch = Stopwatch.StartNew();

            List<GlobalParameter> parameters = null;

            string key = "GlobalParameters";

            if (_redisSettings.Enabled
                && _redis.IsConnected)
            {
                var db = _redis.GetDatabase((int)_redisSettings.Database);

                parameters = await db.GetRecord<List<GlobalParameter>>(key);
            }

            if (parameters is null)
            {
                parameters = new List<GlobalParameter>
                {
                    new GlobalParameter
                    {
                        Name = "rsp_КоличествоДнейЗаполненияГрафика",
                        DefaultDouble = 5
                    },
                    new GlobalParameter
                    {
                        Name = "КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа",
                        DefaultDouble = 4
                    },
                    new GlobalParameter
                    {
                        Name = "ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа",
                        DefaultDouble = 3
                    },
                    new GlobalParameter
                    {
                        Name = "Логистика_ЭтажПоУмолчанию",
                        DefaultDouble = 4,
                        UseDefault = true
                    },
                    new GlobalParameter
                    {
                        Name = "ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров",
                        DefaultDouble = 0
                    },
                    new GlobalParameter
                    {
                        Name = "КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров",
                        DefaultDouble = 0
                    },
                    new GlobalParameter
                    {
                        Name = "ПриоритизироватьСток_64854",
                        DefaultDouble = 0
                    }
                };

                await GlobalParameter.FillValues(connection, parameters, token);

                if (_redisSettings.Enabled
                    && _redis.IsConnected)
                {
                    var db = _redis.GetDatabase((int)_redisSettings.Database);

                    // кешируем ГП в памяти на 1 час, потом они снова обновятся
                    await db.SetRecord(key, parameters, TimeSpan.FromSeconds(600));
                }
            }

            watch.Stop();
            //_contextAccessor.HttpContext.Items["GlobalParametersExecution"] = watch.ElapsedMilliseconds;

            return parameters;
        }

        private async Task SaveToCache(AvailableDateResult result, string cityId)
        {
            if (!_redisSettings.Enabled || !_redis.IsConnected)
            {
                return;
            }

            // Время жизни ключей
            var expiry = TimeSpan.FromSeconds(_redisSettings.LifeTime);
            var db = _redis.GetDatabase((int)_redisSettings.Database);

            // Запись пар ключ-значение в Redis
            foreach (var item in result.Data)
            {
                var key = $"{item.Key}-{cityId}";
                await db.SetRecord(key, item.Value, expiry);
            }
        }

        private async Task<AvailableDateResult> GetFromCache(List<CodeItemQuery> queryItems, string cityId)
        {
            var result = new AvailableDateResult();

            if (!_redisSettings.Enabled || !_redis.IsConnected)
            {
                return result;
            }

            var watch = Stopwatch.StartNew();

            var db = _redis.GetDatabase((int)_redisSettings.Database);

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

            //_contextAccessor.HttpContext.Items["TimeGettingFromCache"] = watch.ElapsedMilliseconds;

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
