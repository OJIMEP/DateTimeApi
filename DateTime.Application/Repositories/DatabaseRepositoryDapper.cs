using Dapper;
using DateTimeService.Application.Database;
using DateTimeService.Application.Models;
using DateTimeService.Application.Queries;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;

namespace DateTimeService.Application.Repositories
{
    public class DatabaseRepositoryDapper : IDatabaseRepository
    {
        private readonly IDbConnectionFactory _dbConnectionFactory;
        private readonly IGeoZones _geoZones;
        private readonly IHttpContextAccessor _contextAccessor;
        private readonly IConfiguration _configuration;
        private readonly IMemoryCache _memoryCache;

        public DatabaseRepositoryDapper(IHttpContextAccessor contextAccessor, IConfiguration configuration, 
            IDbConnectionFactory dbConnectionFactory, IMemoryCache memoryCache, IGeoZones geoZones)
        {
            _contextAccessor = contextAccessor;
            _configuration = configuration;
            _dbConnectionFactory = dbConnectionFactory;
            _memoryCache = memoryCache;
            _geoZones = geoZones;
        }

        public async Task<AvailableDateResult> GetAvailableDate(AvailableDateQuery query, CancellationToken token = default)
        {
            DbConnection dbConnection = await _dbConnectionFactory.GetDbConnection(token: token);

            IEnumerable<AvailableDateRecord> dbResult;

            using (var connection = dbConnection.Connection)
            {
                var globalParameters = await GetGlobalParameters(connection, token);

                var queryParameters = new DynamicParameters();

                string queryText = AvailableDateQueryText(query, globalParameters, queryParameters, dbConnection);

                Stopwatch watch = Stopwatch.StartNew();

                dbResult = await connection.QueryAsync<AvailableDateRecord>(
                    new CommandDefinition(queryText, queryParameters, cancellationToken: token)
                );

                watch.Stop();
                _contextAccessor.HttpContext.Items["TimeSqlExecution"] = watch.ElapsedMilliseconds;
            }

            foreach (var record in dbResult)
            {
                record.Courier = record.Courier.AddYears(-2000);
                record.Self = record.Self.AddYears(-2000);
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
            catch (ArgumentException)
            {
                throw new Exception("Duplicated keys in dictionary");
            }
            catch (Exception)
            {
                throw;
            }

            return resultDict;
        }

        public async Task<DeliveryTypeAvailabilityResult> GetDeliveryTypeAvailability(AvailableDeliveryTypesQuery query, string deliveryType, CancellationToken token = default)
        {
            bool deliveryTypeAvailable;
            long elapsedMs;

            DbConnection dbConnection = await _dbConnectionFactory.GetDbConnection(token);

            using (var connection = dbConnection.Connection)
            {
                var globalParameters = await GetGlobalParameters(dbConnection.Connection, token);

                var queryParameters = new DynamicParameters();

                string queryText = AvailableDeliveryTypesQueryText(query, globalParameters, queryParameters, deliveryType, dbConnection.DatabaseType);

                var watch = Stopwatch.StartNew();

                var result = await dbConnection.Connection.QueryAsync<int>(
                    new CommandDefinition(queryText, queryParameters, cancellationToken: token));

                watch.Stop();

                elapsedMs = watch.ElapsedMilliseconds;

                deliveryTypeAvailable = result != null && result.Any();
            }

            return new DeliveryTypeAvailabilityResult
            {
                deliveryType = deliveryType,
                available = deliveryTypeAvailable,
                sqlExecutionTime = elapsedMs,
                sqlConnectionTime = dbConnection.ConnectTimeInMilliseconds
            };
        }

        public async Task<IntervalListResult> GetIntervalList(IntervalListQuery query, CancellationToken token = default)
        {
            var result = new IntervalListResult();

            DbConnection dbConnection = await _dbConnectionFactory.GetDbConnection(token);

            bool adressExists;
            string zoneId;
            Stopwatch watch = Stopwatch.StartNew();

            (adressExists, zoneId) = await _geoZones.CheckAddressGeozone(query, dbConnection.Connection, token);

            watch.Stop();
            _contextAccessor.HttpContext.Items["TimeLocationExecution"] = watch.ElapsedMilliseconds;

            if (!adressExists && zoneId == "")
            {
                throw new ValidationException("Адрес и геозона не найдены!");
            }

            using (var connection = dbConnection.Connection)
            {
                var globalParameters = await GetGlobalParameters(connection, token);

                var queryParameters = new DynamicParameters();

                string queryText = IntervalListQueryText(query, globalParameters, queryParameters, dbConnection, zoneId);

                watch.Restart();

                var results = await connection.QueryAsync<IntervalListRecord>(
                    new CommandDefinition(queryText, queryParameters, cancellationToken: token)
                );

                foreach (var element in results)
                {
                    var begin = element.StartDate.AddYears(-2000);
                    var end = element.EndDate.AddYears(-2000);
                    var bonus = element.Bonus == 1;

                    result.Data.Add(new IntervalListElementResult
                    {
                        Begin = begin,
                        End = end,
                        Bonus = bonus
                    });
                }

                watch.Stop();

                _contextAccessor.HttpContext.Items["TimeSqlExecution"] = watch.ElapsedMilliseconds;
            }

            return result;
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

            var DateMove = DateTime.Now.AddYears(2000);

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
            string dateTimeNowOptimizeString = DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            var pickupWorkingHoursJoinType = _configuration.GetValue<string>("pickupWorkingHoursJoinType");

            string useIndexHint = _configuration.GetValue<string>("UseIndexHintWarehouseDates");// @", INDEX([_InfoRg23830_Custom2])";
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

            if (_configuration.GetValue<bool>("DisableKeepFixedPlan"))
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

            var DateMove = DateTime.Now.AddYears(2000);

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
            queryParameters.Add("@P_OrderDate", query.OrderDate.AddYears(2000));
            queryParameters.Add("@P_OrderNumber", query.OrderNumber);
            queryParameters.Add("@P_ApplyShifting", (int)globalParameters.GetValue("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров"));
            queryParameters.Add("@P_DaysToShift", (int)globalParameters.GetValue("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров"));
            queryParameters.Add("@P_StockPriority", (int)globalParameters.GetValue("ПриоритизироватьСток_64854"));
            queryParameters.Add("@P_YourTimeDelivery", yourTimeDelivery ? 1 : 0);

            string dateTimeNowOptimizeString = DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            queryText = queryTextBegin + string.Format(queryText,
                "",
                dateTimeNowOptimizeString,
                DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                DateMove.Date.AddDays(globalParameters.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                globalParameters.GetValue("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
            globalParameters.GetValue("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                dbConnection.DatabaseType == DatabaseType.ReplicaTables ? _configuration.GetValue<string>("UseIndexHintWarehouseDates") : ""); // index hint

            if (_configuration.GetValue<bool>("DisableKeepFixedPlan"))
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

            var DateMove = DateTime.Now.AddYears(2000);

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

            string dateTimeNowOptimizeString = DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            queryText = queryTextBegin + string.Format(queryText,
                dateTimeNowOptimizeString,
                DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                DateMove.Date.AddDays(7 - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                globalParameters.GetValue("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                globalParameters.GetValue("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                databaseType == DatabaseType.ReplicaTables ? _configuration.GetValue<string>("UseIndexHintWarehouseDates") : "", // index hint
                pickupPointsString);

            if (_configuration.GetValue<bool>("DisableKeepFixedPlan"))
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

        private async Task<List<GlobalParameter>> GetGlobalParameters(SqlConnection connection, CancellationToken token = default)
        {
            var watch = Stopwatch.StartNew();

            string key = "GlobalParameters";

            var parameters = await _memoryCache.GetOrCreateAsync(key, async entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1);
                return await GlobalParameter.GetParameters(connection, token);
            });

            //if (_redisSettings.Enabled
            //    && _redis.IsConnected)
            //{
            //    var db = _redis.GetDatabase((int)_redisSettings.Database);

            //    parameters = await db.GetRecord<List<GlobalParameter>>(key);
            //}

            //if (parameters is null)
            //{
            //    parameters = await GlobalParameter.GetParameters(connection, token);

            //    if (_redisSettings.Enabled
            //        && _redis.IsConnected)
            //    {
            //        var db = _redis.GetDatabase((int)_redisSettings.Database);

            //        // кешируем ГП в памяти на 1 час, потом они снова обновятся
            //        await db.SetRecord(key, parameters, TimeSpan.FromSeconds(600));
            //    }
            //}

            watch.Stop();

            lock (_contextAccessor.HttpContext.Items)
            {
                _contextAccessor.HttpContext.Items["TimeGlobalParametersExecution"] = watch.ElapsedMilliseconds;
            }

            return parameters;
        }
    }
}
