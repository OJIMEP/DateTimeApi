using DateTimeService.Application.Database;
using DateTimeService.Application.Models;
using DateTimeService.Application.Queries;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Diagnostics;

namespace DateTimeService.Application.Repositories
{
    public class DatabaseRepositoryBasic : IDatabaseRepository
    {
        private readonly IDbConnectionFactory _dbConnectionFactory;
        private readonly IGeoZones _geoZones;
        private readonly IHttpContextAccessor _contextAccessor;
        private readonly IConfiguration _configuration;
        private readonly IMemoryCache _memoryCache;

        public DatabaseRepositoryBasic(IHttpContextAccessor contextAccessor, IConfiguration configuration,
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

            List<AvailableDateRecord> dbResult = new();

            using (var connection = dbConnection.Connection)
            {
                SqlCommand command = await AvailableDateCommand(connection, query, dbConnection.DatabaseType, dbConnection.UseAggregations);

                Stopwatch watch = Stopwatch.StartNew();

                SqlDataReader dr = await command.ExecuteReaderAsync(token);

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        var record = new AvailableDateRecord
                        {
                            Article = dr.GetString(0),
                            Code = dr.GetString(1),
                            Courier = dr.GetDateTime(2).AddYears(-2000),
                            Self = dr.GetDateTime(3).AddYears(-2000)
                        };
                        
                        dbResult.Add(record);
                    }
                }

                await dr.CloseAsync();

                watch.Stop();
                _contextAccessor.HttpContext.Items["TimeSqlExecution"] = watch.ElapsedMilliseconds;
            }

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

        public async Task<IntervalListResult> GetIntervalList(IntervalListQuery query, CancellationToken token = default)
        {
            var result = new IntervalListResult();

            DbConnection dbConnection = await _dbConnectionFactory.GetDbConnection(token);

            bool adressExists;
            string zoneId;

            using (var connection = dbConnection.Connection)
            {
                Stopwatch watch = Stopwatch.StartNew();

                (adressExists, zoneId) = await _geoZones.CheckAddressGeozone(query, connection, token);

                watch.Stop();
                _contextAccessor.HttpContext.Items["TimeLocationExecution"] = watch.ElapsedMilliseconds;

                if (!adressExists && zoneId == "")
                {
                    throw new ValidationException("Адрес и геозона не найдены!");
                }

                SqlCommand command = await IntervalListCommand(connection, query, dbConnection.DatabaseType, zoneId);

                watch.Restart();

                SqlDataReader dr = command.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        var begin = dr.GetDateTime(0).AddYears(-2000);
                        var end = dr.GetDateTime(1).AddYears(-2000);
                        var bonus = dr.GetInt32(3) == 1;

                        result.Data.Add(new IntervalListElementResult
                        {
                            Begin = begin,
                            End = end,
                            Bonus = bonus
                        });
                    }
                }

                await dr.CloseAsync();

                watch.Stop();

                _contextAccessor.HttpContext.Items["TimeSqlExecution"] = watch.ElapsedMilliseconds;
            }

            return result;
        }

        public async Task<DeliveryTypeAvailabilityResult> GetDeliveryTypeAvailability(AvailableDeliveryTypesQuery query, string deliveryType, CancellationToken token = default)
        {
            bool deliveryTypeAvailable;
            long elapsedMs;

            DbConnection dbConnection = await _dbConnectionFactory.GetDbConnection(token);

            using (var connection = dbConnection.Connection)
            {
                SqlCommand command = await AvailableDeliveryTypesCommand(connection, query, deliveryType, dbConnection.DatabaseType);

                var watch = Stopwatch.StartNew();

                object result = await command.ExecuteScalarAsync(token);

                watch.Stop();

                elapsedMs = watch.ElapsedMilliseconds;

                deliveryTypeAvailable = result != null && (int)result > 0;
            }

            return new DeliveryTypeAvailabilityResult
            {
                deliveryType = deliveryType,
                available = deliveryTypeAvailable,
                sqlExecutionTime = elapsedMs,
                sqlConnectionTime = dbConnection.ConnectTimeInMilliseconds
            };
        }
      
        private async Task<SqlCommand> AvailableDateCommand(SqlConnection connection, AvailableDateQuery query, DatabaseType databaseType, bool customAggs)
        {
            var parameters1C = await GetGlobalParameters(connection);

            string queryText = "";

            SqlCommand cmd = new(queryText, connection)
            {
                CommandTimeout = 5
            };

            List<string> pickups = new();

            var queryTextBegin = TextFillGoodsTable(query, cmd, true, pickups);

            if (_configuration.GetValue<bool>("disableKeepFixedPlan"))
            {
                queryTextBegin = queryTextBegin.Replace(", KEEPFIXED PLAN", "");
            }
            List<string> queryParts = new()
            {
                query.CheckQuantity == true ? AvailableDateQueries.AvailableDateWithCount1 : AvailableDateQueries.AvailableDate1,
                customAggs == true ? AvailableDateQueries.AvailableDate2MinimumWarehousesCustom : AvailableDateQueries.AvailableDate2MinimumWarehousesBasic,
                query.CheckQuantity == true ? AvailableDateQueries.AvailableDateWithCount3 : AvailableDateQueries.AvailableDate3,
                AvailableDateQueries.AvailableDate4SourcesWithPrices,
                query.CheckQuantity == true ? AvailableDateQueries.AvailableDateWithCount5 : AvailableDateQueries.AvailableDate5,
                customAggs == true ? AvailableDateQueries.AvailableDate6IntervalsCustom : AvailableDateQueries.AvailableDate6IntervalsBasic,
                AvailableDateQueries.AvailableDate7,
                customAggs == true ? AvailableDateQueries.AvailableDate8DeliveryPowerCustom : AvailableDateQueries.AvailableDate8DeliveryPowerBasic,
                AvailableDateQueries.AvailableDate9
            };

            queryText = String.Join("", queryParts);

            List<string> pickupParameters = new();
            foreach (var pickupPoint in pickups)
            {
                var parameterString = string.Format("@PickupPointAll{0}", pickups.IndexOf(pickupPoint));
                pickupParameters.Add(parameterString);
                cmd.Parameters.Add(parameterString, SqlDbType.NVarChar, 4);
                cmd.Parameters[parameterString].Value = pickupPoint;
            }
            if (pickupParameters.Count == 0)
            {
                pickupParameters.Add("NULL");
            }

            var DateMove = DateTime.Now.AddYears(2000);

            cmd.Parameters.AddWithValue("@P_CityCode", query.CityId);
            cmd.Parameters.AddWithValue("@P_DateTimeNow", DateMove);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodBegin", DateMove.Date);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodEnd", DateMove.Date.AddDays(parameters1C.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1));
            cmd.Parameters.AddWithValue("@P_TimeNow", new DateTime(2001, 1, 1, DateMove.Hour, DateMove.Minute, DateMove.Second));
            cmd.Parameters.AddWithValue("@P_EmptyDate", new DateTime(2001, 1, 1, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_MaxDate", new DateTime(5999, 11, 11, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_DaysToShow", 7);
            cmd.Parameters.AddWithValue("@P_ApplyShifting", (int)parameters1C.GetValue("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров"));
            cmd.Parameters.AddWithValue("@P_DaysToShift", (int)parameters1C.GetValue("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров"));

            if (query.CheckQuantity)
            {
                cmd.Parameters.AddWithValue("@P_StockPriority", (int)parameters1C.GetValue("ПриоритизироватьСток_64854"));
            }

            string dateTimeNowOptimizeString = _configuration.GetValue<bool>("optimizeDateTimeNowEveryHour")
                ? DateMove.ToString("yyyy-MM-ddTHH:00:00")
                : DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            var pickupWorkingHoursJoinType = _configuration.GetValue<string>("pickupWorkingHoursJoinType");

            string useIndexHint = _configuration.GetValue<string>("useIndexHintWarehouseDates");// @", INDEX([_InfoRg23830_Custom2])";
            if (databaseType != DatabaseType.ReplicaTables || customAggs)
            {
                useIndexHint = "";
            }

            cmd.CommandText = queryTextBegin + string.Format(queryText, string.Join(",", pickupParameters),
                dateTimeNowOptimizeString,
                DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                DateMove.Date.AddDays(parameters1C.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                parameters1C.GetValue("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                parameters1C.GetValue("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                pickupWorkingHoursJoinType,
                useIndexHint);

            return cmd;
        }

        private async Task<SqlCommand> IntervalListCommand(SqlConnection connection, IntervalListQuery query, DatabaseType databaseType, string zoneId)
        {
            var parameters1C = await GetGlobalParameters(connection);

            string queryText = IntervalListQueries.IntervalList;
            SqlCommand cmd = new(queryText, connection)
            {
                CommandTimeout = 5
            };

            var queryTextBegin = TextFillGoodsTable(query, cmd);

            if (_configuration.GetValue<bool>("disableKeepFixedPlan"))
            {
                queryText = queryText.Replace(", KEEPFIXED PLAN", "");
                queryTextBegin = queryTextBegin.Replace(", KEEPFIXED PLAN", "");
            }

            var yourTimeDelivery = false;

            if (query.DeliveryType == Constants.YourTimeDelivery)
            {
                query.DeliveryType = Constants.CourierDelivery;
                yourTimeDelivery = true;
            }

            var DateMove = DateTime.Now.AddYears(2000);

            cmd.Parameters.AddWithValue("@P_AdressCode", query.AddressId != null ? query.AddressId : DBNull.Value);
            cmd.Parameters.AddWithValue("@PickupPoint1", query.PickupPoint != null ? query.PickupPoint : DBNull.Value);
            cmd.Parameters.AddWithValue("@P_Credit", query.Payment == "partly_pay" ? 1 : 0);
            cmd.Parameters.AddWithValue("@P_Floor", (double)(query.Floor != null ? query.Floor : parameters1C.GetValue("Логистика_ЭтажПоУмолчанию")));
            cmd.Parameters.AddWithValue("@P_DaysToShow", 7);
            cmd.Parameters.AddWithValue("@P_DateTimeNow", DateMove);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodBegin", DateMove.Date);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodEnd", DateMove.Date.AddDays(parameters1C.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1));
            cmd.Parameters.AddWithValue("@P_TimeNow", new DateTime(2001, 1, 1, DateMove.Hour, DateMove.Minute, DateMove.Second));
            cmd.Parameters.AddWithValue("@P_EmptyDate", new DateTime(2001, 1, 1, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_MaxDate", new DateTime(5999, 11, 11, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_GeoCode", zoneId);
            cmd.Parameters.AddWithValue("@P_OrderDate", query.OrderDate.AddYears(2000));
            cmd.Parameters.AddWithValue("@P_OrderNumber", query.OrderNumber != null ? query.OrderNumber : DBNull.Value);
            cmd.Parameters.AddWithValue("@P_ApplyShifting", (int)parameters1C.GetValue("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров"));
            cmd.Parameters.AddWithValue("@P_DaysToShift", (int)parameters1C.GetValue("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров"));
            cmd.Parameters.AddWithValue("@P_StockPriority", (int)parameters1C.GetValue("ПриоритизироватьСток_64854"));
            cmd.Parameters.AddWithValue("@P_YourTimeDelivery", yourTimeDelivery ? 1 : 0);

            string dateTimeNowOptimizeString = _configuration.GetValue<bool>("optimizeDateTimeNowEveryHour")
                ? DateMove.ToString("yyyy-MM-ddTHH:00:00")
                : DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            cmd.CommandText = queryTextBegin + string.Format(queryText,
                "",
                dateTimeNowOptimizeString,
                DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                DateMove.Date.AddDays(parameters1C.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                parameters1C.GetValue("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                parameters1C.GetValue("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                databaseType == DatabaseType.ReplicaTables ? _configuration.GetValue<string>("useIndexHintWarehouseDates") : ""); // index hint

            return cmd;
        }

        private async Task<SqlCommand> AvailableDeliveryTypesCommand(SqlConnection connection, AvailableDeliveryTypesQuery query, string deliveryType, DatabaseType databaseType)
        {
            var parameters1C = await GetGlobalParameters(connection);

            string queryText = AvailableDeliveryTypesQueries.AvailableDelivery;
            SqlCommand cmd = new(queryText, connection)
            {
                CommandTimeout = 5
            };

            var queryTextBegin = TextFillGoodsTable(query, cmd);

            if (_configuration.GetValue<bool>("disableKeepFixedPlan"))
            {
                queryText = queryText.Replace(", KEEPFIXED PLAN", "");
                queryTextBegin = queryTextBegin.Replace(", KEEPFIXED PLAN", "");
            }

            var DateMove = DateTime.Now.AddYears(2000);

            cmd.Parameters.AddWithValue("@P_CityCode", query.CityId);
            cmd.Parameters.AddWithValue("@P_Floor", (double)(parameters1C.GetValue("Логистика_ЭтажПоУмолчанию")));
            cmd.Parameters.AddWithValue("@P_DaysToShow", 7);
            cmd.Parameters.AddWithValue("@P_DateTimeNow", DateMove);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodBegin", DateMove.Date);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodEnd", DateMove.Date.AddDays(parameters1C.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1));
            cmd.Parameters.AddWithValue("@P_TimeNow", new DateTime(2001, 1, 1, DateMove.Hour, DateMove.Minute, DateMove.Second));
            cmd.Parameters.AddWithValue("@P_EmptyDate", new DateTime(2001, 1, 1, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_MaxDate", new DateTime(5999, 11, 11, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_ApplyShifting", (int)parameters1C.GetValue("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров"));
            cmd.Parameters.AddWithValue("@P_DaysToShift", (int)parameters1C.GetValue("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров"));
            cmd.Parameters.AddWithValue("@P_StockPriority", (int)parameters1C.GetValue("ПриоритизироватьСток_64854"));
            cmd.Parameters.AddWithValue("@P_YourTimeDelivery", deliveryType == Constants.YourTimeDelivery ? 1 : 0);
            cmd.Parameters.AddWithValue("@P_IsDelivery", deliveryType == Constants.Self ? 0 : 1);
            cmd.Parameters.AddWithValue("@P_IsPickup", deliveryType == Constants.Self ? 1 : 0);

            string pickupPointsString = string.Join(", ", query.PickupPoints
                .Select((value, index) =>
                {
                    string parameterName = $"@PickupPoint{index}";
                    cmd.Parameters.AddWithValue(parameterName, value);
                    return parameterName;
                }));

            string dateTimeNowOptimizeString = _configuration.GetValue<bool>("optimizeDateTimeNowEveryHour")
                ? DateMove.ToString("yyyy-MM-ddTHH:00:00")
                : DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            cmd.CommandText = queryTextBegin + string.Format(queryText,
                dateTimeNowOptimizeString,
                DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                DateMove.Date.AddDays(parameters1C.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                parameters1C.GetValue("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                parameters1C.GetValue("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                databaseType == DatabaseType.ReplicaTables ? _configuration.GetValue<string>("useIndexHintWarehouseDates") : "", // index hint
                pickupPointsString);

            return cmd;
        }
  
        public static string TextFillGoodsTable(AvailableDateQuery query, SqlCommand cmdGoodsTable, bool optimizeRowsCount, List<string> PickupsList)
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

                cmdGoodsTable.Parameters.AddWithValue(article, item.Article);
                cmdGoodsTable.Parameters.AddWithValue(code, string.IsNullOrEmpty(item.Code) ? DBNull.Value : item.Code);
                cmdGoodsTable.Parameters.AddWithValue(quantity, item.Quantity);

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

                    cmdGoodsTable.Parameters.AddWithValue(pickupPoint, string.Join(",", item.PickupPoints));

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

        private static string TextFillGoodsTable(IntervalListQuery query, SqlCommand cmdGoodsTable)
        {
            var resultString = CommonQueries.TableGoodsRawCreate;

            var parameters = query.OrderItems.Select((item, index) =>
            {
                var article = $"@Article{index}";
                var code = $"@Code{index}";
                var quantity = $"@Quantity{index}";

                cmdGoodsTable.Parameters.AddWithValue(article, item.Article);
                cmdGoodsTable.Parameters.AddWithValue(code, string.IsNullOrEmpty(item.Code) ? DBNull.Value : item.Code);
                cmdGoodsTable.Parameters.AddWithValue(quantity, item.Quantity);

                return $"({article}, {code}, NULL, {quantity})";
            }).ToList();

            if (parameters.Count > 0)
            {
                resultString += string.Format(CommonQueries.TableGoodsRawInsert, string.Join(", ", parameters));
            }

            return resultString;
        }

        private static string TextFillGoodsTable(AvailableDeliveryTypesQuery query, SqlCommand cmdGoodsTable)
        {
            var resultString = AvailableDeliveryTypesQueries.GoodsRawCreate;

            var parameters = query.OrderItems.Select((item, index) =>
            {
                var article = $"@Article{index}";
                var code = $"@Code{index}";
                var quantity = $"@Quantity{index}";

                cmdGoodsTable.Parameters.AddWithValue(article, item.Article);
                cmdGoodsTable.Parameters.AddWithValue(code, string.IsNullOrEmpty(item.Code) ? DBNull.Value : item.Code);
                cmdGoodsTable.Parameters.AddWithValue(quantity, item.Quantity);

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
