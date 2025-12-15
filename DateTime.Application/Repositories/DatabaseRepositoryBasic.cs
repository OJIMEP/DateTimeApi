using Dapper;
using DateTimeService.Application.Database;
using DateTimeService.Application.Database.DatabaseManagement;
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
        private readonly RedisRepository _redisRepository;

        public DatabaseRepositoryBasic(IHttpContextAccessor contextAccessor, IConfiguration configuration,
            IDbConnectionFactory dbConnectionFactory, IGeoZones geoZones, RedisRepository redisRepository)
        {
            _contextAccessor = contextAccessor;
            _configuration = configuration;
            _dbConnectionFactory = dbConnectionFactory;
            _geoZones = geoZones;
            _redisRepository = redisRepository;
        }

        public async Task<AvailableDateResult> GetAvailableDates(AvailableDateQuery query, CancellationToken token = default)
        {
            var resultDict = new AvailableDateResult
            {
                WithQuantity = query.CheckQuantity
            };

            if (query.Codes.Count == 0)
            {
                return resultDict;
            }

            if (query.UsePreliminaryCalculation)
            {
                return await GetAvailableDatesPreliminaryCalculation(query, token);
            }

            DbConnection dbConnection = await _dbConnectionFactory.GetDbConnection(ServiceEndpoint.AvailableDate, token: token);

            List<AvailableDateRecord> dbResult = new();

            using (var connection = dbConnection.Connection)
            {
                SqlCommand command;

                command = await AvailableDateCommand(connection, query, dbConnection);

                Stopwatch watch = Stopwatch.StartNew();

                SqlDataReader dr = await command.ExecuteReaderAsync(token);
                
                if (dr.HasRows)
                {
                    while (await dr.ReadAsync(token))
                    {
                        var record = new AvailableDateRecord
                        {
                            Article = dr.GetString(0),
                            Code = dr.GetString(1),
                            Courier = dr.GetDateTime(2).AddYears(-2000),
                            Self = dr.GetDateTime(3).AddYears(-2000),
                            YourTimeInterval = dr.GetInt32(4)
                        };
                        
                        dbResult.Add(record);
                    }
                }

                _ = dr.CloseAsync();
                
                watch.Stop();
                lock (_contextAccessor.HttpContext.Items)
                {
                    _contextAccessor.HttpContext.Items["TimeSqlExecution"] = watch.ElapsedMilliseconds;
                }              
            }

            resultDict.FillFromAvailableDateRecords(dbResult, query);

            return resultDict;
        }

        public async Task<IntervalListResult> GetIntervalList(IntervalListQuery query, CancellationToken token = default)
        {
            var result = new IntervalListResult();

            DbConnection dbConnection = await _dbConnectionFactory.GetDbConnection(ServiceEndpoint.IntervalList, token);

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

                var globalParameters = await GetGlobalParameters(connection, token);
                var isBelpostDelivery = query.PickupPointType == Constants.BelpostPickupPoint;
                var courierDelivery = query.DeliveryType == Constants.CourierDelivery;

                if (isBelpostDelivery)
                {
                    query.DeliveryType = Constants.Self;
                    query.PickupPoint = globalParameters.GetString("БелпочтаКодЦентральногоОтделения");
                }

                SqlCommand command = await IntervalListCommand(connection, query, dbConnection.DatabaseType, zoneId, globalParameters);

                watch.Restart();

                SqlDataReader dr = command.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        var begin = dr.GetDateTime(0).AddYears(-2000);
                        var end = dr.GetDateTime(1).AddYears(-2000);
                        var bonus = dr.GetInt32(2) == 1;
                        var intervalType = "";
                        if (courierDelivery)
                        {
                            intervalType = dr.GetString(3);
                        }

                        if (isBelpostDelivery)
                        {
                            begin = begin.AddDays(globalParameters.GetValue("БелпочтаМинимальныйСрокДоставки"));
                            end = end.AddDays(globalParameters.GetValue("БелпочтаМаксимальныйСрокДоставки"));
                        }

                        result.Data.Add(new IntervalListElementResult
                        {
                            Begin = begin,
                            End = end,
                            Bonus = bonus,
                            IntervalType = intervalType
                        });

                        if (isBelpostDelivery) { break; }
                    }
                }

                _ = dr.CloseAsync();

                watch.Stop();

                _contextAccessor.HttpContext.Items["TimeSqlExecution"] = watch.ElapsedMilliseconds;
            }

            return result;
        }

        public async Task<DeliveryTypeAvailabilityResult> GetDeliveryTypeAvailability(AvailableDeliveryTypesQuery query, string deliveryType, CancellationToken token = default)
        {
            if (deliveryType == Constants.Self && !query.PickupPoints.Any())
            {
                return new DeliveryTypeAvailabilityResult
                {
                    deliveryType = deliveryType,
                    available = false
                };
            }

            bool deliveryTypeAvailable;
            long elapsedMs;

            DbConnection dbConnection = await _dbConnectionFactory.GetDbConnection(ServiceEndpoint.AvailableDeliveryTypes, token);

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
      
        private async Task<SqlCommand> AvailableDateCommand(SqlConnection connection, AvailableDateQuery query, DbConnection dbConnection)
        {
            var parameters1C = await GetGlobalParameters(connection);

            string queryText = "";

            SqlCommand cmd = new(queryText, connection)
            {
                CommandTimeout = 5
            };

            List<string> pickups = new();

            var queryTextBegin = TextFillGoodsTable(query, cmd, true, pickups);

            List<string> queryParts = new()
            {
                query.CheckQuantity? AvailableDateQueries.AvailableDateWithCount1 : AvailableDateQueries.AvailableDate1,
                dbConnection.UseAggregations ? AvailableDateQueries.AvailableDate2MinimumWarehousesCustom : AvailableDateQueries.AvailableDate2MinimumWarehousesBasic,
                query.CheckQuantity ? AvailableDateQueries.AvailableDateWithCount3 : AvailableDateQueries.AvailableDate3,
                AvailableDateQueries.AvailableDate4SourcesWithPrices,
                AvailableDateQueries.PickupDateShift,
                query.CheckQuantity ? AvailableDateQueries.AvailableDateWithCount5 : AvailableDateQueries.AvailableDate5,
                dbConnection.UseAggregations ? AvailableDateQueries.AvailableDate6IntervalsCustom : AvailableDateQueries.AvailableDate6IntervalsBasic,
                AvailableDateQueries.AvailableDate7,
                dbConnection.UseAggregations ? AvailableDateQueries.AvailableDate8DeliveryPowerCustom : AvailableDateQueries.AvailableDate8DeliveryPowerBasic,
                AvailableDateQueries.AvailableDate9
            };

            queryText = String.Join("", queryParts);

            List<string> pickupParameters = new();
            foreach (var pickupPoint in pickups)
            {
                var parameterString = string.Format("@PickupPointAll{0}", pickups.IndexOf(pickupPoint));
                pickupParameters.Add(parameterString);
                cmd.Parameters.AddWithValue(parameterString, pickupPoint);
            }
            if (pickupParameters.Count == 0)
            {
                pickupParameters.Add("NULL");
            }

            var DateMove = DateTime.Now.AddYears(2000);

            cmd.Parameters.AddWithValue("@P_DateTimeNow", DateMove);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodBegin", DateMove.Date);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodEnd", DateMove.Date.AddDays(parameters1C.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1));
            cmd.Parameters.AddWithValue("@P_TimeNow", new DateTime(2001, 1, 1, DateMove.Hour, DateMove.Minute, DateMove.Second));
            cmd.Parameters.AddWithValue("@P_EmptyDate", new DateTime(2001, 1, 1, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_MaxDate", new DateTime(5999, 11, 11, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_DaysToShow", 7);
            cmd.Parameters.AddWithValue("@P_ApplyShifting", (int)parameters1C.GetValue("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров"));
            cmd.Parameters.AddWithValue("@P_DaysToShift", (int)parameters1C.GetValue("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров"));

            cmd.Parameters.Add("@P_CityCode", SqlDbType.NVarChar, 10);
            cmd.Parameters["@P_CityCode"].Value = query.CityId;

            if (query.CheckQuantity)
            {
                cmd.Parameters.AddWithValue("@P_StockPriority", (int)parameters1C.GetValue("ПриоритизироватьСток_64854"));
            }

            string dateTimeNowOptimizeString = DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            var pickupWorkingHoursJoinType = _configuration.GetValue<string>("pickupWorkingHoursJoinType");

            string useIndexHint = string.Empty;

            queryText = queryTextBegin + string.Format(queryText, string.Join(",", pickupParameters),
                dateTimeNowOptimizeString,
                DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                DateMove.Date.AddDays(parameters1C.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                parameters1C.GetValue("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                parameters1C.GetValue("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                pickupWorkingHoursJoinType,
                useIndexHint,
                _configuration.GetValue<int>("AvailableDatePickupPointsCount", 5));

            cmd.CommandText = queryText;

            return cmd;
        }

        private async Task<AvailableDateResult> GetAvailableDatesPreliminaryCalculation(AvailableDateQuery query, CancellationToken token = default)
        {
            var resultDict = new AvailableDateResult
            {
                WithQuantity = query.CheckQuantity
            };

            // Запускаем обе функции параллельно
            var taskDelivery = Task.Run(() => GetAvailableDeliveryDates(query, token));
            var taskPickup = Task.Run(() => GetAvailablePickupDates(query, token));

            // Ожидаем завершения обеих задач
            await Task.WhenAll(taskDelivery, taskPickup);

            // Получаем результаты
            var deliveryResult = taskDelivery.Result;
            var pickupResult = taskPickup.Result;

            lock (_contextAccessor.HttpContext.Items)
            {
                var items = _contextAccessor.HttpContext.Items;
                var timeSqlExecution = items.TryGetValue("TimeSqlExecution", out object? timeSql) ? (long)(timeSql ?? 0) : 0;
                var timeSqlExecutionDelivery = items.TryGetValue("TimeSqlExecutionDelivery", out object? timeSqlDelivery) ? (long)(timeSqlDelivery ?? 0) : 0;
                var timeSqlExecutionPickup = items.TryGetValue("TimeSqlExecutionPickup", out object? timeSqlPickup) ? (long)(timeSqlPickup ?? 0) : 0;

                var currentMaxSqlExecution = Math.Max(deliveryResult.sqlExecutionTime, pickupResult.sqlExecutionTime);

                _contextAccessor.HttpContext.Items["TimeSqlExecution"] = Math.Max(timeSqlExecution, currentMaxSqlExecution);
                _contextAccessor.HttpContext.Items["TimeSqlExecutionDelivery"] = Math.Max(timeSqlExecutionDelivery, deliveryResult.sqlExecutionTime);
                _contextAccessor.HttpContext.Items["TimeSqlExecutionPickup"] = Math.Max(timeSqlExecutionPickup, pickupResult.sqlExecutionTime);
            }

            // Полное внешнее объединение
            var deliveryDict = deliveryResult.availableDateRecords
                .ToDictionary(x => new { x.Article, x.Code });
            var pickupDict = pickupResult.availableDateRecords
                .ToDictionary(x => new { x.Article, x.Code });

            // Получаем все уникальные ключи из обоих списков
            var allKeys = deliveryDict.Keys.Union(pickupDict.Keys).Distinct();

            var mergedList = new List<AvailableDateRecord>();
            var maxDate = new DateTime(3999, 11, 11, 0, 0, 0);

            foreach (var key in allKeys)
            {
                deliveryDict.TryGetValue(key, out AvailableDateRecord? deliveryRecord);
                pickupDict.TryGetValue(key, out AvailableDateRecord? pickupRecord);

                mergedList.Add(new AvailableDateRecord
                {
                    Article = key.Article,
                    Code = key.Code,
                    Courier = deliveryRecord?.Courier ?? maxDate,
                    Self = pickupRecord?.Self ?? maxDate,
                    YourTimeInterval = deliveryRecord?.YourTimeInterval ?? 0
                });
            }

            resultDict.FillFromAvailableDateRecords(mergedList, query);

            return resultDict;
        }

        private async Task<AvailableDatePreliminaryCalculationResult> GetAvailableDeliveryDates(AvailableDateQuery query, CancellationToken token = default)
        {
            DbConnection dbConnection = await _dbConnectionFactory.GetDbConnection(ServiceEndpoint.AvailableDate, token: token);

            AvailableDatePreliminaryCalculationResult result = new();

            using (var connection = dbConnection.Connection)
            {
                SqlCommand command;

                command = await AvailableDeliveryDatesCommand(connection, query, dbConnection.UseAggregations, token);

                Stopwatch watch = Stopwatch.StartNew();

                SqlDataReader dr = await command.ExecuteReaderAsync(token);

                if (dr.HasRows)
                {
                    while (await dr.ReadAsync(token))
                    {
                        var record = new AvailableDateRecord
                        {
                            Article = dr.GetString(0),
                            Code = dr.GetString(1),
                            Courier = dr.GetDateTime(2).AddYears(-2000),
                            YourTimeInterval = dr.GetInt32(3)
                        };

                        result.availableDateRecords.Add(record);
                    }
                }

                _ = dr.CloseAsync();

                watch.Stop();
                result.sqlExecutionTime = watch.ElapsedMilliseconds;
            }

            return result;
        }

        private async Task<AvailableDatePreliminaryCalculationResult> GetAvailablePickupDates(AvailableDateQuery query, CancellationToken token = default)
        {
            DbConnection dbConnection = await _dbConnectionFactory.GetDbConnection(ServiceEndpoint.AvailableDate, token: token);

            AvailableDatePreliminaryCalculationResult result = new();

            using (var connection = dbConnection.Connection)
            {
                SqlCommand command;

                command = await AvailablePickupDatesCommand(connection, query, token);

                Stopwatch watch = Stopwatch.StartNew();

                SqlDataReader dr = await command.ExecuteReaderAsync(token);

                if (dr.HasRows)
                {
                    while (await dr.ReadAsync(token))
                    {
                        var record = new AvailableDateRecord
                        {
                            Article = dr.GetString(0),
                            Code = dr.GetString(1),
                            Self = dr.GetDateTime(2).AddYears(-2000)
                        };

                        result.availableDateRecords.Add(record);
                    }
                }

                _ = dr.CloseAsync();

                watch.Stop();
                result.sqlExecutionTime = watch.ElapsedMilliseconds;
            }

            return result;
        }

        private async Task<SqlCommand> AvailableDeliveryDatesCommand(SqlConnection connection, AvailableDateQuery query, bool useAggregation, CancellationToken token = default)
        {
            string queryText = "";

            SqlCommand cmd = new(queryText, connection)
            {
                CommandTimeout = 2
            };

            var queryTextBegin = TextFillGoodsTablePreliminaryCalculation(query, cmd);

            var parameters1C = await GetGlobalParameters(connection, token);
            var currentDate = DateTime.Now.AddYears(2000);

            SetCommonParameters(cmd, currentDate, parameters1C);

            cmd.Parameters.Add("@P_CityCode", SqlDbType.NVarChar, 10);
            cmd.Parameters["@P_CityCode"].Value = query.CityId;

            List<string> queryParts = new()
            {
                AvailableDatePreliminaryCalculationQuery.AvailableDateDelivery1,
                useAggregation ? AvailableDatePreliminaryCalculationQuery.TempAllIntervalsAggregate : AvailableDatePreliminaryCalculationQuery.TempAllIntervals,
                AvailableDatePreliminaryCalculationQuery.AvailableDateDelivery2
            };

            var mainQueryText = String.Join("", queryParts);
            //var mainQueryText = AvailableDatePreliminaryCalculationQuery.AvailableDateDelivery;

            queryText = queryTextBegin + string.Format(mainQueryText,
                currentDate.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                currentDate.Date.AddDays(parameters1C.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1).ToString("yyyy-MM-ddTHH:mm:ss")
            );

            cmd.CommandText = queryText;

            return cmd;
        }

        private async Task<SqlCommand> AvailablePickupDatesCommand(SqlConnection connection, AvailableDateQuery query, CancellationToken token = default)
        {
            string queryText = "";

            SqlCommand cmd = new(queryText, connection)
            {
                CommandTimeout = 2
            };

            var queryTextBegin = TextFillGoodsTablePreliminaryCalculation(query, cmd, true);

            var parameters1C = await GetGlobalParameters(connection, token);
            var currentDate = DateTime.Now.AddYears(2000);

            SetCommonParameters(cmd, currentDate, parameters1C);

            queryText = queryTextBegin + AvailableDatePreliminaryCalculationQuery.AvailableDatePickup;

            cmd.CommandText = queryText;

            return cmd;
        }

        private void SetCommonParameters(SqlCommand cmd, DateTime currentDate, List<GlobalParameter> parameters1C)
        {
            cmd.Parameters.AddWithValue("@P_DateTimeNow", currentDate);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodBegin", currentDate.Date);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodEnd", currentDate.Date.AddDays(parameters1C.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1));
            cmd.Parameters.AddWithValue("@P_TimeNow", new DateTime(2001, 1, 1, currentDate.Hour, currentDate.Minute, currentDate.Second));
            cmd.Parameters.AddWithValue("@P_EmptyDate", new DateTime(2001, 1, 1, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_MaxDate", new DateTime(5999, 11, 11, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_DaysToShow", 7);
            cmd.Parameters.AddWithValue("@P_ApplyShifting", (int)parameters1C.GetValue("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров"));
            cmd.Parameters.AddWithValue("@P_DaysToShift", (int)parameters1C.GetValue("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров"));
            cmd.Parameters.AddWithValue("@P_ActualDate", currentDate.AddMinutes(_configuration.GetValue<int>("ActualityDateGap") * -1));
        }

        private async Task<SqlCommand> IntervalListCommand(SqlConnection connection, 
            IntervalListQuery query, 
            DatabaseType databaseType, 
            string zoneId, 
            List<GlobalParameter> globalParameters)
        {
            string queryText = IntervalListQueries.IntervalList;
            SqlCommand cmd = new(queryText, connection)
            {
                CommandTimeout = 5
            };

            var queryTextBegin = TextFillGoodsTable(query, cmd);

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
            cmd.Parameters.AddWithValue("@P_Floor", query.FloorForIntervalList());
            cmd.Parameters.AddWithValue("@P_DaysToShow", 7);
            cmd.Parameters.AddWithValue("@P_DateTimeNow", DateMove);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodBegin", DateMove.Date);
            cmd.Parameters.AddWithValue("@P_DateTimePeriodEnd", DateMove.Date.AddDays(globalParameters.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1));
            cmd.Parameters.AddWithValue("@P_TimeNow", new DateTime(2001, 1, 1, DateMove.Hour, DateMove.Minute, DateMove.Second));
            cmd.Parameters.AddWithValue("@P_EmptyDate", new DateTime(2001, 1, 1, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_MaxDate", new DateTime(5999, 11, 11, 0, 0, 0));
            cmd.Parameters.AddWithValue("@P_GeoCode", zoneId);
            cmd.Parameters.AddWithValue("@P_OrderDate", query.OrderDate.AddYears(2000));
            cmd.Parameters.AddWithValue("@P_OrderNumber", query.OrderNumber != null ? query.OrderNumber : DBNull.Value);
            cmd.Parameters.AddWithValue("@P_ApplyShifting", (int)globalParameters.GetValue("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров"));
            cmd.Parameters.AddWithValue("@P_DaysToShift", (int)globalParameters.GetValue("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров"));
            cmd.Parameters.AddWithValue("@P_StockPriority", (int)globalParameters.GetValue("ПриоритизироватьСток_64854"));
            cmd.Parameters.AddWithValue("@P_YourTimeDelivery", yourTimeDelivery ? 1 : 0);
            cmd.Parameters.AddWithValue("@LoadedIntervalsDays", (int)globalParameters.GetValue("ДнейСЗагруженнымиИнтерваламиДоставкиОтДатыДоступностиЗаказа"));
            cmd.Parameters.AddWithValue("@P_LoadedIntervalsUsagePercent",
                (double)globalParameters.GetValue("ПроцентФактическиИспользованныхМощностейДляЗагруженныхИнтервалов"));

            cmd.Parameters.Add("@P_Jewelry", SqlDbType.Binary, 16).Value = globalParameters.GetRef("СегментНоменклатурыЮвелирныеИзделия");

            string dateTimeNowOptimizeString = DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            queryText = queryTextBegin + string.Format(queryText,
                "",
                dateTimeNowOptimizeString,
                DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                DateMove.Date.AddDays(globalParameters.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                globalParameters.GetValue("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                globalParameters.GetValue("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                databaseType == DatabaseType.ReplicaTables ? _configuration.GetValue<string>("useIndexHintWarehouseDates") : "", // index hint
                globalParameters.GetValue("EPassКоличествоРабочихДнейДляРегистрацииШтрихкода")
            ); 

            cmd.CommandText = queryText;

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
            cmd.Parameters.AddWithValue("@P_YourTimeDelivery", deliveryType == Constants.YourTimeDelivery ? 1 : 0);
            cmd.Parameters.AddWithValue("@P_IsDelivery", deliveryType == Constants.Self ? 0 : 1);
            cmd.Parameters.AddWithValue("@P_IsPickup", deliveryType == Constants.Self ? 1 : 0);
            
            string pickupPointsString = !query.PickupPoints.Any() ? "''" : string.Join(", ", query.PickupPoints.Take(10)
                .Select((value, index) =>
                {
                    string parameterName = $"@PickupPoint{index}";
                    cmd.Parameters.AddWithValue(parameterName, value);
                    return parameterName;
                }));

            string dateTimeNowOptimizeString = DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

            queryText = queryTextBegin + string.Format(queryText,
                dateTimeNowOptimizeString,
                DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                DateMove.Date.AddDays(parameters1C.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                parameters1C.GetValue("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                parameters1C.GetValue("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа"),
                databaseType == DatabaseType.ReplicaTables ? _configuration.GetValue<string>("useIndexHintWarehouseDates") : "", // index hint
                pickupPointsString);

            cmd.CommandText = queryText;

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

                //cmdGoodsTable.Parameters.AddWithValue(article, item.Article);
                //cmdGoodsTable.Parameters.AddWithValue(code, string.IsNullOrEmpty(item.Code) ? DBNull.Value : item.Code);
                cmdGoodsTable.Parameters.AddWithValue(quantity, item.Quantity);
                cmdGoodsTable.Parameters.Add(article, SqlDbType.NVarChar, 11);
                cmdGoodsTable.Parameters[article].Value = item.Article;

                cmdGoodsTable.Parameters.Add(code, SqlDbType.NVarChar, 11);
                cmdGoodsTable.Parameters[code].Value = string.IsNullOrEmpty(item.Code) ? DBNull.Value : item.Code;
                
                //cmdGoodsTable.Parameters.Add(quantity, SqlDbType.Int, 10);
                //cmdGoodsTable.Parameters[quantity].Value = item.Quantity;

                var parameterString = $"({article}, {code}, NULL, {quantity})";

                parameters.Add(parameterString);

                if (parameters.Count == insertRowsLimit)
                {
                    resultString += string.Format(CommonQueries.TableGoodsRawInsert, string.Join(", ", parameters));

                    parameters.Clear();
                }

                if (item.PickupPoints.Any())
                {
                    var pickupPoint = $"@PickupPoint{index}";

                    //cmdGoodsTable.Parameters.AddWithValue(pickupPoint, string.Join(",", item.PickupPoints.Take(3)));
                    cmdGoodsTable.Parameters.Add(pickupPoint, SqlDbType.NVarChar, 150);
                    cmdGoodsTable.Parameters[pickupPoint].Value = string.Join(",", item.PickupPoints); //читерство .Take(10)

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

        private static string TextFillGoodsTablePreliminaryCalculation(AvailableDateQuery query, SqlCommand cmdGoodsTable, bool withPickupPoints = false)
        {
            var resultString = "";
            int maxCodes = 50;

            var pickupPoints = query.Codes.FirstOrDefault()?.PickupPoints ?? new List<string>();
            var pickups = pickupPoints.Concat(Enumerable.Repeat<string?>(null, maxCodes)).Take(maxCodes).ToList();

            var parameters = new List<string>();

            for (int index = 0; index < maxCodes; index++)
            {
                string article = $"@Article{index}";
                string code = $"@Code{index}";

                var parameterString = $"({article}, {code})";

                parameters.Add(parameterString);

                cmdGoodsTable.Parameters.Add(article, SqlDbType.NVarChar, 50);
                cmdGoodsTable.Parameters.Add(code, SqlDbType.NVarChar, 11);

                if (index < query.Codes.Count)
                {
                    // Берем существующий элемент
                    CodeItemQuery item = query.Codes[index];
                    cmdGoodsTable.Parameters[article].Value = !string.IsNullOrEmpty(item.Code) ? DBNull.Value : item.Article;
                    cmdGoodsTable.Parameters[code].Value = string.IsNullOrEmpty(item.Code) ? DBNull.Value : item.Code;
                }
                else
                {
                    // Добавляем NULL значения для недостающих элементов
                    cmdGoodsTable.Parameters[article].Value = DBNull.Value;
                    cmdGoodsTable.Parameters[code].Value = DBNull.Value;
                }
            }

            resultString += CommonQueries.TableGoodsRawCreatePreliminary;
            resultString += string.Format(CommonQueries.TableGoodsRawInsertPreliminary, string.Join(", ", parameters));

            if (withPickupPoints)
            {
                var pickupPointsList = new List<string>();

                int i = 0;
                foreach (var pickupPoint in pickups)
                {
                    var parameterString = $"@PickupPoint{i}";
                    cmdGoodsTable.Parameters.Add(parameterString, SqlDbType.NVarChar, 11);
                    cmdGoodsTable.Parameters[parameterString].Value = string.IsNullOrEmpty(pickupPoint) ? DBNull.Value : pickupPoint;

                    pickupPointsList.Add(parameterString);
                    i++;
                }

                resultString += string.Format(CommonQueries.PickupPointsQuery, string.Join(", ", pickupPointsList));
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

            var parameters = await _redisRepository.GetFromCache<List<GlobalParameter>>(key);

            if (parameters is null)
            {
                parameters = await GlobalParameter.GetParameters(connection, token);

                await _redisRepository.SaveToCache(parameters, key, 600);
            }

            watch.Stop();

            lock (_contextAccessor.HttpContext.Items)
            {
                _contextAccessor.HttpContext.Items["TimeGlobalParametersExecution"] = watch.ElapsedMilliseconds;
            }

            return parameters;
        }
    

        public async Task<bool> UsePreliminaryCalculation(string cityId, CancellationToken token = default)
        {
            bool result = false;

            if (!_configuration.GetValue<bool>("UsePreliminaryCalculation"))
            {
                return result;
            }

            using var dbConnection = await _dbConnectionFactory.GetDbConnection(ServiceEndpoint.AvailableDate, token: token);

            Stopwatch watch = Stopwatch.StartNew();

            try
            {
                string query = """
                    DECLARE @DateTimeNow datetime2(3) = DATEADD(YEAR, 2000, GETDATE());
                    DECLARE @ActualityDate datetime2(3) = DATEADD(MINUTE, -1 * @ActualityDateGap, @DateTimeNow);

                    WITH Geozone AS (
                    	SELECT TOP 1
                    		T3._Fld26708RRef AS Геозона --геозона из рс векРасстоянияАВ
                    	FROM (SELECT
                    			T1._Fld25549 AS Fld25549_,
                    			MAX(T1._Period) AS MAXPERIOD_ 
                    		FROM dbo._InfoRg21711 T1 WITH (NOLOCK)
                    		WHERE T1._Fld26708RRef <> 0x00 AND T1._Fld25549 = @CityId
                    		GROUP BY T1._Fld25549) T2
                    	INNER JOIN dbo._InfoRg21711 T3 WITH (NOLOCK)
                    	ON T2.Fld25549_ = T3._Fld25549 AND T2.MAXPERIOD_ = T3._Period
                    ),
                    Warehouses AS (
                    	SELECT DISTINCT
                    		T1._Fld23372RRef Склад
                    	FROM dbo._Reference114_VT23370 T1 WITH (NOLOCK)
                    		INNER JOIN Geozone T2 WITH (NOLOCK)
                    		ON T1._Reference114_IDRRef = T2.Геозона
                    		INNER JOIN dbo._InfoRg33512 T3 WITH (NOLOCK)
                    		ON T1._Fld23372RRef = T3._Fld33513RRef
                    		AND T3._Fld33514 = 0x01
                    ),
                    NotActualDates AS (
                    	SELECT TOP 1
                    		T1._Fld33513RRef AS Склад
                    	FROM dbo._InfoRg33512 T1 WITH (NOLOCK)
                    		INNER JOIN Warehouses T2 WITH (NOLOCK)
                    		ON T1._Fld33513RRef = T2.Склад
                    			AND T1._Fld33525 < @ActualityDate
                    )
                    SELECT
                    	SUM(CASE WHEN T2.Склад IS NULL THEN 0 ELSE 1 END)
                    FROM Warehouses T1 WITH (NOLOCK)
                    	LEFT JOIN NotActualDates T2 WITH (NOLOCK)
                    	ON T1.Склад = T2.Склад
                    HAVING 
                    	SUM(CASE WHEN T2.Склад IS NULL THEN 0 ELSE 1 END) = 0
                    """
                ;

                var actualityDateGap = _configuration.GetValue<int>("ActualityDateGap");

                var queryResult = await dbConnection.Connection.QueryAsync<int>(new CommandDefinition(query, new { CityId = cityId, ActualityDateGap = actualityDateGap }, cancellationToken: token));

                result = queryResult != null && queryResult.Any();
            }
            catch (Exception)
            {
                throw;
            }

            watch.Stop();
            lock (_contextAccessor.HttpContext.Items)
            {
                _contextAccessor.HttpContext.Items["TimeUsePreliminaryCalculation"] = watch.ElapsedMilliseconds;
            }

            return result;
        }
    }
}
