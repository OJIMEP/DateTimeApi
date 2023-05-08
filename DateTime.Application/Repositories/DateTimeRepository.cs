using Dapper;
using DateTime.Application.Cache;
using DateTime.Application.Database;
using DateTime.Application.Models;
using DateTime.Application.Queries;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using System.Data;
using System.Diagnostics;

namespace DateTime.Application.Repositories
{
    public class DateTimeRepository : IDateTimeRepository
    {
        private readonly IConfiguration _configuration;
        private readonly IDbConnectionFactory _dbConnectionFactory;
        private readonly RedisSettings _redisSettings;
        private readonly IConnectionMultiplexer _redis;

        public DateTimeRepository(IDbConnectionFactory dbConnectionFactory, RedisSettings redisSettings, IConnectionMultiplexer redis, IConfiguration configuration)
        {
            _dbConnectionFactory = dbConnectionFactory;
            _redisSettings = redisSettings;
            _redis = redis;
            _configuration = configuration;
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
        public async Task<AvailableDateResult> GetAvailableDateFromDatabase(AvailableDateQuery query, CancellationToken token = default)
        {
            DbConnection dbConnection = await GetDbConnection(token: token);

            SqlConnection connection = dbConnection.Connection;

            var globalParameters = await GetGlobalParameters(connection);

            var queryParameters = new DynamicParameters();

            string queryText = AvailableDateQueryText(query, queryParameters, globalParameters, dbConnection);

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

        private string AvailableDateQueryText(AvailableDateQuery query, DynamicParameters parameters, List<GlobalParameter> globalParameters, DbConnection dbConnection)
        {
            List<string> pickups = new();

            var queryTextBegin = TextFillGoodsTable(query, parameters, true, pickups);

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
                parameters.Add(parameterString, pickupPoint);
            }
            if (pickupParameters.Count == 0)
            {
                pickupParameters.Add("NULL");
            }

            var DateMove = System.DateTime.Now.AddMonths(24000);

            parameters.Add("@P_CityCode", query.CityId);
            parameters.Add("@P_DateTimeNow", DateMove);
            parameters.Add("@P_DateTimePeriodBegin", DateMove.Date);
            parameters.Add("@P_DateTimePeriodEnd", DateMove.Date.AddDays(globalParameters.GetValue("rsp_КоличествоДнейЗаполненияГрафика") - 1));
            parameters.Add("@P_TimeNow", new System.DateTime(2001, 1, 1, DateMove.Hour, DateMove.Minute, DateMove.Second));
            parameters.Add("@P_EmptyDate", new System.DateTime(2001, 1, 1, 0, 0, 0));
            parameters.Add("@P_MaxDate", new System.DateTime(5999, 11, 11, 0, 0, 0));
            parameters.Add("@P_DaysToShow", 7);
            parameters.Add("@P_ApplyShifting", (int)globalParameters.GetValue("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров"));
            parameters.Add("@P_DaysToShift", (int)globalParameters.GetValue("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров"));

            if (query.CheckQuantity)
            {
                parameters.Add("@P_StockPriority", (int)globalParameters.GetValue("ПриоритизироватьСток_64854"));
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

        private async Task<DbConnection> GetDbConnection(CancellationToken token = default, bool logging = true)
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

            if (_redisSettings.Enabled)
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

                if (_redisSettings.Enabled)
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
            if (!_redisSettings.Enabled)
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

            if (!_redisSettings.Enabled)
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
