﻿using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Diagnostics;
using System.Net.Sockets;
using System.Net;
using System.Text.Json;
using System.Text;
using Microsoft.Extensions.Configuration;
using DateTimeService.Application.Database.DatabaseManagement.Elastic;
using DateTimeService.Application.Logging;
using DateTimeService.Api;
using DateTimeService.Application.Queries;
using DateTimeService.Application.Models;
using DateTimeService.Application.Repositories;

namespace DateTimeService.Application.Database.DatabaseManagement
{
    public class DatabaseCheckService: IDatabaseCheck
    {
        private readonly ILogger<DatabaseCheckService> _logger;
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly bool _productionEnv;

        public DatabaseCheckService(ILogger<DatabaseCheckService> logger, IHttpClientFactory httpClientFactory, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;

            _productionEnv = configuration["Environment"] == "Production";          
        }

        public async Task<bool> CheckAggregationsAsync(DatabaseInfo databaseInfo, CancellationToken cancellationToken)
        {
            int result = -1;

            Stopwatch watch = new();
            watch.Start();
            try
            {
                using SqlConnection conn = new(databaseInfo.Connection);

                conn.Open();

                string query = DbCheckQueries.CheckAggregations;

                SqlCommand cmd = new(query, conn)
                {
                    CommandTimeout = 20,
                    CommandText = query
                };

                //execute the SQLCommand
                var dataReader = await cmd.ExecuteReaderAsync(cancellationToken);
                if (dataReader.HasRows)
                {
                    if (dataReader.Read())
                    {

                        if (dataReader.GetInt32(0) == 0)
                        {
                            result = 0;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                result = -1;

                var logElement = new ElasticLogElement
                {
                    ErrorDescription = ex.Message,
                    Status = LogStatus.Error,
                    DatabaseConnection = databaseInfo.ConnectionWithoutCredentials
                };

                _logger.LogElastic(logElement);

            }
            watch.Stop();

            return result >= 0;
        }

        public async Task<bool> CheckAvailabilityAsync(DatabaseInfo databaseInfo, CancellationToken cancellationToken, long executionLimit = 5000)
        {
            int result = 1;

            Stopwatch watch = new();
            watch.Start();
            try
            {
                using SqlConnection conn = new(databaseInfo.Connection);
                await conn.OpenAsync();

                List<string> queryParts = new()
                {
                    AvailableDateQueries.AvailableDate1,
                    AvailableDateQueries.AvailableDate2MinimumWarehousesBasic,
                    AvailableDateQueries.AvailableDate3,
                    AvailableDateQueries.AvailableDate4SourcesWithPrices,
                    AvailableDateQueries.PickupDateShift,
                    AvailableDateQueries.AvailableDate5,
                    AvailableDateQueries.AvailableDate6IntervalsBasic,
                    AvailableDateQueries.AvailableDate7,
                    AvailableDateQueries.AvailableDate8DeliveryPowerBasic,
                    AvailableDateQueries.AvailableDate9
                };

                string query = String.Join("", queryParts);

                var DateMove = DateTime.Now.AddYears(2000);
                var TimeNow = new DateTime(2001, 1, 1, DateMove.Hour, DateMove.Minute, DateMove.Second);
                var EmptyDate = new DateTime(2001, 1, 1, 0, 0, 0);
                var MaxDate = new DateTime(5999, 11, 11, 0, 0, 0);

                SqlCommand cmd = new(query, conn);

                List<string> pickups = new();

                var data = new AvailableDateQuery(true);

                var queryTextBegin = TextFillGoodsTable(data, cmd, true, pickups);

                //define the SqlCommand object
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

                var Parameters1C = new List<GlobalParameter>
                {
                    new GlobalParameter
                    {
                        Name = "rsp_КоличествоДнейЗаполненияГрафика",
                        ValueDouble = 5,
                    },
                    new GlobalParameter
                    {
                        Name = "КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа",
                        ValueDouble = 4
                    },
                    new GlobalParameter
                    {
                        Name = "ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа",
                        ValueDouble = 3
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
                    }
                };

                cmd.Parameters.Add("@P_CityCode", SqlDbType.NVarChar, 10);
                cmd.Parameters["@P_CityCode"].Value = data.CityId;

                cmd.Parameters.Add("@P_DateTimeNow", SqlDbType.DateTime);
                cmd.Parameters["@P_DateTimeNow"].Value = DateMove;

                cmd.Parameters.Add("@P_DateTimePeriodBegin", SqlDbType.DateTime);
                cmd.Parameters["@P_DateTimePeriodBegin"].Value = DateMove.Date;

                cmd.Parameters.Add("@P_DateTimePeriodEnd", SqlDbType.DateTime);
                cmd.Parameters["@P_DateTimePeriodEnd"].Value = DateMove.Date.AddDays(Parameters1C.First(x => x.Name.Contains("rsp_КоличествоДнейЗаполненияГрафика")).ValueDouble - 1);

                cmd.Parameters.Add("@P_TimeNow", SqlDbType.DateTime);
                cmd.Parameters["@P_TimeNow"].Value = TimeNow;

                cmd.Parameters.Add("@P_EmptyDate", SqlDbType.DateTime);
                cmd.Parameters["@P_EmptyDate"].Value = EmptyDate;

                cmd.Parameters.Add("@P_MaxDate", SqlDbType.DateTime);
                cmd.Parameters["@P_MaxDate"].Value = MaxDate;

                cmd.Parameters.Add("@P_DaysToShow", SqlDbType.Int);
                cmd.Parameters["@P_DaysToShow"].Value = 7;

                cmd.Parameters.Add("@P_ApplyShifting", SqlDbType.Int);
                cmd.Parameters["@P_ApplyShifting"].Value = Parameters1C.First(x => x.Name.Contains("ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров")).ValueDouble;

                cmd.Parameters.Add("@P_DaysToShift", SqlDbType.Int);
                cmd.Parameters["@P_DaysToShift"].Value = Parameters1C.First(x => x.Name.Contains("КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров")).ValueDouble;

                cmd.CommandTimeout = (int)(executionLimit / 1000);

                string dateTimeNowOptimizeString = dateTimeNowOptimizeString = DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss");

                var pickupWorkingHoursJoinType = _configuration.GetValue<string>("pickupWorkingHoursJoinType");

                cmd.CommandText = queryTextBegin + string.Format(query, string.Join(",", pickupParameters),
                    dateTimeNowOptimizeString,
                    DateMove.Date.ToString("yyyy-MM-ddTHH:mm:ss"),
                    DateMove.Date.AddDays(Parameters1C.First(x => x.Name.Contains("rsp_КоличествоДнейЗаполненияГрафика")).ValueDouble - 1).ToString("yyyy-MM-ddTHH:mm:ss"),
                    Parameters1C.First(x => x.Name.Contains("КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа")).ValueDouble,
                    Parameters1C.First(x => x.Name.Contains("ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа")).ValueDouble,
                    pickupWorkingHoursJoinType,
                    "",
                    5);

                //execute the SQLCommand
                result = await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                result = -1;

                var logElement = new ElasticLogElement
                {
                    ErrorDescription = ex.Message,
                    Status = LogStatus.Error,
                    DatabaseConnection = databaseInfo.ConnectionWithoutCredentials
                };

                _logger.LogElastic(logElement);
            }
            watch.Stop();

            if (watch.ElapsedMilliseconds > executionLimit)
            {
                result = -1;
                var logElement = new ElasticLogElement
                {
                    ErrorDescription = "Availability false because of ElapsedMilliseconds=" + watch.ElapsedMilliseconds,
                    Status = LogStatus.Error,
                    DatabaseConnection = databaseInfo.ConnectionWithoutCredentials
                };

                _logger.LogElastic(logElement);
            }

            return result >= 0;
        }

        public async Task<ElasticDatabaseStats?> GetElasticLogsInformationAsync(string connectionWithOutCredentials, CancellationToken token)
        {
            var elasticHost = _configuration["ElasticConfiguration:Host"];
            var elasticPort = _configuration.GetValue<int>("ElasticConfiguration:Port");
            var ApiKey = _configuration["ElasticConfiguration:ApiKey"];
            var indexPath = _configuration["ElasticConfiguration:IndexName"];
            
            var analyzeInterval = "now-1m";
            var clearCacheCriterias = _configuration.GetSection("ClearCacheCriterias").Get<List<ClearCacheCriteria>>();

            Response? elasticResponse = null;

            var httpClient = _httpClientFactory.CreateClient("elastic");

            UriBuilder elasticUri = new("https", elasticHost, elasticPort, indexPath + "/_search");

            IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
            var serverIP = ipHostInfo.AddressList.Where(s => s.AddressFamily == AddressFamily.InterNetwork).First().ToString();
            HttpRequestMessage requestMessage = new(HttpMethod.Get, elasticUri.Uri);
            requestMessage.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("ApiKey", ApiKey);

            var searchrequest = new Request
            {
                Size = 0
            };

            FilterElement element = new()
            {
                Range = new()
                {
                    { "@timestamp", new { gt = analyzeInterval } }
                }
            };
            searchrequest.Query.Bool.Filter.Add(element);

            //element = new()
            //{
            //    Term = new()
            //    {
            //        { "server_host", new { value = serverIP } }
            //    }
            //};
            //searchrequest.Query.Bool.Filter.Add(element);

            element = new()
            {
                Term = new()
                {
                    { "Message.DatabaseConnection", new { value = connectionWithOutCredentials } }
                }
            };
            searchrequest.Query.Bool.Filter.Add(element);

            AggregationClass rootAgg = new()
            {
                Terms = new()
                {
                    Field = "Message.DatabaseConnection",
                    Size = 5
                },

                Aggregations = new()
            };

            AggregationClass timePercentile = new()
            {
                Percentiles = new()
                {
                    Field = "Message.TimeFullExecution",
                    Percents = new double[] { 95, 99, 99.5 }
                }
            };

            rootAgg.Aggregations.Add("time_percentile", timePercentile);

            AggregationClass loadBal = new()
            {
                Avg = new()
                {
                    Field = "Message.TimeDatabaseConnection"
                }
            };
            rootAgg.Aggregations.Add("load_bal", loadBal);

            AggregationClass average = new()
            {
                Avg = new()
                {
                    Field = "Message.TimeFullExecution"
                }
            };
            rootAgg.Aggregations.Add("week_avg", average);

            searchrequest.Aggregations.Add("load_time_outlier", rootAgg);

            var content = JsonSerializer.Serialize(searchrequest);
            requestMessage.Content = new StringContent(content, Encoding.UTF8, "application/json");

            var result = "";

            try
            {
                var response = await httpClient.SendAsync(requestMessage, token);

                if (response.IsSuccessStatusCode)
                {
                    result = await response.Content.ReadAsStringAsync(token);
                }
                else
                {
                    var errodData = await response.Content.ReadAsStringAsync(token);
                    var logElement = new ElasticLogElement
                    {
                        ErrorDescription = errodData,
                        Status = LogStatus.Error,
                        DatabaseConnection = elasticUri.ToString()
                    };

                    logElement.AdditionalData.Add("requestContent", content);
                    
                    _logger.LogElastic(logElement);
                }
            }
            catch (Exception ex)
            {
                var logElement = new ElasticLogElement
                {
                    ErrorDescription = ex.Message,
                    Status = LogStatus.Error,
                    DatabaseConnection = elasticUri.ToString()
                };

                _logger.LogElastic(logElement);
            }

            try
            {
                if (!String.IsNullOrEmpty(result))
                {
                    elasticResponse = JsonSerializer.Deserialize<Response>(result);
                }
            }
            catch (Exception ex)
            {
                var logElement = new ElasticLogElement
                {
                    ErrorDescription = ex.Message,
                    Status = LogStatus.Error,
                    DatabaseConnection = elasticUri.ToString()
                };
                logElement.AdditionalData.Add("responseContent", result);
                
                _logger.LogElastic(logElement);
            }

            if (elasticResponse is null)
            {
                return null;
            }

            if (!elasticResponse.Aggregations.TryGetValue("load_time_outlier", out Aggregations? aggregation))
            {
                _logger.LogElastic("Aggregation load_time_outlier not found in Elastic response");
                return null;
            }

            var responseBucket = aggregation.Buckets.Find(x => x.Key == connectionWithOutCredentials);
            if (responseBucket == null)
            {
                if (_productionEnv)
                {
                    //_logger.LogElastic($"Database with key {connectionWithOutCredentials} has no logs in Elastic!");
                }
                return null;
            }

            var databaseStats = new ElasticDatabaseStats
            {
                RecordCount = responseBucket.DocCount,
                LoadBalanceTime = responseBucket.LoadBalance.Value,
                AverageTime = responseBucket.WeekAvg.Value,
                Percentile95Time = responseBucket.TimePercentile.Values.GetValueOrDefault("95.0")
            };

            if (databaseStats.AverageTime == default || databaseStats.Percentile95Time == default)
            {
                if (_productionEnv)
                {
                    _logger.LogElastic($"db stats for {connectionWithOutCredentials} is empty");
                }

                return null;
            }

            return databaseStats;
        }

        private static string TextFillGoodsTable(AvailableDateQuery data, SqlCommand cmdGoodsTable, bool optimizeRowsCount, List<string> PickupsList)
        {

            var resultString = CommonQueries.TableGoodsRawCreate;

            var insertRowsLimit = 900;

            var parameters = new List<string>();

            data.Codes = data.Codes.Where(x =>
            {
                if (data.CheckQuantity)
                {
                    return x.Quantity > 0;
                }
                else return true;
            }).ToList();

            if (data.CheckQuantity)
            {
                data.CheckQuantity = data.Codes.Any(x => x.Quantity != 1); //we can use basic query if all quantity is 1
            }

            var maxCodes = data.Codes.Count;

            foreach (var codesElem in data.Codes)
            {
                foreach (var item in codesElem.PickupPoints)
                {
                    if (!PickupsList.Contains(item))
                    {
                        PickupsList.Add(item);
                    }
                }
            }

            int maxPickups = PickupsList.Count;

            if (data.Codes.Count > 2) maxCodes = 10;
            if (data.Codes.Count > 10) maxCodes = 30;
            if (data.Codes.Count > 30) maxCodes = 60;
            if (data.Codes.Count > 60) maxCodes = 100;
            if (data.Codes.Count > maxCodes || !optimizeRowsCount) maxCodes = data.Codes.Count;


            for (int codesCounter = 0; codesCounter < maxCodes; codesCounter++)
            {

                CodeItemQuery codesElem;
                if (codesCounter < data.Codes.Count)
                {
                    codesElem = data.Codes[codesCounter];
                }
                else
                {
                    codesElem = data.Codes[^1];
                }

                var parameterString = string.Format("(@Article{0}, @Code{0}, NULL, @Quantity{0})", codesCounter);

                cmdGoodsTable.Parameters.Add(string.Format("@Article{0}", codesCounter), SqlDbType.NVarChar, 11);
                cmdGoodsTable.Parameters[string.Format("@Article{0}", codesCounter)].Value = codesElem.Article;

                cmdGoodsTable.Parameters.Add(string.Format("@Code{0}", codesCounter), SqlDbType.NVarChar, 11);
                if (String.IsNullOrEmpty(codesElem.Code))
                    cmdGoodsTable.Parameters[string.Format("@Code{0}", codesCounter)].Value = DBNull.Value;
                else
                    cmdGoodsTable.Parameters[string.Format("@Code{0}", codesCounter)].Value = codesElem.Code;

                //cmdGoodsTable.Parameters.AddWithValue(string.Format("@Quantity{0}", codesCounter), codesElem.quantity);
                cmdGoodsTable.Parameters.Add(string.Format("@Quantity{0}", codesCounter), SqlDbType.Int, 10);
                cmdGoodsTable.Parameters[string.Format("@Quantity{0}", codesCounter)].Value = codesElem.Quantity;

                parameters.Add(parameterString);

                if (parameters.Count == insertRowsLimit)
                {
                    resultString += string.Format(CommonQueries.TableGoodsRawInsert, string.Join(", ", parameters));

                    parameters.Clear();
                }

                if (maxPickups > 0)
                {
                    var PickupParameter = string.Join(",", codesElem.PickupPoints);

                    cmdGoodsTable.Parameters.Add(string.Format("@PickupPoint{0}", codesCounter), SqlDbType.NVarChar, 45);
                    cmdGoodsTable.Parameters[string.Format("@PickupPoint{0}", codesCounter)].Value = PickupParameter;

                    var parameterStringPickup = string.Format("(@Article{0}, @Code{0}, @PickupPoint{0}, @Quantity{0})", codesCounter);
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
    }
}
