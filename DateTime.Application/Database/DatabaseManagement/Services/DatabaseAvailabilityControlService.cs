using DateTimeService.Api;
using DateTimeService.Application.Logging;
using DateTimeService.Application.Queries;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DateTimeService.Application.Database.DatabaseManagement
{
    public class DatabaseAvailabilityControlService : IDatabaseAvailabilityControl
    {
        private readonly IDatabaseCheck _databaseCheckService;
        private readonly IReadableDatabase _readableDatabaseService;
        private readonly ILogger<DatabaseAvailabilityControlService> _logger;
        private readonly IConfiguration _configuration;

        private readonly List<ClearCacheCriteria> clearCacheCriterias;
        private readonly string analyzeInterval = "now-1m";
        private readonly int errorsCountToSendClearCache;
        private readonly int delayBetweenClearCache;

        public DatabaseAvailabilityControlService(IDatabaseCheck databaseCheckService,
                                                  IReadableDatabase readableDatabaseService,
                                                  ILogger<DatabaseAvailabilityControlService> logger,
                                                  IConfiguration configuration)
        {
            _databaseCheckService = databaseCheckService;
            _readableDatabaseService = readableDatabaseService;
            _logger = logger;
            _configuration = configuration;

            clearCacheCriterias = _configuration.GetSection("ClearCacheCriterias").Get<List<ClearCacheCriteria>>();
            errorsCountToSendClearCache = _configuration.GetValue<int>("ErrorsCountToSendClearCache");
            if (errorsCountToSendClearCache == 0)
            {
                errorsCountToSendClearCache = 1;
            }

            delayBetweenClearCache = _configuration.GetValue<int>("DelayBetweenClearCache");
            if (delayBetweenClearCache == 0)
            {
                delayBetweenClearCache = 180;
            }
        }

        public async Task CheckAndUpdateDatabasesStatus(CancellationToken token)
        {
            var dbList = _readableDatabaseService.GetAllDatabases();

            foreach (var databaseInfo in dbList)
            {
                if (token.IsCancellationRequested)
                {
                    break;
                }

                if (databaseInfo.AvailableToUse)
                {
                    int customAggsFailCount = databaseInfo.CustomAggsFailCount;
                    int timeCriteriaFailCount = databaseInfo.TimeCriteriaFailCount;

                    if (databaseInfo.DatabaseType == DatabaseType.ReplicaTables)
                    {
                        await CheckAndUpdateAggregations(databaseInfo, customAggsFailCount, token);
                    }

                    await CheckAndUpdatePerfomance(databaseInfo, databaseInfo.LastFreeProcCacheCommand, timeCriteriaFailCount, databaseInfo.DatabaseType != DatabaseType.Main, token);
                }
                else
                {
                    if (databaseInfo.LastCheckAvailability == default
                        || DateTimeOffset.Now - databaseInfo.LastCheckAvailability > TimeSpan.FromSeconds(60))
                    {
                        var availabilityResult = await _databaseCheckService.CheckAvailabilityAsync(databaseInfo, token, 5000);
                        if (availabilityResult)
                        {
                            _readableDatabaseService.EnableDatabase(databaseInfo.Connection);
                        }
                        _readableDatabaseService.UpdateDatabaseLastAvailabilityCheckTime(databaseInfo.Connection);
                    }
                }
            }
        }

        private async Task CheckAndUpdatePerfomance(DatabaseInfo databaseInfo, DateTimeOffset lastFreeProcCacheCommand, int timeCriteriaFailCount, bool clearCacheAllowed, CancellationToken token)
        {
            if (databaseInfo.Priority == 0)
            {
                return;
            }

            var stats = await _databaseCheckService.GetElasticLogsInformationAsync(databaseInfo.ConnectionWithoutCredentials, token);
            if (stats != null)
            {
                var dbAction = AnalyzeElasticResponse(stats);

                switch (dbAction)
                {
                    case DatabaseActions.Error:
                        break;
                    case DatabaseActions.None:
                        _readableDatabaseService.SetPriorityCoefficient(databaseInfo.Connection);
                        break;
                    case DatabaseActions.SendClearCache:
                        if (clearCacheAllowed)
                        {
                            await ProcessSendClearCacheAction(databaseInfo, lastFreeProcCacheCommand, timeCriteriaFailCount, token);
                        }
                        break;
                    case DatabaseActions.DisableZeroExecutionTime:
                        _readableDatabaseService.DisableDatabase(databaseInfo.Connection, "zero execution time");
                        break;
                    case DatabaseActions.DisableBigExecutionTime:
                        _readableDatabaseService.DisableDatabase(databaseInfo.Connection, "big execution time");
                        break;
                    case DatabaseActions.DisableBigLoadBalanceTime:
                        _readableDatabaseService.DisableDatabase(databaseInfo.Connection, "big load balance time");
                        break;
                    default:
                        break;
                }
                _readableDatabaseService.UpdateDatabaseLastPerfomanceCheckTime(databaseInfo.Connection);
            }
            else
            {
                var logElement = new ElasticLogElement
                {
                    Status = LogStatus.Info,
                    ErrorDescription = "Check and update perfomance - no elastic logs",
                    DatabaseConnection = databaseInfo.ConnectionWithoutCredentials
                };
                _logger.LogElastic(logElement);
            }
        }

        private async Task ProcessSendClearCacheAction(DatabaseInfo databaseInfo, DateTimeOffset lastFreeProcCacheCommand, int timeCriteriaFailCount, CancellationToken token)
        {
            if (lastFreeProcCacheCommand == default
                                        || DateTimeOffset.Now - lastFreeProcCacheCommand > TimeSpan.FromSeconds(delayBetweenClearCache))
            {
                if (timeCriteriaFailCount > errorsCountToSendClearCache)
                {
                    await SendClearCacheScript(databaseInfo, token);
                    _readableDatabaseService.UpdateDatabasePerfomanceFailCount(databaseInfo.Connection, timeCriteriaFailCount, 0);
                    _readableDatabaseService.UpdateDatabaseLastClearCacheTime(databaseInfo.Connection);
                }
                else
                {
                    _readableDatabaseService.UpdateDatabasePerfomanceFailCount(databaseInfo.Connection, timeCriteriaFailCount, timeCriteriaFailCount + 1);
                }
            }
            else
            {
                _readableDatabaseService.UpdateDatabasePerfomanceFailCount(databaseInfo.Connection, timeCriteriaFailCount, 0);
            }
        }

        private async Task CheckAndUpdateAggregations(DatabaseInfo databaseInfo, int customAggsFailCount, CancellationToken token)
        {
            var aggsResult = await _databaseCheckService.CheckAggregationsAsync(databaseInfo, token);

            if (!aggsResult)
            {

                if (customAggsFailCount > 6)//TODO make config
                {
                    _readableDatabaseService.UpdateDatabaseAggregationsFailCount(databaseInfo.Connection, customAggsFailCount, 0);
                    _readableDatabaseService.DisableDatabaseAggs(databaseInfo.Connection);
                }
                else
                {
                    _readableDatabaseService.UpdateDatabaseAggregationsFailCount(databaseInfo.Connection, customAggsFailCount, customAggsFailCount + 1);
                }
            }
            else
            {
                _readableDatabaseService.UpdateDatabaseAggregationsFailCount(databaseInfo.Connection, customAggsFailCount, 0);
                _readableDatabaseService.EnableDatabaseAggs(databaseInfo.Connection);
            }

            _readableDatabaseService.UpdateDatabaseLastAggregationCheckTime(databaseInfo.Connection);
        }

        private async Task<bool> SendClearCacheScript(DatabaseInfo databaseInfo, CancellationToken token)
        {
            bool result = false;

            if (databaseInfo.DatabaseType == DatabaseType.Main
                || databaseInfo.DatabaseType == DatabaseType.ReplicaFull)
            {
                return result;
            }

            try
            {
                using SqlConnection conn = new(databaseInfo.Connection);

                conn.Open();

                var clearCacheScript = DbCheckQueries.ClearCacheScriptDefault;

                var clearCacheScriptFromConfig = _configuration.GetValue<string>("ClearCacheScript");

                if (!string.IsNullOrEmpty(clearCacheScriptFromConfig))
                {
                    clearCacheScript = clearCacheScriptFromConfig;
                }

                SqlCommand cmd = new(clearCacheScript, conn)
                {
                    CommandTimeout = 1
                };

                var clearCacheResult = await cmd.ExecuteNonQueryAsync(token);
                conn.Close();

                //database.LastFreeProcCacheCommand = DateTimeOffset.Now;

                var logElement = new ElasticLogElement
                {
                    ErrorDescription = "Send dbcc freeproccache",
                    Status = LogStatus.Ok,
                    DatabaseConnection = databaseInfo.ConnectionWithoutCredentials
                };

                _logger.LogElastic(logElement);

                result = true;
            }
            catch (Exception ex)
            {
                var logElement = new ElasticLogElement
                {
                    ErrorDescription = ex.Message,
                    Status = LogStatus.Error,
                    DatabaseConnection = databaseInfo.ConnectionWithoutCredentials
                };

                _logger.LogElastic(logElement);

                result = false;
            }

            return result;
        }

        private DatabaseActions AnalyzeElasticResponse(ElasticDatabaseStats elasticStats)
        {
            var minutesInBucket = int.Parse(analyzeInterval.Replace("now-", "").Replace("m", ""));
            var recordsByMinute = elasticStats.RecordCount / minutesInBucket;
            var criteria = clearCacheCriterias.FirstOrDefault(s => recordsByMinute >= s.RecordCountBegin && recordsByMinute <= s.RecordCountEnd && s.CriteriaType == "RecordCount");
            var criteriaMaxTime = clearCacheCriterias.FirstOrDefault(s => s.CriteriaType == "MaximumResponseTime");
            var percentile95rate = elasticStats.Percentile95Time;

            if (criteria == default || percentile95rate == default)
            {
                return DatabaseActions.Error; //TODO log error
            }

            if (recordsByMinute >= 100 && elasticStats.AverageTime == 0)
            {
                return DatabaseActions.DisableZeroExecutionTime;
            }

            if (recordsByMinute >= 100 && elasticStats.LoadBalanceTime > criteriaMaxTime.LoadBalance)
            {
                return DatabaseActions.DisableBigLoadBalanceTime;
            }

            if (recordsByMinute >= 100 && elasticStats.AverageTime > criteriaMaxTime.Percentile_95)
            {
                return DatabaseActions.DisableBigExecutionTime;
            }

            if (percentile95rate > criteria.Percentile_95)
            {
                return DatabaseActions.SendClearCache;
            }

            return DatabaseActions.None;
        }
    }
}
