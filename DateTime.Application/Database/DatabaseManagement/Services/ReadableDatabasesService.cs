using DateTimeService.Api;
using DateTimeService.Application.Logging;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace DateTimeService.Application.Database.DatabaseManagement
{
    public class ReadableDatabasesService: IReadableDatabase
    {
        private readonly ILogger<ReadableDatabasesService> _logger;
        private readonly IDatabaseCheck _databaseCheckService;

        private readonly ConcurrentDictionary<string, DatabaseInfo> dbDictionary = new();

        public ReadableDatabasesService(ILogger<ReadableDatabasesService> logger, IDatabaseCheck databaseCheckService)
        {
            _logger = logger;
            _databaseCheckService = databaseCheckService;
        }

        public async Task<bool> AddDatabase(DatabaseInfo database)
        {
            bool addResult;
            try
            {
                database.AvailableToUse = await _databaseCheckService.CheckAvailabilityAsync(database, CancellationToken.None);
                if (database.Type == "replica_tables")
                {
                    database.CustomAggregationsAvailable = await _databaseCheckService.CheckAggregationsAsync(database, CancellationToken.None);
                }

                addResult = dbDictionary.TryAdd(database.Connection, database);
                if (addResult)
                {
                    LogUpdatedChanges(database.ConnectionWithoutCredentials, "Added database", "");
                    if (database.CustomAggregationsAvailable)
                    {
                        LogUpdatedChanges(database.ConnectionWithoutCredentials, $"Database aggs enabled", "");
                    }
                }
                else
                {
                    throw new Exception("Database already exists");
                }
            }
            catch (Exception ex)
            {
                _logger.LogElastic("Adding database failed", ex);
                addResult = false;
            }

            return addResult;
        }

        public bool DeleteDatabase(string connection)
        {
            bool removeResult;

            try
            {
                removeResult = false;

                removeResult = dbDictionary.TryRemove(connection, out DatabaseInfo removedValue);
                if (removeResult)
                {
                    LogUpdatedChanges(removedValue.ConnectionWithoutCredentials, "Removed database", "");
                }
                else
                {
                    throw new Exception("Database not found");
                }
            }
            catch (Exception ex)
            {
                removeResult = false;
                _logger.LogElastic("Deleting database failed", ex);
            }

            return removeResult;
        }

        public bool DisableDatabase(string connection, string reason = "")
        {
            // Не отключаем базы, снижаем приоритет
            return true;

            //bool updateResult;
            //try
            //{
            //    var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
            //    if (getResult)
            //    {
            //        if (!currentDatabaseEntity.AvailableToUse)
            //        {
            //            return true;
            //        }

            //        var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
            //        changedEntity.AvailableToUse = false;

            //        updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

            //        if (updateResult)
            //        {
            //            LogUpdatedChanges(changedEntity.ConnectionWithoutCredentials, $"Database disabled due to {reason}", "");
            //        }
            //        else
            //            throw new Exception("Database update failed");
            //    }
            //    else
            //        throw new Exception("Database not found");

            //}
            //catch (Exception ex)
            //{
            //    _logger.LogElastic("Updating database failed", ex);
            //    updateResult = false;
            //}

            //return updateResult;
        }

        public bool DisableDatabaseAggs(string connection, string reason = "")
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {

                    if (!currentDatabaseEntity.CustomAggregationsAvailable)
                    {
                        return true;
                    }

                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.CustomAggregationsAvailable = false;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (updateResult)
                    {
                        LogUpdatedChanges(changedEntity.ConnectionWithoutCredentials, $"Database aggs disabled due to {reason}", "");
                    }
                    else
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public bool EnableDatabase(string connection, string reason = "")
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    if (currentDatabaseEntity.AvailableToUse)
                    {
                        return true;
                    }

                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.AvailableToUse = true;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (updateResult)
                    {
                        LogUpdatedChanges(changedEntity.ConnectionWithoutCredentials, $"Database enabled {reason}", "");
                    }
                    else
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public bool EnableDatabaseAggs(string connection, string reason = "")
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    if (currentDatabaseEntity.CustomAggregationsAvailable)
                    {
                        return true;
                    }

                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.CustomAggregationsAvailable = true;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (updateResult)
                    {
                        LogUpdatedChanges(changedEntity.ConnectionWithoutCredentials, $"Database aggs enabled {reason}", "");
                    }
                    else
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public bool SetPriorityCoefficient(string connection, double newValue = 0)
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    if (newValue == 0)
                    {
                        if (currentDatabaseEntity.PriorityCoefficient < 1)
                        {
                            newValue = Math.Min(currentDatabaseEntity.PriorityCoefficient + 0.1, 1);
                        }
                        else
                        {
                            return true;
                        }
                    }

                    if (currentDatabaseEntity.PriorityCoefficient == newValue)
                    {
                        return true;
                    }

                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.PriorityCoefficient = newValue;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (updateResult)
                    {
                        //LogUpdatedChanges(newDatabaseEntity.ConnectionWithoutCredentials, "Priority changed", $"New priority = {newDatabaseEntity.Priority}");   
                    }
                    else
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public List<DatabaseInfo> GetAllDatabases()
        {
            var result = dbDictionary.Values.ToList();

            return result;
        }

        public IEnumerable<DatabaseInfo> AvailableDatabases()
        {
            return dbDictionary.Values.ToList().Where(x => x.AvailableToUse);
        }

        public async Task<bool> SynchronizeDatabasesListFromFile(List<DatabaseInfo> newDatabases)
        {
            bool result = true;

            var databasesToDelete = dbDictionary.Keys.Where(pldDbConnString => !newDatabases.Any(newDb => newDb.Connection == pldDbConnString)).ToList();

            foreach (var database in databasesToDelete)
            {
                result = result && DeleteDatabase(database);
            }

            foreach (var newDb in newDatabases)
            {
                if (dbDictionary.ContainsKey(newDb.Connection))
                {
                    result = result && UpdateDatabaseFromFile(newDb);
                }
                else
                {
                    result = result && await AddDatabase(newDb);
                }
            }

            return result;
        }

        public bool UpdateDatabaseAggregationsFailCount(string connection, int oldFailCount, int newFailCount)
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    if (currentDatabaseEntity.CustomAggsFailCount != oldFailCount)
                    {
                        throw new Exception("Database CustomAggsFailCount old is different from current");
                    }

                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.CustomAggsFailCount = newFailCount;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (updateResult)
                    {
                        //LogUpdatedChanges(newDatabaseEntity.ConnectionWithoutCredentials, "Priority changed", $"New priority = {newDatabaseEntity.Priority}");   
                    }
                    else
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public bool UpdateDatabaseFromFile(DatabaseInfo newDatabaseEntity)
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(newDatabaseEntity.Connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    bool changedPriority = newDatabaseEntity.Priority != currentDatabaseEntity.Priority;
                    bool changedType = newDatabaseEntity.Type != currentDatabaseEntity.Type;
                    bool changedEndpoints = newDatabaseEntity.Endpoints != currentDatabaseEntity.Endpoints;

                    if (changedPriority || changedType || changedEndpoints)
                    {
                        var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                        changedEntity.Priority = newDatabaseEntity.Priority;
                        changedEntity.Type = newDatabaseEntity.Type;
                        changedEntity.Endpoints = newDatabaseEntity.Endpoints;
                        if (changedEntity.Endpoints is not null)
                        {
                            changedEntity.EndpointsList = changedEntity.Endpoints.Split(",")
                                .Select(s => (ServiceEndpoint)Enum.Parse(typeof(ServiceEndpoint), s.Trim(), ignoreCase: true))
                                .ToList();
                        }
                        else
                        {
                            changedEntity.EndpointsList = new List<ServiceEndpoint> { ServiceEndpoint.All };
                        }
                        updateResult = dbDictionary.TryUpdate(newDatabaseEntity.Connection, changedEntity, currentDatabaseEntity);

                        if (updateResult)
                        {
                            if (changedPriority)
                            {
                                LogUpdatedChanges(newDatabaseEntity.ConnectionWithoutCredentials, "Priority changed", $"New priority = {newDatabaseEntity.Priority}");
                            }
                            if (changedType)
                            {
                                LogUpdatedChanges(newDatabaseEntity.ConnectionWithoutCredentials, "Priority changed", $"New priority = {newDatabaseEntity.Priority}");
                            }
                            if (changedEndpoints)
                            {
                                LogUpdatedChanges(newDatabaseEntity.ConnectionWithoutCredentials, "Endpoints changed", $"New endpoints = {newDatabaseEntity.Endpoints}");
                            }
                        }
                        else
                            throw new Exception("Database update failed");
                    }
                    else
                        updateResult = true;

                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;

        }

        public bool UpdateDatabaseLastAggregationCheckTime(string connection)
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.LastCheckAggregations = DateTimeOffset.Now;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (!updateResult)
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public bool UpdateDatabaseLastAvailabilityCheckTime(string connection)
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.LastCheckAvailability = DateTimeOffset.Now;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (!updateResult)
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public bool UpdateDatabaseLastClearCacheTime(string connection)
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.LastFreeProcCacheCommand = DateTimeOffset.Now;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (!updateResult)
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public bool UpdateDatabaseLastPerfomanceCheckTime(string connection)
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.LastCheckPerfomance = DateTimeOffset.Now;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (!updateResult)
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public bool UpdateDatabasePerfomanceFailCount(string connection, int oldFailCount, int newFailCount)
        {

            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    if (currentDatabaseEntity.TimeCriteriaFailCount != oldFailCount)
                    {
                        throw new Exception("Database PerfomanceFailCount old is diffrent from current");
                    }

                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.TimeCriteriaFailCount = newFailCount;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (updateResult)
                    {
                        //LogUpdatedChanges(newDatabaseEntity.ConnectionWithoutCredentials, "Priority changed", $"New priority = {newDatabaseEntity.Priority}");   
                    }
                    else
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public bool UpdateLastRecompileAvailableDate(string connection)
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.LastRecompileAvailableDate = DateTimeOffset.Now;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (!updateResult)
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public bool UpdateLastRecompileIntervalList(string connection)
        {
            bool updateResult;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    var changedEntity = (DatabaseInfo)currentDatabaseEntity.Clone();
                    changedEntity.LastRecompileIntervalList = DateTimeOffset.Now;

                    updateResult = dbDictionary.TryUpdate(connection, changedEntity, currentDatabaseEntity);

                    if (!updateResult)
                        throw new Exception("Database update failed");
                }
                else
                    throw new Exception("Database not found");

            }
            catch (Exception ex)
            {
                _logger.LogElastic("Updating database failed", ex);
                updateResult = false;
            }

            return updateResult;
        }

        public DateTimeOffset GetLastRecompileAvailableDate(string connection)
        {
            DateTimeOffset result = DateTimeOffset.Now;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    return currentDatabaseEntity.LastRecompileAvailableDate;
                }
            }
            catch (Exception)
            {
            }

            return result;
        }

        public DateTimeOffset GetLastRecompileIntervalList(string connection)
        {
            DateTimeOffset result = DateTimeOffset.Now;
            try
            {
                var getResult = dbDictionary.TryGetValue(connection, out DatabaseInfo currentDatabaseEntity);
                if (getResult)
                {
                    return currentDatabaseEntity.LastRecompileIntervalList;
                }
            }
            catch (Exception)
            {
            }

            return result;
        }

        private void LogUpdatedChanges(string connectionName, string description, string updateDesc, LogStatus status = LogStatus.Ok)
        {
            var logElement = new ElasticLogElement
            {
                ErrorDescription = description,
                Status = status,
                DatabaseConnection = connectionName
            };

            logElement.AdditionalData.Add("UpdateCause", updateDesc);
            
            _logger.LogElastic(logElement);
        }
    }
}
