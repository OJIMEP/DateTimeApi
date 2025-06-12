namespace DateTimeService.Application.Database.DatabaseManagement
{
    public interface IReadableDatabase
    {
        public List<DatabaseInfo> GetAllDatabases();

        public IEnumerable<DatabaseInfo> AvailableDatabases();
        public Task<bool> SynchronizeDatabasesListFromFile(List<DatabaseInfo> newDatabases);
        public Task<bool> AddDatabase(DatabaseInfo newDatabaseEntity);
        public bool DeleteDatabase(string connection);
        public bool UpdateDatabaseFromFile(DatabaseInfo newDatabaseEntity);
        public bool DisableDatabase(string connection, string reason = "");
        public bool EnableDatabase(string connection, string reason = "");
        public bool DisableDatabaseAggs(string connection, string reason = "");
        public bool EnableDatabaseAggs(string connection, string reason = "");
        public bool UpdateDatabasePerfomanceFailCount(string connection, int oldFailCount, int newFailCount);
        public bool UpdateDatabaseAggregationsFailCount(string connection, int oldFailCount, int newFailCount);
        public bool UpdateDatabaseLastClearCacheTime(string connection);
        public bool UpdateDatabaseLastAvailabilityCheckTime(string connection);
        public bool UpdateDatabaseLastAggregationCheckTime(string connection);
        public bool UpdateDatabaseLastPerfomanceCheckTime(string connection);
        public bool SetPriorityCoefficient(string connection, double priorityCoefficient = 0);
    }
}
