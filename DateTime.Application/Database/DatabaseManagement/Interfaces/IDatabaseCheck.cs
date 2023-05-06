namespace DateTime.Application.Database.DatabaseManagement
{
    public interface IDatabaseCheck
    {
        public Task<ElasticDatabaseStats?> GetElasticLogsInformationAsync(string connectionWithoutCredentials, CancellationToken token);

        public Task<bool> CheckAvailabilityAsync(DatabaseInfo databaseInfo, CancellationToken token, long executionLimit = 5000);

        public Task<bool> CheckAggregationsAsync(DatabaseInfo databaseInfo, CancellationToken token);
    }
}
