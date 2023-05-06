using DateTime.Application.Database.DatabaseManagement;
using Microsoft.Data.SqlClient;
using System.Diagnostics;

namespace DateTime.Application.Database
{
    public interface IDbConnectionFactory
    {
        Task<DbConnection> CreateConnectionAsync (CancellationToken token = default);
    }

    public class SqlConnectionFactory : IDbConnectionFactory
    {
        private readonly IReadableDatabase _databaseService;

        public SqlConnectionFactory(IReadableDatabase databaseService)
        {
            _databaseService = databaseService;
        }

        public async Task<DbConnection> CreateConnectionAsync(CancellationToken token = default)
        {
            var result = new DbConnection();

            var watch = Stopwatch.StartNew();

            var connectionParameters = _databaseService.GetAllDatabases();

            var timeMS = System.DateTime.Now.Millisecond % 100;

            List<string> failedConnections = new();

            bool firstAvailable = false;

            var resultString = "";

            while (true)
            {
                int percentCounter = 0;
                foreach (var connParameter in connectionParameters)
                {
                    if (firstAvailable && failedConnections.Contains(connParameter.Connection))
                        continue;

                    if (!connParameter.AvailableToUse)
                        continue;

                    percentCounter += connParameter.Priority;
                    if ((timeMS <= percentCounter && connParameter.Priority != 0) || firstAvailable)
                    {
                        try
                        {
                            var connection = await GetConnectionByDatabaseInfo(connParameter, token);

                            result.Connection = connection;
                            resultString = connParameter.Connection;
                            result.DatabaseType = connParameter.DatabaseType;
                            result.UseAggregations = connParameter.CustomAggregationsAvailable;
                            result.ConnectionWithoutCredentials = connParameter.ConnectionWithoutCredentials;
                            break;
                        }
                        catch (Exception)
                        {
                            failedConnections.Add(connParameter.Connection);
                        }
                    }
                }
                if (resultString.Length > 0 || firstAvailable)
                    break;
                else
                    firstAvailable = true;
            }

            watch.Stop();
            result.ConnectTimeInMilliseconds = watch.ElapsedMilliseconds;

            return result;
        }

        private static async Task<SqlConnection> GetConnectionByDatabaseInfo(DatabaseInfo databaseInfo, CancellationToken token = default)
        {
            var queryStringCheck = databaseInfo.DatabaseType switch
            {
                DatabaseType.Main => DbCheckQueries.Main,
                DatabaseType.ReplicaFull => DbCheckQueries.ReplicaFull,
                DatabaseType.ReplicaTables => DbCheckQueries.ReplicaTables,
                _ => ""
            };

            SqlConnection connection = new(databaseInfo.Connection);
            await connection.OpenAsync(token);

            SqlCommand cmd = new(queryStringCheck, connection)
            {
                CommandTimeout = 1
            };

            _ = await cmd.ExecuteScalarAsync(token);

            return connection;
        }
    }
}
