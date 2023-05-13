using DateTimeService.Api;
using DateTimeService.Application.Database.DatabaseManagement;
using DateTimeService.Application.Logging;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace DateTimeService.Application.Database
{
    public interface IDbConnectionFactory
    {
        Task<DbConnection> CreateConnectionAsync (CancellationToken token = default);
    }

    public class SqlConnectionFactory : IDbConnectionFactory
    {
        private readonly IReadableDatabase _databaseService;
        private readonly IConfiguration _configuration;
        private readonly ILogger<SqlConnectionFactory> _logger;

        private readonly bool _checkConnection;

        public SqlConnectionFactory(IReadableDatabase databaseService, IConfiguration configuration, ILogger<SqlConnectionFactory> logger)
        {
            _databaseService = databaseService;
            _configuration = configuration;
            _logger = logger;

            _checkConnection = !_configuration.GetValue<bool>("DisableConnectionCheck");       
        }

        public async Task<DbConnection> CreateConnectionAsync(CancellationToken token = default)
        {
            var result = new DbConnection();

            var watch = Stopwatch.StartNew();

            var connectionParameters = _databaseService.GetAllDatabases();

            var timeMS = DateTime.Now.Millisecond % 100;

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
                        catch (Exception ex)
                        {
                            var logElement = new ElasticLogElement
                            {
                                Status = LogStatus.Error,
                                ErrorDescription = ex.Message,
                                DatabaseConnection = connParameter.ConnectionWithoutCredentials
                            };
                            _logger.LogElastic(logElement);
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

        private async Task<SqlConnection> GetConnectionByDatabaseInfo(DatabaseInfo databaseInfo, CancellationToken token = default)
        {
            string connectionString = databaseInfo.Connection;

            if (_configuration.GetValue<bool>("UseConnectionPool"))
            {
                var builder = new SqlConnectionStringBuilder(databaseInfo.Connection)
                {
                    Pooling = true,
                    MaxPoolSize = _configuration.GetValue<int>("MaxConnectionPoolSize"),
                    MinPoolSize = 10,
                    ConnectTimeout = 5
                };

                connectionString = builder.ConnectionString;
            }

            SqlConnection connection = new(connectionString);

            await connection.OpenAsync(token);

            if (_checkConnection) {

                var queryStringCheck = databaseInfo.DatabaseType switch
                {
                    DatabaseType.Main => DbCheckQueries.Main,
                    DatabaseType.ReplicaFull => DbCheckQueries.ReplicaFull,
                    DatabaseType.ReplicaTables => DbCheckQueries.ReplicaTables,
                    _ => ""
                };

                SqlCommand cmd = new(queryStringCheck, connection)
                {
                    CommandTimeout = 1
                };

                _ = await cmd.ExecuteScalarAsync(token);
            }
            
            return connection;
        }
    }
}
