using DateTimeService.Api;
using DateTimeService.Application.Database.DatabaseManagement;
using DateTimeService.Application.Logging;
using DateTimeService.Application.Queries;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Weighted_Randomizer;

namespace DateTimeService.Application.Database
{
    public interface IDbConnectionFactory
    {
        Task<DbConnection> CreateConnectionAsync (ServiceEndpoint endpoint = ServiceEndpoint.All, CancellationToken token = default);

        Task<DbConnection> GetDbConnection(ServiceEndpoint endpoint = ServiceEndpoint.All, CancellationToken token = default);
    }

    public class SqlConnectionFactory : IDbConnectionFactory
    {
        private readonly IReadableDatabase _databaseService;
        private readonly IConfiguration _configuration;
        private readonly ILogger<SqlConnectionFactory> _logger;
        private readonly IHttpContextAccessor _contextAccessor;

        private readonly bool _checkConnection;
        private readonly bool _useConnectionByEndpoints;

        public SqlConnectionFactory(IReadableDatabase databaseService, IConfiguration configuration, ILogger<SqlConnectionFactory> logger, IHttpContextAccessor contextAccessor)
        {
            _databaseService = databaseService;
            _configuration = configuration;
            _logger = logger;

            _checkConnection = !_configuration.GetValue<bool>("DisableConnectionCheck");
            _useConnectionByEndpoints = _configuration.GetValue<bool>("UseConnectionByEndpoints");
            _contextAccessor = contextAccessor;
        }

        public async Task<DbConnection> GetDbConnection(ServiceEndpoint endpoint = ServiceEndpoint.All, CancellationToken token = default)
        {
            DbConnection dbConnection;

            try
            {
                dbConnection = await CreateConnectionAsync(endpoint, token);

                lock (_contextAccessor.HttpContext.Items)
                {
                    _contextAccessor.HttpContext.Items["ConnectionString"] = dbConnection.ConnectionString;
                    _contextAccessor.HttpContext.Items["DatabaseConnection"] = dbConnection.ConnectionWithoutCredentials;
                    _contextAccessor.HttpContext.Items["TimeDatabaseConnection"] = dbConnection.ConnectTimeInMilliseconds;
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

        public async Task<DbConnection> CreateConnectionAsync(ServiceEndpoint endpoint = ServiceEndpoint.All, CancellationToken token = default)
        {
            var result = new DbConnection();

            var watch = Stopwatch.StartNew();

            if (_useConnectionByEndpoints)
            {
                result = await CreateConnectionByEndpoint(endpoint, token);
            }
            // TODO - Delete
            else
            {
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
                            var connection = await GetConnectionByDatabaseInfo(connParameter, token);

                            if (connection != null)
                            {
                                result.Connection = connection;
                                result.ConnectionString = connection.ConnectionString;
                                resultString = connParameter.Connection;
                                result.DatabaseType = connParameter.DatabaseType;
                                result.UseAggregations = connParameter.CustomAggregationsAvailable;
                                result.ConnectionWithoutCredentials = connParameter.ConnectionWithoutCredentials;
                                break;
                            }
                            else
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
            }

            watch.Stop();
            result.ConnectTimeInMilliseconds = watch.ElapsedMilliseconds;

            return result;
        }

        private async Task<DbConnection> CreateConnectionByEndpoint(ServiceEndpoint endpoint, CancellationToken token = default)
        {
            var result = await CreateConnectionByEndpointsPriority(endpoint, token);

            // если не нашли подключение по нужному ендпоинту, берем по всем
            if (result.Connection is null && endpoint != ServiceEndpoint.All)
            {
                result = await CreateConnectionByEndpointsPriority(ServiceEndpoint.All, token);
            }

            return result;
        }

        private async Task<DbConnection> CreateConnectionByEndpointsPriority(ServiceEndpoint endpoint, CancellationToken token = default)
        {
            var availableDatabases = _databaseService.AvailableDatabases();
            var databasesByEndpoint = availableDatabases.Where(x => x.EndpointsList.Contains(endpoint));

            var randomizer = new DynamicWeightedRandomizer<DatabaseInfo>();
            foreach (var database in databasesByEndpoint)
            {
                var priority = database.Priority;
                if (database.PriorityCoefficient > 0)
                {
                    priority = (int)(priority * database.PriorityCoefficient);
                }
                randomizer.Add(database, priority);
            }

            var result = new DbConnection();
            SqlConnection? connection = null;

            while (connection == null && randomizer.Any())
            {
                var randomDatabase = randomizer.NextWithRemoval();

                connection = await GetConnectionByDatabaseInfo(randomDatabase, token);

                if (connection != null)
                {
                    result.Connection = connection;
                    result.ConnectionString = randomDatabase.Connection;
                    result.DatabaseType = randomDatabase.DatabaseType;
                    result.UseAggregations = randomDatabase.CustomAggregationsAvailable;
                    result.ConnectionWithoutCredentials = randomDatabase.ConnectionWithoutCredentials;
                }
            }        

            return result;
        }

        private async Task<SqlConnection?> GetConnectionByDatabaseInfo(DatabaseInfo databaseInfo, CancellationToken token = default)
        {
            SqlConnection? connection = null;

            try
            {
                connection = new(databaseInfo.Connection);

                await connection.OpenAsync(token);

                if (_checkConnection)
                {

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
            }
            catch (Exception ex)
            {
                var logElement = new ElasticLogElement
                {
                    Status = LogStatus.Error,
                    ErrorDescription = ex.Message,
                    DatabaseConnection = databaseInfo.ConnectionWithoutCredentials
                };
                _logger.LogElastic(logElement);
            }
                     
            return connection;
        }
    }
}
