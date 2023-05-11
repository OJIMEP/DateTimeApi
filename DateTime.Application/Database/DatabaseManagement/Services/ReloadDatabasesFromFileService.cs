using DateTimeService.Api;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DateTimeService.Application.Database.DatabaseManagement
{
    public class ReloadDatabasesFromFileService : IReloadDatabasesService
    {
        protected readonly IReadableDatabase _databasesService;
        private readonly ILogger<ReloadDatabasesFromFileService> _logger;
        private readonly IConfiguration _configuration;

        public ReloadDatabasesFromFileService(IReadableDatabase databases,
                                              ILogger<ReloadDatabasesFromFileService> logger,
                                              IConfiguration configuration)
        {
            _databasesService = databases;
            _logger = logger;
            _configuration = configuration;
        }

        public async Task ReloadAsync(CancellationToken cancellationToken)
        {
            var dbList = _configuration.GetSection("OneSDatabases").Get<List<DbConnectionParameter>>();

            dbList ??= new();

            List<DatabaseInfo> databases = dbList.Select(x => new DatabaseInfo(x)).ToList();

            var result = await _databasesService.SynchronizeDatabasesListFromFile(databases);

            if (!result)
            {
                _logger.LogElastic("Database reloading from file failed!");
            }
        }
    }
}
