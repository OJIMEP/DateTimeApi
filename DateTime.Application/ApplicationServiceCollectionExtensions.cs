using DateTimeService.Application.Cache;
using DateTimeService.Application.Database;
using DateTimeService.Application.Database.DatabaseManagement;
using DateTimeService.Application.Logging;
using DateTimeService.Application.Repositories;
using Hangfire;
using Hangfire.MemoryStorage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DateTimeService.Application
{
    public static class ApplicationServiceCollectionExtensions
    {
        public static IServiceCollection AddApplication(this IServiceCollection services, IConfiguration configuration)
        {
            services
                .AddDatabaseManagement()
                .AddRepositories()
                .AddHttpClients()
                .AddRedis(configuration)
                .ConfigureHangfire();

            AppSettings.Initialize(configuration);

            return services;
        }

        private static IServiceCollection AddDatabaseManagement(this IServiceCollection services)
        {
            services.AddSingleton<IDbConnectionFactory, SqlConnectionFactory>();

            services.AddSingleton<IReadableDatabase, ReadableDatabasesService>();
            services.AddTransient<IReloadDatabasesService, ReloadDatabasesFromFileService>();
            services.AddTransient<IDatabaseCheck, DatabaseCheckService>();
            services.AddTransient<IDatabaseAvailabilityControl, DatabaseAvailabilityControlService>();
            
            return services;
        }

        private static IServiceCollection AddRepositories(this IServiceCollection services)
        {
            services.AddSingleton<IDateTimeRepository, DateTimeRepository>();

            return services;
        }

        private static IServiceCollection ConfigureHangfire(this IServiceCollection services)
        {
            services.AddHangfire(x => x.UseMemoryStorage());

            services.AddHangfireServer(options =>
            {
                options.SchedulePollingInterval = TimeSpan.FromMilliseconds(5000);
            });

            return services;
        }

        private static IServiceCollection AddHttpClients(this IServiceCollection services)
        {
            //services.AddHttpClient<IGeoZones, GeoZones>();
            services.AddHttpClient<ILogger, HttpLogger>();

            services.AddHttpClient<DatabaseCheckService>("elastic").ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
            });

            return services;
        }

        private static IServiceCollection AddRedis(this IServiceCollection services, IConfiguration configuration)
        {
            var redisSettings = new RedisSettings();

            configuration.GetSection(nameof(RedisSettings)).Bind(redisSettings);

            services.AddSingleton(redisSettings);

            if (redisSettings.Enabled)
            {
                services.AddSingleton<IConnectionMultiplexer>(x =>
                    ConnectionMultiplexer.Connect(redisSettings.ConnectionString)
                );
            }
            else
            {
                services.AddSingleton<IConnectionMultiplexer>(x => null);
            }

            return services;
        }
    }
}
