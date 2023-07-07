using DateTimeService.Application.Cache;
using DateTimeService.Application.Database;
using DateTimeService.Application.Database.DatabaseManagement;
using DateTimeService.Application.Repositories;
using Hangfire;
using Hangfire.MemoryStorage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace DateTimeService.Application
{
    public static class ApplicationServiceCollectionExtensions
    {
        public static IServiceCollection AddApplication(this IServiceCollection services, IConfiguration configuration)
        {
            services
                .AddDatabaseManagement()
                .AddRepositories(configuration)
                .AddHttpClients()
                .AddRedis(configuration)
                .ConfigureHangfire();

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

        private static IServiceCollection AddRepositories(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton<IDateTimeRepository, DateTimeRepository>();

            if (configuration.GetValue<bool>("UseDapper"))
            {
                services.AddSingleton<IDatabaseRepository, DatabaseRepositoryDapper>();
            }
            else
            {
                services.AddSingleton<IDatabaseRepository, DatabaseRepositoryBasic>();
            }

            services.AddSingleton<RedisRepository>();

            services.AddTransient<IGeoZones, GeoZones>();

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

            if (redisSettings.Enabled)
            {
                try
                {
                    services.AddSingleton<IConnectionMultiplexer>(x =>
                        ConnectionMultiplexer.Connect(new ConfigurationOptions
                        {
                            EndPoints = { redisSettings.ConnectionString },
                            Password = redisSettings.Password,
                            ConnectTimeout = 1,
                            DefaultDatabase = (int)redisSettings.Database,
                            AbortOnConnectFail = false                         
                        })
                    );
                }
                catch {
                    services.AddSingleton<IConnectionMultiplexer>(x => null);
                    redisSettings.Enabled = false;
                }            
            }
            else
            {
                services.AddSingleton<IConnectionMultiplexer>(x => null);
            }

            services.AddSingleton(redisSettings);

            return services;
        }
    }
}
