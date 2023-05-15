using AuthLibrary.Data;
using DateTimeService.Api;
using DateTimeService.Api.Middlewares;
using DateTimeService.Application;
using DateTimeService.Application.Database.DatabaseManagement;
using Hangfire;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.AspNetCore.Identity;
using Serilog;
using Serilog.Formatting.Elasticsearch;
using System.Collections.Concurrent;

var builder = WebApplication.CreateBuilder(args);

var configuration = builder.Configuration;

// Add services to the container.
builder.Services.AddCors();

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();

builder.Services.AddApplication(configuration);
builder.Services.AddApi(configuration);

builder.Services.AddHttpContextAccessor();

Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .WriteTo.Http(
        requestUri: configuration["ElasticConfiguration:Uri"], null, textFormatter: new ElasticsearchJsonFormatter(inlineFields: true)
        )
    //.Filter.ByIncludingOnly(Matching.WithProperty<string>("SourceContext", s => s.Contains("DateTimeService")))
    .Enrich.WithProperty("Environment", configuration["Environment"])
    .Enrich.WithProperty("ServiceName", "DateTime")
    .ReadFrom.Configuration(configuration)
    .CreateLogger();

builder.Host.UseSerilog(Log.Logger);

var app = builder.Build();

app.UseForwardedHeaders(new ForwardedHeadersOptions
{
    ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto
});

app.UseCors(builder => builder.AllowAnyHeader()
    .AllowAnyMethod()
    .SetIsOriginAllowedToAllowWildcardSubdomains()
    .WithOrigins(configuration.GetSection("CorsOrigins").Get<List<string>>().ToArray()));

app.UseRouting();

app.UseAuthentication();
app.UseAuthorization();
app.MapControllers();

app.UseSwagger();
app.UseSwaggerUI();

app.UseHangfireDashboard();

app.UseMiddleware<GlobalExceptionHandlingMiddleware>();

app.Use(async (context, next) =>
{
    // создаем новый экземпляр ConcurrentDictionary для каждого запроса
    context.Items[typeof(ConcurrentDictionary<object, object>)] = new ConcurrentDictionary<object, object>();

    await next();
});

await OnAppStarting();

if (app.Environment.IsDevelopment())
{
    var logger = app.Services.GetRequiredService<ILogger<Program>>();
    logger.LogElastic("Starting development");
}

app.Run();

async Task OnAppStarting()
{
    try
    {
        var reloadDatabasesService = app.Services.GetRequiredService<IReloadDatabasesService>();
        RecurringJob.AddOrUpdate("ReloadDatabasesFromFiles", () => reloadDatabasesService.ReloadAsync(CancellationToken.None), "*/10 * * * * *"); //every 10 seconds

        var checkStatusService = app.Services.GetRequiredService<IDatabaseAvailabilityControl>();
        RecurringJob.AddOrUpdate("CheckAndUpdateDatabasesStatus", () => checkStatusService.CheckAndUpdateDatabasesStatus(CancellationToken.None), Cron.Minutely());
    }
    catch (Exception ex)
    {
        var logger = app.Services.GetRequiredService<ILogger<Program>>();
        logger.LogError(ex, "An error occurred while starting recurring job.");
    }

    using (var scope = app.Services.CreateScope())
    {
        var services = scope.ServiceProvider;
        var logger = services.GetRequiredService<ILogger<Program>>();
        try
        {
            //var db = services.GetRequiredService<DateTimeServiceContext>();
            //db.Database.Migrate();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred while migrating the database.");
        }

        try
        {
            var userManager = services.GetRequiredService<UserManager<DateTimeServiceUser>>();
            var rolesManager = services.GetRequiredService<RoleManager<IdentityRole>>();
            await RoleInitializer.InitializeAsync(userManager, rolesManager, configuration);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred while seeding the database.");
        }

        try
        {
            //var db = services.GetRequiredService<DateTimeServiceContext>();
            //await RoleInitializer.CleanTokensAsync(db);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred while clearing the database.");
        }
    }
}
