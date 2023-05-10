using DateTimeService.Api.Filters;
using DateTimeService.Application;
using DateTimeService.Application.Database.DatabaseManagement;
using DateTimeService.Application.Logging;
using Hangfire;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
//builder.Services.AddSwaggerGen();

builder.Services.AddApplication(builder.Configuration);

builder.Services.AddScoped<LogActionFilter>();

builder.Logging.ClearProviders();
builder.Logging.AddProvider(new HttpLoggerProvider(builder.Configuration["loggerHost"],
    builder.Configuration.GetValue<int>("loggerPortUdp"),
    builder.Configuration.GetValue<int>("loggerPortHttp"),
    builder.Configuration["loggerEnv"]));

var app = builder.Build();

app.UseRouting();

// Configure the HTTP request pipeline.

app.UseHttpsRedirection();
app.UseAuthentication();
app.UseAuthorization();
app.MapControllers();

app.UseHangfireDashboard();

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

app.Run();
