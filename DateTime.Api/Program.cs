using AuthLibrary.Data;
using DateTimeService.Api;
using DateTimeService.Api.Filters;
using DateTimeService.Api.Middlewares;
using DateTimeService.Application;
using DateTimeService.Application.Database.DatabaseManagement;
using DateTimeService.Application.Logging;
using Hangfire;
using Microsoft.AspNetCore.Identity;
using System.Collections.Concurrent;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
//builder.Services.AddSwaggerGen();

builder.Services.AddApplication(builder.Configuration);
builder.Services.AddApi(builder.Configuration);

builder.Services.AddScoped<LogActionFilter>();
builder.Services.AddTransient<GlobalExceptionHandlingMiddleware>();

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

app.UseMiddleware<GlobalExceptionHandlingMiddleware>();

//app.Use(async (context, next) =>
//{
//    // создаем новый экземпляр ConcurrentDictionary для каждого запроса
//    context.Items[typeof(ConcurrentDictionary<object, object>)] = new ConcurrentDictionary<object, object>();

//    await next();
//});

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
        var configuration = services.GetRequiredService<IConfiguration>();
        await RoleInitializer.InitializeAsync(userManager, rolesManager, configuration);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "An error occurred while seeding the database.");
    }

    try
    {
        var db = services.GetRequiredService<DateTimeServiceContext>();
        await RoleInitializer.CleanTokensAsync(db);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "An error occurred while clearing the database.");
    }
}

app.Run();
