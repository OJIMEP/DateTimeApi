using AuthLibrary.Data;
using DateTimeService.Api.Mapping;
using DateTimeService.Application.Database;
using DateTimeService.Application.Database.DatabaseManagement;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;

namespace DateTimeService.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ServiceController : ControllerBase
    {
        private readonly IReadableDatabase _readableDatabaseService;
        private readonly IDbConnectionFactory _dbConnectionFactory;

        public ServiceController(IReadableDatabase readableDatabaseService, IDbConnectionFactory dbConnectionFactory)
        {
            _readableDatabaseService = readableDatabaseService;
            _dbConnectionFactory = dbConnectionFactory;
        }

        [Route("HealthCheck")]
        [HttpGet]
        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public async Task<IActionResult> HealthCheckAsync(CancellationToken token)
        {
            SqlConnection conn;

            try
            {
                var dbConnection = await _dbConnectionFactory.CreateConnectionAsync(token);
                conn = dbConnection.Connection;
            }
            catch
            {
                return StatusCode(500);
            }

            if (conn == null)
            {
                Dictionary<string, string> errorDesc = new()
                {
                    { "ErrorDescription", "Не найдено доступное соединение к БД" }
                };

                return StatusCode(500, errorDesc);
            }

            await conn.CloseAsync();
            return StatusCode(200, new { Status = "Ok" });
        }

        [Route("Databases")]
        [Authorize(Roles = UserRoles.Admin)]
        [HttpGet]
        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public async Task<IActionResult> CurrenDBStatesAsync()
        {
            var dbList = _readableDatabaseService.GetAllDatabases().Select(x => x.MapToDatabaseStatusListResponse()).ToList();

            return Ok(dbList);
        }
    }
}
