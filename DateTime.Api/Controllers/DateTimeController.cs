using Microsoft.AspNetCore.Mvc;
using DateTime.Application.Repositories;
using DateTime.Contracts.Requests;
using DateTime.Api.Mapping;
using DateTime.Api.Filters;

namespace DateTime.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    [ServiceFilter(typeof(LogActionFilter))]
    public class DateTimeController: ControllerBase
    {
        private readonly IDateTimeRepository _dateTimeRepository;

        public DateTimeController(IDateTimeRepository dateTimeRepository)
        {
            _dateTimeRepository = dateTimeRepository;
        }

        [HttpPost("AvailableDate")]
        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public async Task<IActionResult> GetAvailableDateAsync(AvailableDateRequest request, CancellationToken token = default)
        {
            var query = request.MapToAvailableDateQuery();

            var result = await _dateTimeRepository.GetAvailableDateAsync(query, token);
            
            return Ok(result.MapToAvailableDateResponse());
        }
    }
}
