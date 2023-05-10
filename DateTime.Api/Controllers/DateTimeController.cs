using Microsoft.AspNetCore.Mvc;
using DateTimeService.Application.Repositories;
using DateTimeService.Contracts.Requests;
using DateTimeService.Api.Mapping;
using DateTimeService.Api.Filters;

namespace DateTimeService.Api.Controllers
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

        [HttpPost("IntervalList")]
        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public async Task<IActionResult> GetIntervalListAsync(IntervalListRequest request, CancellationToken token = default)
        {
            var query = request.MapToIntervalListQuery();

            var result = await _dateTimeRepository.GetIntervalListAsync(query, token);

            return Ok(result.MapToIntervalListResponse());
        }
    }
}
