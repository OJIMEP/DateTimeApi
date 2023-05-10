using Microsoft.AspNetCore.Mvc;
using DateTimeService.Application.Repositories;
using DateTimeService.Contracts.Requests;
using DateTimeService.Api.Mapping;
using DateTimeService.Api.Filters;
using AuthLibrary.Data;
using Microsoft.AspNetCore.Authorization;
using DateTimeService.Contracts.Responses;

namespace DateTimeService.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
    [ProducesResponseType(StatusCodes.Status401Unauthorized, Type = typeof(string))]
    [ProducesResponseType(StatusCodes.Status500InternalServerError, Type = typeof(string))]
    [ServiceFilter(typeof(LogActionFilter))]
    public class DateTimeController: ControllerBase
    {
        private readonly IDateTimeRepository _dateTimeRepository;

        public DateTimeController(IDateTimeRepository dateTimeRepository)
        {
            _dateTimeRepository = dateTimeRepository;
        }

        [HttpPost("AvailableDate")]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(AvailableDateResponse))]
        [Authorize(Roles = UserRoles.AvailableDate + "," + UserRoles.Admin)]
        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public async Task<IActionResult> GetAvailableDateAsync(AvailableDateRequest request, CancellationToken token = default)
        {
            var query = request.MapToAvailableDateQuery();

            var result = await _dateTimeRepository.GetAvailableDateAsync(query, token);
            
            return Ok(result.MapToAvailableDateResponse());
        }

        [HttpPost("IntervalList")]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(IntervalListResponse))]
        [Authorize(Roles = UserRoles.IntervalList + "," + UserRoles.Admin)]
        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public async Task<IActionResult> GetIntervalListAsync(IntervalListRequest request, CancellationToken token = default)
        {
            var query = request.MapToIntervalListQuery();

            var result = await _dateTimeRepository.GetIntervalListAsync(query, token);

            return Ok(result.MapToIntervalListResponse());
        }

        [HttpPost("AvailableDeliveryTypes")]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(AvailableDeliveryTypesResponse))]
        [Authorize(Roles = UserRoles.IntervalList + "," + UserRoles.Admin)]
        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public async Task<IActionResult> GetAvailableDeliveryTypesAsync(AvailableDeliveryTypesRequest request, CancellationToken token = default)
        {
            var query = request.MapToAvailableDeliveryTypesQuery();

            var result = await _dateTimeRepository.GetAvailableDeliveryTypesAsync(query, token);

            return Ok(result.MapToAvailableDeliveryTypesResponse());
        }
    }
}
