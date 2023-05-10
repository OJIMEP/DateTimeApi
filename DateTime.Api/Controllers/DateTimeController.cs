﻿using Microsoft.AspNetCore.Mvc;
using DateTimeService.Application.Repositories;
using DateTimeService.Contracts.Requests;
using DateTimeService.Api.Mapping;
using DateTimeService.Api.Filters;
using AuthLibrary.Data;
using Microsoft.AspNetCore.Authorization;

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
        [Authorize(Roles = UserRoles.AvailableDate + "," + UserRoles.Admin)]
        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public async Task<IActionResult> GetAvailableDateAsync(AvailableDateRequest request, CancellationToken token = default)
        {
            var query = request.MapToAvailableDateQuery();

            var result = await _dateTimeRepository.GetAvailableDateAsync(query, token);
            
            return Ok(result.MapToAvailableDateResponse());
        }

        [HttpPost("IntervalList")]
        [Authorize(Roles = UserRoles.IntervalList + "," + UserRoles.Admin)]
        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public async Task<IActionResult> GetIntervalListAsync(IntervalListRequest request, CancellationToken token = default)
        {
            var query = request.MapToIntervalListQuery();

            var result = await _dateTimeRepository.GetIntervalListAsync(query, token);

            return Ok(result.MapToIntervalListResponse());
        }

        [HttpPost("AvailableDeliveryTypes")]
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
