using DateTimeService.Application.Models;

namespace DateTimeService.Application.Repositories
{
    public interface IDateTimeRepository
    {
        Task<AvailableDateResult> GetAvailableDateAsync(AvailableDateQuery query, CancellationToken token = default); 

        Task<IntervalListResult> GetIntervalListAsync(IntervalListQuery query, CancellationToken token = default);
    }
}
