using DateTime.Application.Models;

namespace DateTime.Application.Repositories
{
    public interface IDateTimeRepository
    {
        Task<AvailableDateResult> GetAvailableDateAsync(AvailableDateQuery query, CancellationToken token = default); 
    }
}
