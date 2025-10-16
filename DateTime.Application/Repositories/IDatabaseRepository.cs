using DateTimeService.Application.Models;

namespace DateTimeService.Application.Repositories
{
    public interface IDatabaseRepository
    {
        Task<AvailableDateResult> GetAvailableDates(AvailableDateQuery query, CancellationToken token = default);

        Task<IntervalListResult> GetIntervalList(IntervalListQuery query, CancellationToken token = default);

        Task<DeliveryTypeAvailabilityResult> GetDeliveryTypeAvailability(AvailableDeliveryTypesQuery query, string deliveryType, CancellationToken token = default);

        Task<bool> UsePreliminaryCalculation(string cityId, CancellationToken token = default);
    }
}
