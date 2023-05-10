namespace DateTimeService.Application.Models
{
    public class DeliveryTypeAvailabilityResult
    {
        public string deliveryType;
        public bool available;
        public long loadBalancingTime;
        public long sqlExecutionTime;
        public string connection;
    }
}
