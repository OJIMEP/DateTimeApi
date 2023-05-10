namespace DateTimeService.Application.Models
{
    public class AvailableDeliveryTypesQuery
    {
        public string CityId { get; set; }

        public string[] PickupPoints { get; set; }

        public List<AvailableDeliveryTypesElementQuery> OrderItems { get; set; }
    }
}
