using System.Text.Json.Serialization;

namespace DateTimeService.Contracts.Requests
{
    public class AvailableDeliveryTypesRequest
    {
        [JsonPropertyName("city_id")]
        public string CityId { get; set; }

        [JsonPropertyName("pickup_points")]
        public string[] PickupPoints { get; set; }

        [JsonPropertyName("items")]
        public List<AvailableDeliveryTypesItemRequest> OrderItems { get; set; }
    }
}
