using System.Text.Json.Serialization;

namespace DateTimeService.Contracts.Responses
{
    public class AvailableDeliveryTypesResponse
    {
        [JsonPropertyName("courier")]
        public DeliveryTypeAvailability Courier { get; set; }

        [JsonPropertyName("pickup_point")]
        public DeliveryTypeAvailability Self { get; set; }

        [JsonPropertyName("interval")]
        public DeliveryTypeAvailability YourTime { get; set; }

        public AvailableDeliveryTypesResponse()
        {
            Courier = new();
            Self = new();
            YourTime = new();
        }
    }

    public class DeliveryTypeAvailability
    {
        [JsonPropertyName("available")]
        public bool IsAvailable { get; set; }
    }
}
