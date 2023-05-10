using System.Text.Json.Serialization;

namespace DateTimeService.Contracts.Requests
{
    public class AvailableDeliveryTypesItemRequest
    {
        [JsonPropertyName("code")]
        public required string Code { get; set; }

        [JsonPropertyName("sales_code")]
        public string? SalesCode { get; set; }

        [JsonPropertyName("count")]
        public int Quantity { get; set; }
    }
}
