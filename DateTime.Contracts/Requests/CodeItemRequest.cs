using System.Text.Json.Serialization;

namespace DateTime.Contracts.Requests
{
    public class CodeItemRequest
    {
        [JsonPropertyName("code")]
        public required string Code { get; set; }

        [JsonPropertyName("sales_code")]
        public string? SalesCode { get; set; }

        [JsonPropertyName("quantity")]
        public int Quantity { get; set; }

        [JsonPropertyName("pickup_points")]
        public IEnumerable<string> PickupPoints { get; set; } = Enumerable.Empty<string>();
    }
}
