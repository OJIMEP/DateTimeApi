using System.Text.Json.Serialization;

namespace DateTimeService.Contracts.Responses
{
    public class AvailableDateElementResponse
    {
        [JsonPropertyName("code")]
        public required string Code { get; init; }

        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull), JsonPropertyName("sales_code")]
        public string? SalesCode { get; init; }

        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull), JsonPropertyName("courier")]
        public string? Courier { get; init; }

        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull), JsonPropertyName("self")]
        public string? Self { get; init; }

        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull), JsonPropertyName("interval")]
        public string? YourTimeInterval { get; init; }
    }
}
