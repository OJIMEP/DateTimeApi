using System.Text.Json.Serialization;

namespace DateTimeService.Contracts.Responses
{
    public class IntervalListElementResponse
    {
        [JsonPropertyName("begin")]
        public DateTime Begin { get; set; }

        [JsonPropertyName("end")]
        public DateTime End { get; set; }

        [JsonPropertyName("bonus")]
        public bool Bonus { get; set; }
    }
}
