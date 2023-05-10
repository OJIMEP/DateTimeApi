using System.Text.Json.Serialization;

namespace DateTimeService.Contracts.Responses
{
    public class IntervalListElementResponse
    {
        [JsonPropertyName("begin")]
        public System.DateTime Begin { get; set; }

        [JsonPropertyName("end")]
        public System.DateTime End { get; set; }

        [JsonPropertyName("bonus")]
        public bool Bonus { get; set; }
    }
}
