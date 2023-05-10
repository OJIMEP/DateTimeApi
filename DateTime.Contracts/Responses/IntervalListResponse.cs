using System.Text.Json.Serialization;

namespace DateTimeService.Contracts.Responses
{
    public class IntervalListResponse
    {
        [JsonPropertyName("data")]
        public List<IntervalListElementResponse> Data { get; set; }

        public IntervalListResponse()
        {
            Data = new List<IntervalListElementResponse>();
        }
    }
}
