using System.Text.Json.Serialization;

namespace DateTimeService.Contracts.Responses
{
    public class AvailableDateResponse
    {
        [JsonPropertyName("data")]
        public Dictionary<string, AvailableDateElementResponse> Data { get; set; }

        public AvailableDateResponse()
        {
            Data = new Dictionary<string, AvailableDateElementResponse>();
        }
    }
}
