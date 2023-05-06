using System.Text.Json.Serialization;

namespace DateTime.Application.Logging
{
    public class ElasticLogMessage
    {
        [JsonPropertyName("message")]
        public List<string> Message { get; set; }

        public ElasticLogMessage()
        {
            Message = new List<string>();
        }
    }
}
