using System.Text.Json.Serialization;

namespace DateTime.Application.Database.DatabaseManagement.Elastic
{
    public class Response
    {
        [JsonPropertyName("took")]
        public int Took { get; set; }

        [JsonPropertyName("timed_out")]
        public bool TimedOut { get; set; }

        [JsonPropertyName("_shards")]
        public Shards Shards { get; set; }

        [JsonPropertyName("aggregations")]
        public Dictionary<string, Aggregations> Aggregations { get; set; }

        public Response()
        {
            Aggregations = new();
        }
    }

    // Root myDeserializedClass = JsonSerializer.Deserialize<Root>(myJsonResponse);
    public class Shards
    {
        [JsonPropertyName("total")]
        public int Total { get; init; }

        [JsonPropertyName("successful")]
        public int Successful { get; init; }

        [JsonPropertyName("skipped")]
        public int Skipped { get; init; }

        [JsonPropertyName("failed")]
        public int Failed { get; init; }
    }

    public class Total
    {
        [JsonPropertyName("value")]
        public int Value { get; init; }

        [JsonPropertyName("relation")]
        public string Relation { get; init; }
    }

    public class Aggregations
    {
        [JsonPropertyName("doc_count_error_upper_bound")]
        public int DocCountErrorUpperBound { get; init; }

        [JsonPropertyName("sum_other_doc_count")]
        public int SumOtherDocCount { get; init; }

        [JsonPropertyName("buckets")]
        public List<BucketClass> Buckets { get; init; }

        public Aggregations()
        {
            Buckets = new();
        }
    }

    public class BucketClass
    {
        [JsonPropertyName("key")]
        public string Key { get; init; }

        [JsonPropertyName("doc_count")]
        public int DocCount { get; init; }

        [JsonPropertyName("week_avg")]
        public AggValues WeekAvg { get; init; }

        [JsonPropertyName("time_percentile")]
        public AggValues TimePercentile { get; init; }

        [JsonPropertyName("load_bal")]
        public AggValues LoadBalance { get; init; }
    }

    public class AggValues
    {
        [JsonPropertyName("value")]
        public double Value { get; init; }

        [JsonPropertyName("values")]
        public Dictionary<string, double> Values { get; init; }
    }
}
