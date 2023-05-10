using System.Text.Json.Serialization;

namespace DateTimeService.Application.Database.DatabaseManagement.Elastic
{
    public class Request
    {
        public Request()
        {
            Query = new();
            Aggregations = new();
        }

        [JsonPropertyName("query"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public QueryClass Query { get; init; }

        [JsonPropertyName("aggs"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public Dictionary<string, AggregationClass> Aggregations { get; init; }

        [JsonPropertyName("size")]
        public int Size { get; init; }
    }

    public class QueryClass
    {
        [JsonPropertyName("bool")]
        public BoolClass Bool { get; init; }

        public QueryClass()
        {
            Bool = new();
        }
    }

    public class BoolClass
    {
        [JsonPropertyName("filter")]
        public List<FilterElement> Filter { get; init; }
        public BoolClass()
        {
            Filter = new();
        }
    }

    public class FilterElement
    {
        [JsonPropertyName("range"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public Dictionary<string, object> Range { get; init; }

        [JsonPropertyName("term"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public Dictionary<string, object> Term { get; init; }
    }

    public class AggregationClass
    {
        [JsonPropertyName("terms"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public ConditionsClass Terms { get; init; }

        [JsonPropertyName("percentiles"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public ConditionsClass Percentiles { get; init; }

        [JsonPropertyName("avg"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public ConditionsClass Avg { get; init; }

        [JsonPropertyName("aggs"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public Dictionary<string, AggregationClass> Aggregations { get; init; }
    }

    public class ConditionsClass
    {
        [JsonPropertyName("field")]
        public string Field { get; init; }

        [JsonPropertyName("size"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public Int32 Size { get; init; }

        [JsonPropertyName("percents"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public double[] Percents { get; init; }
    }
}
