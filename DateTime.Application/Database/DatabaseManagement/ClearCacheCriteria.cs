namespace DateTimeService.Application.Database.DatabaseManagement
{
    public class ClearCacheCriteria
    {
        public required string CriteriaType { get; init; } //RecordCount, MaximumResponseTime
        public float Percentile_95 { get; init; }
        public float RecordCountBegin { get; init; } //в минутах
        public float RecordCountEnd { get; init; }
        public float LoadBalance { get; init; }
    }
}
