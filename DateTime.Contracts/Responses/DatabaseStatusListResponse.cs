using System.Net;

namespace DateTimeService.Contracts.Responses
{
    public class DatabaseStatusListResponse
    {
        public int Priority { get; set; }
        public string Type { get; set; }
        public string ConnectionWithoutCredentials { get; set; }
        public bool AvailableToUse { get; set; }
        public DateTimeOffset LastFreeProcCacheCommand { get; set; }
        public DateTimeOffset LastCheckAvailability { get; set; }
        public DateTimeOffset LastCheckAggregations { get; set; }
        public DateTimeOffset LastCheckPerfomance { get; set; }
        public bool CustomAggregationsAvailable { get; set; }
        public int CustomAggsFailCount { get; set; }
        public int TimeCriteriaFailCount { get; set; }
        public List<string> EndpointList { get; set; }
        public double PriorityCoefficient { get; set; }
        public DateTimeOffset LastRecompileAvailableDate { get; set; }
        public DateTimeOffset LastRecompileIntervalList { get; set; }
    }
}
