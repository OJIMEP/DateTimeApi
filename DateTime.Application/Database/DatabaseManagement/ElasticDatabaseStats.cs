namespace DateTimeService.Application.Database.DatabaseManagement
{
    public class ElasticDatabaseStats
    {
        public int RecordCount { get; set; }
        public double Percentile95Time { get; set; }
        public double AverageTime { get; set; }
        public double LoadBalanceTime { get; set; }
    }
}
