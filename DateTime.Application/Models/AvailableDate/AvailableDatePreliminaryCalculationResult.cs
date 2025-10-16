namespace DateTimeService.Application.Models
{
    public class AvailableDatePreliminaryCalculationResult
    {
        public List<AvailableDateRecord> availableDateRecords { get; set; }
        public long sqlExecutionTime;
        public long sqlConnectionTime;

        public AvailableDatePreliminaryCalculationResult()
        {
            availableDateRecords = new();
        }
    }
}
