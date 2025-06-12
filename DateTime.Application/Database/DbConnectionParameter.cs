namespace DateTimeService.Application.Database
{
    public class DbConnectionParameter
    {
        public string Connection { get; init; }
        public int Priority { get; set; }
        public string Type { get; set; } //main, replica_full, replica_tables 

        public string Endpoints { get; set; } // AvailableDate, IntervalList, AvailableDeliveryTypes
    }
}
