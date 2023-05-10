namespace DateTimeService.Application.Models
{
    public class IntervalListRecord
    {
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
        public int OrdersCount { get; set; }
        public int Bonus { get; set; }
    }
}
