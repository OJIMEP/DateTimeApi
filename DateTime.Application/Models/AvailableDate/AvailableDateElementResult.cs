namespace DateTimeService.Application.Models
{
    public class AvailableDateElementResult
    {
        public required string Code { get; set; }

        public string? SalesCode { get; set; }

        public string? Courier { get; set; }

        public string? Self { get; set; }

        public int YourTimeInterval { get; set; }

        public AvailableDateElementResult()
        {
            this.Courier = null;
            this.Self = null;
            this.YourTimeInterval = 0;
        }
    }
}
