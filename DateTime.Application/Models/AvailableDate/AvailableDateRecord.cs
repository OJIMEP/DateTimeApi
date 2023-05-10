namespace DateTimeService.Application.Models
{
    public class AvailableDateRecord
    {
        public required string Article { get; init; }

        public required string Code { get; init; }

        public System.DateTime Courier { get; init; }

        public System.DateTime Self { get; init; }
    }
}
