namespace DateTimeService.Application.Models
{
    public class AvailableDateRecord
    {
        public required string Article { get; init; }

        public required string Code { get; init; }

        public DateTime Courier { get; init; }

        public DateTime Self { get; init; }
    }
}
