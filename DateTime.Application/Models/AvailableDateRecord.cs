namespace DateTime.Application.Models
{
    public class AvailableDateRecord
    {
        public required string Article { get; init; }

        public string? Code { get; init; }

        public DateTimeOffset Courier { get; init; }

        public DateTimeOffset Self { get; init; }
    }
}
