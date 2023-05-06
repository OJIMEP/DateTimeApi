namespace DateTime.Application.Models
{
    public class AvailableDateElementResult
    {
        public required string Code { get; set; }

        public string? SalesCode { get; set; }

        public string? Courier { get; set; }

        public string? Self { get; set; }
    }
}
