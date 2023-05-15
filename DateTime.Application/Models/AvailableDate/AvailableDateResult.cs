namespace DateTimeService.Application.Models
{
    public class AvailableDateResult
    {
        public bool WithQuantity { get; set; }

        public Dictionary<string, AvailableDateElementResult> Data { get; set; }

        public AvailableDateResult()
        {
            Data = new Dictionary<string, AvailableDateElementResult>();
        }
    }
}
