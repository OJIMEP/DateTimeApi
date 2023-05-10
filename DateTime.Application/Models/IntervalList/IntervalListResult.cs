namespace DateTimeService.Application.Models
{
    public class IntervalListResult
    {
        public List<IntervalListElementResult> Data { get; set; }

        public IntervalListResult()
        {
            Data = new List<IntervalListElementResult>();
        }
    }
}
