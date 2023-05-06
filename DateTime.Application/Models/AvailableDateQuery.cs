namespace DateTime.Application.Models
{
    public class AvailableDateQuery
    {
        public required string CityId { get; init; }

        public required string[] DeliveryTypes { get; init; }

        public bool CheckQuantity { get; set; }

        public List<CodeItemQuery> Codes { get; set; }

        public AvailableDateQuery()
        {
            Codes = new List<CodeItemQuery>();
        }
    }
}
