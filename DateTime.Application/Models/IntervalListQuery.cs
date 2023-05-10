namespace DateTimeService.Application.Models
{
    public class IntervalListQuery
    {
        public string AddressId { get; set; }

        public string DeliveryType { get; set; }

        public string PickupPoint { get; set; }

        public double? Floor { get; set; }

        public string Payment { get; set; }

        public string OrderNumber { get; set; }

        public DateTime OrderDate { get; set; }

        public string Xcoordinate { get; set; }

        public string Ycoordinate { get; set; }

        public List<CodeItemQuery> OrderItems { get; set; }
    }
}
