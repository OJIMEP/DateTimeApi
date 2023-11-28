namespace DateTimeService.Application.Models
{
    public class IntervalListQuery
    {
        public string AddressId { get; set; }

        public string DeliveryType { get; set; }

        public string PickupPoint { get; set; }

        public double? Floor { get; set; }

        public double? Apartment { get; set; }

        public string Payment { get; set; }

        public string OrderNumber { get; set; }

        public DateTime OrderDate { get; set; }

        public string Xcoordinate { get; set; }

        public string Ycoordinate { get; set; }

        public List<CodeItemQuery> OrderItems { get; set; }

        public double FloorForIntervalList()
        {
            var defaultFloor = 4;
            
            if (IsNullOrEmpty(Floor))
            {
                if (IsNullOrEmpty(Apartment))
                {
                    return 0;
                }
                return defaultFloor;
            }

            var maxFloor = 9;
            if (Floor > maxFloor)
            {
                return defaultFloor;
            }

            return (double)Floor;
        }

        private static bool IsNullOrEmpty(double? value)
        {
            if (!value.HasValue)
            {
                return true;
            }
            if (value == 0)
            {
                return true;
            }
            return false;
        }
    }
}
