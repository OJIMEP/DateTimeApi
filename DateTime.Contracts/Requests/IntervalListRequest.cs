﻿using System.Text.Json.Serialization;

namespace DateTimeService.Contracts.Requests
{
    public class IntervalListRequest
    {
        [JsonPropertyName("address_id")]
        public string AddressId { get; set; }

        [JsonPropertyName("delivery_type")]
        public string DeliveryType { get; set; }

        [JsonPropertyName("pickup_point")]
        public string PickupPoint { get; set; }

        [JsonPropertyName("floor")]
        public double? Floor { get; set; }

        [JsonPropertyName("payment")]
        public string Payment { get; set; }

        [JsonPropertyName("order_number")]
        public string OrderNumber { get; set; }

        [JsonPropertyName("order_date")]
        public System.DateTime OrderDate { get; set; }

        [JsonPropertyName("x_coordinate")]
        public string Xcoordinate { get; set; }

        [JsonPropertyName("y_coordinate")]
        public string Ycoordinate { get; set; }

        [JsonPropertyName("order_items")]
        public List<CodeItemRequest> OrderItems { get; set; }
    }
}
