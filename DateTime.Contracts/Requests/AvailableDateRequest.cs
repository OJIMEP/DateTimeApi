using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace DateTime.Contracts.Requests
{
    public class AvailableDateRequest
    {
        [Required, JsonPropertyName("city_id")]
        public required string CityId { get; init; }

        [Required, JsonPropertyName("check_quantity")]
        public bool CheckQuantity { get; init; }

        [Required, JsonPropertyName("delivery_types")]
        public required string[] DeliveryTypes { get; init; }

        [Required, MinLength(1), JsonPropertyName("codes")]
        public required IEnumerable<CodeItemRequest> CodeItems { get; init; } 
    }
}
