namespace DateTime.Application.Models
{
    public class CodeItemQuery
    {
        public required string Article { get; init; }
        public string? Code { get; init; }
        public string? SalesCode { get; init; }  // код с сайта без префиксов и нулей
        public int Quantity { get; init; }
        public required string CacheKey { get; init; }
        public IEnumerable<string> PickupPoints { get; init; } = Enumerable.Empty<string>();
    }
}
