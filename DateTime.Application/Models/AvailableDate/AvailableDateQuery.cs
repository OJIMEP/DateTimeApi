using Microsoft.AspNetCore.Mvc.ModelBinding.Validation;

namespace DateTimeService.Application.Models
{
    public class AvailableDateQuery
    {
        public string CityId { get; init; }

        public string[] DeliveryTypes { get; init; }

        public bool CheckQuantity { get; set; }

        public List<CodeItemQuery> Codes { get; set; }

        public AvailableDateQuery()
        {
            Codes = new List<CodeItemQuery>();
        }

        public AvailableDateQuery(bool fillData)
        {
            if (!fillData)
            {
                return;
            }

            CityId = "17030";
            DeliveryTypes = new string[] { "courier", "self" };
            var codesList = @"5684713, 5888304, 5820023, 5820020, 1095503, 5674906, 375559, 375561, 13775, 
                            5896679, 375560, 6291525, 13773, 13774, 6291513, 798732, 5807722, 5896606, 375554, 
                            375552, 5896563, 715431, 29495, 5915459, 6291529, 6029958, 6492972, 604836, 
                            5896691, 645932, 5896687, 623892, 673875, 86148, 379315, 379314, 514331, 
                            623899, 5896682, 441772, 87373, 86803, 5861320, 963086, 5805546, 86147, 
                            604815, 645930, 379304, 337651, 30695, 30694, 46223, 5896573, 30693, 1161502, 
                            616583, 5902601, 116731, 1161504".Split(",");
            List<CodeItemQuery> items = new();
            foreach (var item in codesList)
            {
                CodeItemQuery itemDTO = new()
                {
                    Article = item,
                    CacheKey = item,
                    PickupPoints = new string[] { "340", "388", "460", "417", "234", "2" }
                };
                items.Add(itemDTO);
            }
            Codes = items;
        }

        public static void SplitByQuantity(AvailableDateQuery query, out AvailableDateQuery queryWithQuantity, out AvailableDateQuery queryWithoutQuantity)
        {
            queryWithQuantity = new AvailableDateQuery
            {
                CityId = query.CityId,
                DeliveryTypes = query.DeliveryTypes,
                CheckQuantity = true
            };

            queryWithoutQuantity = new AvailableDateQuery
            {
                CityId = query.CityId,
                DeliveryTypes = query.DeliveryTypes,
                CheckQuantity = false
            };

            foreach (var item in query.Codes) 
            { 
                if (item.Quantity > 1)
                {
                    queryWithQuantity.Codes.Add(item);
                }
                else
                {
                    queryWithoutQuantity.Codes.Add(item);
                }
            }
        }

        public static IEnumerable<AvailableDateQuery> SplitByCodes(AvailableDateQuery source, int batchSize = 30)
        {
            if (source.Codes == null || source.Codes.Count == 0)
                return new List<AvailableDateQuery> { source };

            if (source.Codes.Count <= batchSize)
                return new List<AvailableDateQuery> { source };

            return source.Codes
                .Select((code, index) => new { code, index })
                .GroupBy(x => x.index / batchSize)
                .Select(group => new AvailableDateQuery
                {
                    CityId = source.CityId,
                    DeliveryTypes = source.DeliveryTypes,
                    CheckQuantity = source.CheckQuantity,
                    Codes = group.Select(x => x.code).ToList()
                })
                .ToList();
        }
    }
}
