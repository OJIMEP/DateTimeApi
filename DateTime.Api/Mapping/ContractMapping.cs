using DateTime.Application.Models;
using DateTime.Contracts.Requests;
using DateTime.Contracts.Responses;

namespace DateTime.Api.Mapping
{
    public static class ContractMapping
    {
        public static AvailableDateQuery MapToAvailableDateQuery(this AvailableDateRequest request)
        {
            return new AvailableDateQuery
            {
                CityId = request.CityId,
                DeliveryTypes = request.DeliveryTypes,
                CheckQuantity = request.CheckQuantity,
                Codes = request.CodeItems.Select(item => item.MapToCodeItemQuery()).ToList()
            };
        }

        public static CodeItemQuery MapToCodeItemQuery(this CodeItemRequest request)
        {
            return new CodeItemQuery
            {
                Article = request.Code,
                SalesCode = request.SalesCode,
                Quantity = request.Quantity,
                Code = request.SalesCode is null ? null : GetCodeFromSaleCode(request.SalesCode),
                CacheKey = request.SalesCode is null ? request.Code : $"{request.Code}_{request.SalesCode}",
                PickupPoints = request.PickupPoints
            };
        }

        public static AvailableDateResponse MapToAvailableDateResponse(this AvailableDateResult result)
        {
            var response = new AvailableDateResponse();

            foreach (var (key, value) in result.Data)
            {
                if (value.Self is null && value.Courier is null)
                {
                    continue;
                }

                response.Data[key] = new AvailableDateElementResponse
                {
                    Code = value.Code,
                    SalesCode = value.SalesCode,
                    Courier = value.Courier,
                    Self = value.Self
                };
            }

            return response;
        }

        //123456 => 00-00123456
        private static string GetCodeFromSaleCode(string saleCode)
        {
            string prefix = "00-";
            int codeLength = 8;
            return $"{prefix}{saleCode.PadLeft(codeLength, '0')}";
        }
    }
}
