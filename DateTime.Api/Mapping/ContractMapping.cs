using DateTimeService.Application.Database.DatabaseManagement;
using DateTimeService.Application.Models;
using DateTimeService.Contracts.Requests;
using DateTimeService.Contracts.Responses;

namespace DateTimeService.Api.Mapping
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

        public static IntervalListQuery MapToIntervalListQuery(this IntervalListRequest request)
        {
            return new IntervalListQuery
            {
                AddressId = request.AddressId,
                DeliveryType = request.DeliveryType,
                PickupPoint = request.PickupPoint,
                PickupPointType = request.PickupPointType,
                Floor = request.Floor,
                Apartment = request.Apartment,
                Payment = request.Payment,
                OrderNumber = request.OrderNumber,
                OrderDate = request.OrderDate,
                Xcoordinate = request.Xcoordinate,
                Ycoordinate = request.Ycoordinate,
                OrderItems = request.OrderItems.Select(item => item.MapToCodeItemQuery()).ToList()
            };
        }

        public static AvailableDeliveryTypesQuery MapToAvailableDeliveryTypesQuery(this AvailableDeliveryTypesRequest request)
        {
            return new AvailableDeliveryTypesQuery
            {
                CityId = request.CityId,
                PickupPoints = request.PickupPoints,
                OrderItems = request.OrderItems.Select(item => item.MapToAvailableDeliveryTypesElementQuery()).ToList()

            };
        }

        public static AvailableDeliveryTypesElementQuery MapToAvailableDeliveryTypesElementQuery(this AvailableDeliveryTypesItemRequest request)
        {
            return new AvailableDeliveryTypesElementQuery
            {
                Article = request.Code,
                SalesCode = request.SalesCode,
                Quantity = request.Quantity,
                Code = request.SalesCode == null ? null : GetCodeFromSaleCode(request.SalesCode)
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
                    Self = value.Self,
                    YourTimeInterval = value.YourTimeInterval
                };
            }

            return response;
        }

        public static IntervalListResponse MapToIntervalListResponse(this IntervalListResult result)
        {
            var response = new IntervalListResponse
            {
                Data = result.Data.Select(item => new IntervalListElementResponse
                {
                    Begin = item.Begin,
                    End = item.End,
                    Bonus = item.Bonus,
                    IntervalType = item.IntervalType
                }).ToList()
            };

            return response;
        }

        public static AvailableDeliveryTypesResponse MapToAvailableDeliveryTypesResponse(this AvailableDeliveryTypesResult result)
        {
            var response = new AvailableDeliveryTypesResponse();
            response.Courier.IsAvailable = result.Courier;
            response.Self.IsAvailable = result.Self;
            response.YourTime.IsAvailable = result.YourTime;

            return response;
        }

        public static DatabaseStatusListResponse MapToDatabaseStatusListResponse(this DatabaseInfo result)
        {
            return new DatabaseStatusListResponse
            {
                Priority = result.Priority,
                Type = result.Type,
                ConnectionWithoutCredentials = result.ConnectionWithoutCredentials,
                AvailableToUse = result.AvailableToUse,
                LastFreeProcCacheCommand = result.LastFreeProcCacheCommand,
                LastCheckAvailability = result.LastCheckAvailability,
                LastCheckAggregations = result.LastCheckAggregations,
                LastCheckPerfomance = result.LastCheckPerfomance,
                CustomAggregationsAvailable = result.CustomAggregationsAvailable,
                CustomAggsFailCount = result.CustomAggsFailCount,
                TimeCriteriaFailCount = result.TimeCriteriaFailCount,
                EndpointList = result.EndpointsList.Select(endpoint => endpoint.ToString()).ToList(),
                PriorityCoefficient = result.PriorityCoefficient
            };
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
