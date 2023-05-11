using DateTimeService.Application;
using DateTimeService.Contracts.Requests;
using FluentValidation;

namespace DateTimeService.Api.Validators
{
    public class IntervalListRequestValidator: AbstractValidator<IntervalListRequest>
    {
        public IntervalListRequestValidator()
        {
            RuleFor(x => x)
                .Must(x => !String.IsNullOrEmpty(x.DeliveryType) || !String.IsNullOrEmpty(x.OrderNumber))
                .WithMessage("Должен быть указан тип доставки или номер имеющегося заказа");

            // доставка
            RuleFor(x => x)
                .Must(x => !String.IsNullOrEmpty(x.AddressId) || (!String.IsNullOrEmpty(x.Xcoordinate) && !String.IsNullOrEmpty(x.Ycoordinate)))
                .WithMessage("При курьерской доставке должен быть заполнен код адреса или координаты")
                .When(x => x.DeliveryType == Constants.CourierDelivery || x.DeliveryType == Constants.YourTimeDelivery);

            RuleFor(x => x)
                .Must(x => !String.IsNullOrEmpty(x.Xcoordinate) && !String.IsNullOrEmpty(x.Ycoordinate))
                .WithMessage("Обе координаты должны быть заполнены")
                .When(x => (x.DeliveryType == Constants.CourierDelivery || x.DeliveryType == Constants.YourTimeDelivery) && String.IsNullOrEmpty(x.AddressId));

            RuleFor(x => x.PickupPoint)
                .Empty()
                .WithMessage("При курьерской доставке код ПВЗ должен отсутствовать")
                .When(x => x.DeliveryType == Constants.CourierDelivery || x.DeliveryType == Constants.YourTimeDelivery);

            RuleFor(x => x.OrderNumber)
                .Empty()
                .WithMessage("При курьерской доставке номер заказа должен отсутствовать")
                .When(x => x.DeliveryType == Constants.CourierDelivery || x.DeliveryType == Constants.YourTimeDelivery);

            RuleFor(x => x.OrderDate)
                .Empty()
                .WithMessage("При курьерской доставке дата заказа должна отсутствовать")
                .When(x => x.DeliveryType == Constants.CourierDelivery || x.DeliveryType == Constants.YourTimeDelivery);

            // самовывоз
            RuleFor(x => x.AddressId)
                .Empty()
                .WithMessage("При самовывозе код адреса должен отсутствовать")
                .When(x => x.DeliveryType == Constants.Self);

            RuleFor(x => x.Floor)
                .Empty()
                .WithMessage("При самовывозе этаж должен отсутствовать")
                .When(x => x.DeliveryType == Constants.Self);

            RuleFor(x => x.PickupPoint)
                .NotEmpty()
                .WithMessage("При самовывозе код ПВЗ должен быть заполнен")
                .When(x => x.DeliveryType == Constants.Self);

            RuleFor(x => x.OrderNumber)
                .Empty()
                .WithMessage("При самовывозе номер заказа должен отсутствовать")
                .When(x => x.DeliveryType == Constants.Self);

            RuleFor(x => x.OrderDate)
                .Empty()
                .WithMessage("При самовывозе дата заказа должна отсутствовать")
                .When(x => x.DeliveryType == Constants.Self);

            // имеющийся заказ
            RuleFor(x => x.OrderNumber)
                .NotEmpty()
                .WithMessage("При указании времени заказа, должен быть указан и номер")
                .When(x => x.OrderDate != default);

            RuleFor(x => x.OrderDate)
                .NotEmpty()
                .WithMessage("При указании номера заказа, должна быть указана и дата")
                .When(x => !String.IsNullOrEmpty(x.OrderNumber));

            RuleFor(x => x.AddressId)
                .Empty()
                .WithMessage("При указании имеющегося заказа код адреса должен отсутствовать")
                .When(x => !String.IsNullOrEmpty(x.OrderNumber) || x.OrderDate != default);

            RuleFor(x => x.Floor)
                .Empty()
                .WithMessage("При указании имеющегося заказа этаж должен отсутствовать")
                .When(x => !String.IsNullOrEmpty(x.OrderNumber) || x.OrderDate != default);

            RuleFor(x => x.PickupPoint)
                .Empty()
                .WithMessage("При указании имеющегося заказа код ПВЗ должен отсутствовать")
                .When(x => !String.IsNullOrEmpty(x.OrderNumber) || x.OrderDate != default);
        }
    }
}
