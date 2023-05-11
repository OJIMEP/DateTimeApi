using DateTimeService.Application;
using DateTimeService.Contracts.Requests;
using FluentValidation;

namespace DateTimeService.Api.Validators
{
    public class AvailableDateRequestValidator : AbstractValidator<AvailableDateRequest>
    {
        public AvailableDateRequestValidator()
        {
            string[] allowedDeliveryTypes = { Constants.CourierDelivery, Constants.Self };

            RuleFor(x => x.CityId)
                .NotEmpty()
                .WithMessage("Должен быть указан код города");

            RuleFor(x => x.DeliveryTypes)
                .NotEmpty()
                .WithMessage("Должен быть указан хоть один тип доставки");

            RuleFor(x => x.DeliveryTypes)
                .Must(arr => arr.All(v => allowedDeliveryTypes.Contains(v)))
                .WithMessage("Указан некорректный тип доставки");

            RuleFor(x => x)
                .Must(x => x.CodeItems.All( item => item.Quantity == 0))
                .WithMessage("При отключенной проверке количества, поле количества должно отсутствовать или быть равным нулю")
                .When(x => !x.CheckQuantity);

            RuleForEach(x => x.CodeItems)
                .SetValidator(new CodeItemRequestValidator());
        }
    }
}
