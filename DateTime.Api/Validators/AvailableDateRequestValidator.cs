using DateTimeService.Contracts.Requests;
using FluentValidation;

namespace DateTimeService.Api.Validators
{
    public class AvailableDateRequestValidator : AbstractValidator<AvailableDateRequest>
    {
        public AvailableDateRequestValidator()
        {
            RuleFor(x => x.CityId)
                .NotEmpty()
                .WithMessage("Должен быть указан код города");

            RuleFor(x => x)
                .Must(x => x.CodeItems.All( item => item.Quantity == 0))
                .WithMessage("При отключенной проверке количества, поле количества должно отсутствовать или быть равным нулю")
                .When(x => !x.CheckQuantity);

            RuleForEach(x => x.CodeItems)
                .SetValidator(new CodeItemRequestValidator());
        }
    }
}
