using DateTimeService.Contracts.Requests;
using FluentValidation;

namespace DateTimeService.Api.Validators
{
    public class CodeItemRequestValidator: AbstractValidator<CodeItemRequest>
    {
        public CodeItemRequestValidator()
        {
            RuleFor(x => x.SalesCode)
                .Must(salesCode => salesCode == null || salesCode.Trim() != "")
                .WithMessage("Поле уценки не должно быть пустой строкой - либо заполнено, либо поле в принципе отсутствует");
        }
    }
}
