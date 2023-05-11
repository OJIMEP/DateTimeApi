using DateTimeService.Application.Logging;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using System.Text.Json;
using Serilog.Context;

namespace DateTimeService.Api.Filters
{
    public class LogActionFilter : IActionFilter
    {
        private readonly ILogger<LogActionFilter> _logger;
        Stopwatch _watch;

        public LogActionFilter(ILogger<LogActionFilter> logger)
        {
            _logger = logger;
        }

        public void OnActionExecuting(ActionExecutingContext context)
        {
            _watch = Stopwatch.StartNew();

            try
            {
                if (context.ActionDescriptor is ControllerActionDescriptor controllerActionDescriptor)
                {
                    var requestBody = FormatRequestBody(context.ActionArguments);
                    context.HttpContext.Items["LogRequestBody"] = requestBody;
                }
            }
            catch (Exception ex)
            {
                _logger.LogElastic("Error in LogActionFilter", ex);
            }
        }

        public void OnActionExecuted(ActionExecutedContext context)
        {
            _watch.Stop();

            if (context.Result is null)
            {
                return;
            }

            var responseBody = "";
            if (context.Result is ObjectResult objectResult)
            {
                responseBody = JsonSerializer.Serialize(objectResult.Value);
            }

            var logElement = new ElasticLogElement()
            {
                ResponseContent = responseBody,
                TimeFullExecution = _watch.ElapsedMilliseconds
            };

            logElement.FillFromHttpContext(context.HttpContext);

            if (context.Result is BadRequestObjectResult badRequest)
            {
                logElement.Status = LogStatus.Error;
                logElement.ErrorDescription = "Некорректные входные данные";
                logElement.AdditionalData.Add("InputErrors", JsonSerializer.Serialize(badRequest.Value));
            }
            if (context.Result is ObjectResult internalError && internalError.StatusCode == StatusCodes.Status500InternalServerError)
            {
                logElement.Status = LogStatus.Error;
                logElement.ErrorDescription = internalError.Value.ToString();
            }

            _logger.LogElastic(logElement);
        }

        public string FormatRequestBody(IDictionary<string, object> actionArguments)
        {
            try
            {
                if (actionArguments != null)
                    return $"{JsonSerializer.Serialize(actionArguments["request"])}";
            }
            catch (Exception ex)
            {
                _logger.LogElastic("Error in LogActionFilter", ex);
            }
            return "";
        }
    }
}
