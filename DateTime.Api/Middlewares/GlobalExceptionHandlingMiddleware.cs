using DateTimeService.Application.Logging;
using System.ComponentModel.DataAnnotations;
using System.Net;

namespace DateTimeService.Api.Middlewares
{
    public class GlobalExceptionHandlingMiddleware : IMiddleware
    {
        private readonly ILogger<GlobalExceptionHandlingMiddleware> _logger;

        public GlobalExceptionHandlingMiddleware(ILogger<GlobalExceptionHandlingMiddleware> logger)
        {
            _logger = logger;
        }

        public async Task InvokeAsync(HttpContext context, RequestDelegate next)
        {
            try
            {
                await next(context);
            }
            catch (Exception ex)
            {
                var logElement = new ElasticLogElement()
                {
                    Status = LogStatus.Error,
                    ErrorDescription = ex.Message
                };

                logElement.FillFromHttpContext(context);

                _logger.LogElastic(logElement);

                if (ex is ValidationException)
                {
                    context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                }
                else
                {
                    context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                }

                context.Response.ContentType = "application/json";

                await context.Response.WriteAsync(ex.Message);
            }
        }
    }
}
