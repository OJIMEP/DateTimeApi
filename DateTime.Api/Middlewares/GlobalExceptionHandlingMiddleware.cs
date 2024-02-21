using DateTimeService.Application.Logging;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
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
            Stopwatch watch = Stopwatch.StartNew();

            try
            {
                await next(context);
            }
            catch (Exception ex)
            {
                var logElement = new ElasticLogElement()
                {
                    ErrorDescription = ex.Message
                };

                logElement.FillFromHttpContext(context);
                logElement.Status = LogStatus.Error;

                watch.Stop();
                logElement.TimeFullExecution = watch.ElapsedMilliseconds;

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
