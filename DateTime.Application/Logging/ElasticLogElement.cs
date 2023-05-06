using Microsoft.AspNetCore.Http;
using System.Text.Json.Serialization;

namespace DateTime.Application.Logging
{
    public class ElasticLogElement
    {
        public Guid Id { get; set; }
        public string? Path { get; set; }
        public string? Host { get; set; }
        public string? ResponseContent { get; set; }
        public string? RequestContent { get; set; }
        public long TimeSQLExecutionFact { get; set; }
        [JsonConverter(typeof(JsonStringEnumConverter))]
        public LogStatus Status { get; set; }
        public string? ErrorDescription { get; set; }
        public long TimeFullExecution { get; set; }
        public string? DatabaseConnection { get; set; }
        public string? AuthenticatedUser { get; set; }
        public long TimeBtsExecution { get; set; }
        public long TimeLocationExecution { get; set; }
        public long LoadBalancingExecution { get; set; }
        public long GlobalParametersExecution { get; set; }
        public long TimeGettingFromCache { get; set; }
        public Dictionary<string, string> AdditionalData { get; set; }
        public string Environment { get; set; }
        public string ServiceName { get; set; }
        public int TotalItems { get; set; }
        public double FromCachePercent { get; set; }

        public ElasticLogElement()
        {
            Environment = AppSettings.Environment ?? "unset";
            AdditionalData = new();
            ServiceName = "DateTime";
            Id = Guid.NewGuid();
        }

        public void FillFromHttpContext(HttpContext httpContext)
        {
            Status = LogStatus.Ok;
            Path = httpContext.Request.Path;
            Host = httpContext.Request.Host.ToString();
            AuthenticatedUser = httpContext.User.Identity?.Name;
            AdditionalData.Add("Referer", httpContext.Request.Headers["Referer"].ToString());
            AdditionalData.Add("User-Agent", httpContext.Request.Headers["User-Agent"].ToString());
            AdditionalData.Add("RemoteIpAddress", httpContext.Connection.RemoteIpAddress.ToString());

            FillFromHttpContextItems(httpContext.Items);
        }

        public void FillFromHttpContextItems(IDictionary<object, object> items)
        {
            try
            {
                RequestContent = items.TryGetValue("LogRequestBody", out object? request) ? request.ToString() : "";
                DatabaseConnection = items.TryGetValue("DatabaseConnection", out object? connection) ? (string)connection : "";
                TimeSQLExecutionFact = items.TryGetValue("TimeSqlExecutionFact", out object? timeSql) ? (long)timeSql : 0;
                LoadBalancingExecution = items.TryGetValue("LoadBalancingExecution", out object? timeLoad) ? (long)timeLoad : 0;
                GlobalParametersExecution = items.TryGetValue("GlobalParametersExecution", out object? globalParameters) ? (long)globalParameters : 0;
                TimeLocationExecution = items.TryGetValue("TimeLocationExecution", out object? timeLocation) ? (long)timeLocation : 0;
                TimeGettingFromCache = items.TryGetValue("TimeGettingFromCache", out object? timeCache) ? (long)timeCache : 0;
                TotalItems = items.TryGetValue("TotalItems", out object? totalItems) ? (int)totalItems : 0;
                var FromCache = items.TryGetValue("FromCache", out object? fromCache) ? (int)fromCache : 0;
                FromCachePercent = TotalItems != 0 ? Math.Round(FromCache / (double)TotalItems * 100, 2) : 0;
            }
            catch { }
        }
    }
}
