using Microsoft.Extensions.Logging;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace DateTime.Application.Logging
{
    public class HttpLogger : ILogger
    {
        private readonly string _logsHost;
        private readonly int _logsPortHttp;
        private readonly UdpClient _udpClient;
        private readonly HttpClient _httpClient;

        public HttpLogger(string host, int portUdp, int portHttp, string env)
        {
            _logsHost = host;
            _logsPortHttp = portHttp;
            _udpClient = new UdpClient(host, portUdp);
            _httpClient = new HttpClient();
        }

        public IDisposable BeginScope<TState>(TState state) => default!;

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public async void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (formatter != null)
            {
                var logMessage = new ElasticLogMessage();
                if (!formatter(state, exception).Contains("ResponseContent"))
                {
                    var logElement = new ElasticLogElement
                    {
                        ErrorDescription = formatter(state, exception),
                        Status = LogStatus.Info
                    };

                    if (exception != null)
                    {
                        logElement.ErrorDescription += ";" + exception.Message;
                        logElement.Status = LogStatus.Ok;
                        logElement.AdditionalData.Add("StackTrace", exception.StackTrace);
                    }

                    var logstringElement = JsonSerializer.Serialize(logElement);
                    logMessage.Message.Add(logstringElement);
                }
                else
                {
                    logMessage.Message.Add(formatter(state, exception));
                }

                var resultLog = JsonSerializer.Serialize(logMessage);

                Byte[] sendBytes = Encoding.UTF8.GetBytes(resultLog);

                try
                {
                    if (sendBytes.Length > 60000)
                    {
                        var _ = await _httpClient.PostAsync(
                            new Uri($"http://{_logsHost}:{_logsPortHttp:D}"),
                            new StringContent(resultLog, Encoding.UTF8, "application/json")
                        );
                    }
                    else
                        await _udpClient.SendAsync(sendBytes, sendBytes.Length);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            }
        }
    }
}
