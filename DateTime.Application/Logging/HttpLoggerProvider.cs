using Microsoft.Extensions.Logging;

namespace DateTimeService.Application.Logging
{
    public class HttpLoggerProvider : ILoggerProvider
    {
        private readonly string host;
        private readonly int port;
        private readonly int portHttp;
        private readonly string env;
        public HttpLoggerProvider(string _host, int _port, int _portHttp, string _env)
        {
            host = _host;
            port = _port;
            portHttp = _portHttp;
            env = _env;
        }
        public ILogger CreateLogger(string categoryName)
        {
            return new HttpLogger(host, port, portHttp, env);
        }

        public void Dispose()
        {
        }
    }
}
