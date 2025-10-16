using Microsoft.Data.SqlClient;

namespace DateTimeService.Application.Database
{
    public class DbConnection: IDisposable
    {
        public SqlConnection Connection { get; set; }
        public DatabaseType DatabaseType { get; set; }
        public bool UseAggregations { get; set; }
        public string ConnectionWithoutCredentials { get; set; } = "";
        public long ConnectTimeInMilliseconds { get; set; }
        public string ConnectionString { get; set; } = "";
        public DateTimeOffset LastRecompileAvailableDate { get; set; }
        public DateTimeOffset LastRecompileIntervalList { get; set; }

        public void Dispose()
        {
            Connection.Dispose();
        }
    }

    public enum DatabaseType
    {
        Main,
        ReplicaFull,
        ReplicaTables
    }

    public enum ServiceEndpoint
    {
        AvailableDate,
        IntervalList,
        AvailableDeliveryTypes,
        All
    }
}
