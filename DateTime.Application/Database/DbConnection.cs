using Microsoft.Data.SqlClient;

namespace DateTime.Application.Database
{
    public class DbConnection
    {
        public SqlConnection Connection { get; set; }
        public DatabaseType DatabaseType { get; set; }
        public bool UseAggregations { get; set; }
        public string ConnectionWithoutCredentials { get; set; } = "";
        public long ConnectTimeInMilliseconds { get; set; }
    }

    public enum DatabaseType
    {
        Main,
        ReplicaFull,
        ReplicaTables
    }
}
