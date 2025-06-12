namespace DateTimeService.Application.Database.DatabaseManagement
{
    public class DatabaseInfo : DbConnectionParameter, ICloneable, IComparable<DatabaseInfo>
    {
        public string ConnectionWithoutCredentials { get; set; }
        public bool AvailableToUse { get; set; }
        public DateTimeOffset LastFreeProcCacheCommand { get; set; }
        public DateTimeOffset LastCheckAvailability { get; set; }
        public DateTimeOffset LastCheckAggregations { get; set; }
        public DateTimeOffset LastCheckPerfomance { get; set; }
        public int ActualPriority { get; set; }
        public bool ExistsInFile { get; set; }
        public bool CustomAggregationsAvailable { get; set; }
        public int CustomAggsFailCount { get; set; }
        public int TimeCriteriaFailCount { get; set; }
        public double PriorityCoefficient { get; set; } = 1;
        public DatabaseType DatabaseType { get; set; }
        public List<ServiceEndpoint> EndpointsList { get; set; }

        public DatabaseInfo(DbConnectionParameter connectionParameter)
        {
            Connection = connectionParameter.Connection;
            ConnectionWithoutCredentials = RemoveCredentialsFromConnectionString(connectionParameter.Connection);
            Priority = connectionParameter.Priority;
            Type = connectionParameter.Type;
            ActualPriority = connectionParameter.Priority;
            DatabaseType = Type switch
            {
                "main" => DatabaseType.Main,
                "replica_full" => DatabaseType.ReplicaFull,
                "replica_tables" => DatabaseType.ReplicaTables,
                _ => DatabaseType.Main
            };
            Endpoints = connectionParameter.Endpoints;
            if (Endpoints is not null)
            {
                EndpointsList = Endpoints.Split(",")
                    .Select(s => (ServiceEndpoint)Enum.Parse(typeof(ServiceEndpoint), s.Trim(), ignoreCase: true))
                    .ToList();
            } else
            {
                EndpointsList = new List<ServiceEndpoint> { ServiceEndpoint.All };
            }
            
        }

        public object Clone()
        {
            var result = new DatabaseInfo(this)
            {
                AvailableToUse = AvailableToUse,
                LastFreeProcCacheCommand = LastFreeProcCacheCommand,
                LastCheckAvailability = LastCheckAvailability,
                LastCheckAggregations = LastCheckAggregations,
                LastCheckPerfomance = LastCheckPerfomance,
                ActualPriority = ActualPriority,
                ExistsInFile = ExistsInFile,
                CustomAggregationsAvailable = CustomAggregationsAvailable,
                CustomAggsFailCount = CustomAggsFailCount,
                TimeCriteriaFailCount = TimeCriteriaFailCount,
                PriorityCoefficient = PriorityCoefficient,
                Type = Type,
                Connection = Connection,
                DatabaseType = DatabaseType,
                EndpointsList = EndpointsList
            };

            return result;
        }
        private static string RemoveCredentialsFromConnectionString(string connectionString)
        {
            return string.Join(";",
                connectionString.Split(";")
                    .Where(item => !item.Contains("Uid") && !item.Contains("User") && !item.Contains("Pwd") && !item.Contains("Password") && item.Length > 0));
        }

        public int CompareTo(DatabaseInfo? other)
        {
            return ConnectionWithoutCredentials.CompareTo(other.ConnectionWithoutCredentials);
        }
    }
}
