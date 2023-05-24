namespace DateTimeService.Application.Queries
{
    public static class DbCheckQueries
    {
        public const string ReplicaFull = """
            select datediff(ms, last_commit_time, getdate())
            from [master].[sys].[dm_hadr_database_replica_states]
            """;

        public const string Main = "Select TOP(1) _RecordKey FROM dbo._Const21165";

        public const string ReplicaTables = "Select TOP(1) _RecordKey FROM dbo._Const21165";

        public const string CheckAggregations = @"EXEC [dbo].[spCheckAggregates]";

        public const string ClearCacheScriptDefault = @"dbcc freeproccache";
    }
}
