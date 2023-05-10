namespace DateTimeService.Application.Cache
{
    public class RedisSettings
    {
        public bool Enabled { get; set; }
        public string ConnectionString { get; set; }
        public RedisDatabase Database { get; set; }
        public int LifeTime { get; set; }
    }

    public enum RedisDatabase
    {
        Prod,
        Test,
        Dev
    }
}
