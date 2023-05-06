namespace DateTime.Application.Database.DatabaseManagement
{
    public enum DatabaseActions
    {
        Error,
        None,
        SendClearCache,
        DisableZeroExecutionTime,
        DisableBigExecutionTime,
        DisableBigLoadBalanceTime
    }
}
