namespace DateTimeService.Application.Database.DatabaseManagement
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
