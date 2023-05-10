namespace DateTimeService.Application.Database.DatabaseManagement
{
    public interface IDatabaseAvailabilityControl
    {
        public Task CheckAndUpdateDatabasesStatus(CancellationToken cancellationToken);
    }
}
