﻿namespace DateTimeService.Application.Database.DatabaseManagement
{
    public interface IReloadDatabasesService
    {
        public Task ReloadAsync(CancellationToken cancellationToken);
    }
}
