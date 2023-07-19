using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Services;

namespace DynamoCounter.Tests;

[CollectionDefinition("dynamo")]
public class DynamoCollection : ICollectionFixture<DynamoFixture>
{
}

public class DynamoFixture : IAsyncLifetime
{
    private IContainerService container = null!;
    public bool KillContainerAfterTests { get; set; }

    public Task InitializeAsync()
    {
        this.container = new Builder()
            .UseContainer()
            .WithName("dynamocounters")
            .DeleteIfExists(true, true)
            .UseImage("amazon/dynamodb-local:2.0.0")
            .KeepContainer()
            .KeepRunning()
            .Command(string.Empty, new[] { "-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb" })
            .ExposePort(8000, 8000)
            .WaitForPort("8000/tcp", 5000)
            .Build()
            .Start();

        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        if (this.KillContainerAfterTests)
        {
            this.container.StopOnDispose = true;
            this.container.RemoveOnDispose = true;
        }

        this.container.Dispose();
        return Task.CompletedTask;
    }
}