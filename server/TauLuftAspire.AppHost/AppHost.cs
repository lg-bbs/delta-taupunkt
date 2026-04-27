var builder = DistributedApplication.CreateBuilder(args);

builder.AddDockerComposeEnvironment("env");

var postgres = builder.AddPostgres("postgres-server")
                      .WithImageTag("latest")
                      .WithDataVolume();

var myDb = postgres.AddDatabase("tauluftdb");

var apiService = builder.AddProject<Projects.TauLuftAspire_ApiService>("apiservice")
    .WithReference(myDb)
    .WithHttpHealthCheck("/health");

builder.AddProject<Projects.TauLuftAspire_Web>("webfrontend")
    .WithExternalHttpEndpoints()
    .WithHttpHealthCheck("/health")
    .WithReference(apiService)
    .WaitFor(apiService);

builder.Build().Run();
