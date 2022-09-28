// See https://aka.ms/new-console-template for more information

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SQLElasticDataBase;

using IHost host = Host.CreateDefaultBuilder(args).Build();

// Ask the service provider for the configuration abstraction.
IConfiguration config = host.Services.GetRequiredService<IConfiguration>();
string connectionString = config["connectionString"];

// Write the values to the console.
Console.WriteLine($"IPAddressRange:0 = {connectionString}");

// Get values from the config given their key and their target type.
int keyOneValue = config.GetValue<int>("KeyOne");
bool keyTwoValue = config.GetValue<bool>("KeyTwo");
string keyThreeNestedValue = config.GetValue<string>("KeyThree:Message");

// Write the values to the console.
Console.WriteLine($"KeyOne = {keyOneValue}");
Console.WriteLine($"KeyTwo = {keyTwoValue}");
Console.WriteLine($"KeyThree:Message = {keyThreeNestedValue}");

// Try to get a reference to the Shard Map Manager via the Shard Map Manager database.  
// If it doesn't already exist, then create it. 
ShardMapManager shardMapManager;
bool shardMapManagerExists = ShardMapManagerFactory.TryGetSqlShardMapManager(
                                    connectionString,
                                    ShardMapManagerLoadPolicy.Lazy,
                                    out shardMapManager);
 
if (shardMapManagerExists)
{
    Console.WriteLine("Shard Map Manager already exists");
}
else
{
    // Create the Shard Map Manager. 
    ShardMapManagerFactory.CreateSqlShardMapManager(connectionString);
    Console.WriteLine("Created SqlShardMapManager");

    shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
        connectionString,
        ShardMapManagerLoadPolicy.Lazy);

    // The connectionString contains server name, database name, and admin credentials 
    // for privileges on both the GSM and the shards themselves.
}
//var crr= new CreatePopulatedRangeMap(shardMapManager, "SQLElasticDataBaseShard0", config["SqlServerName"]);


Console.WriteLine("Hello, World!");
await host.RunAsync();