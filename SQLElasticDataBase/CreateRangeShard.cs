// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//Modified: Madhan KAMALAKANNAN, 21/09/2022

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Extensions.Configuration;

namespace SQLElasticDataBase
{
    public class CreateRangeShard
    {
        /// <summary>
        /// Creates a new shard (or uses an existing empty shard), adds it to the shard map,
        /// and assigns it the specified range if possible.
        /// </summary>
        /// 
        static IConfiguration config = null;
        static string shardMapManagerServerName = string.Empty;// config["ShardMapManagerServerName"]; // Choose the shard name
        //private static void AddShard()
        //{
        //    RangeShardMap<int> shardMap = TryGetShardMap();
        //    if (shardMap != null)
        //    {
        //        // Here we assume that the ranges start at 0, are contiguous, 
        //        // and are bounded (i.e. there is no range where HighIsMax == true)
        //        int currentMaxHighKey = shardMap.GetMappings().Max(m => m.Value.High);
        //        int defaultNewHighKey = currentMaxHighKey + 100;

        //        Console.WriteLine("A new range with low key {0} will be mapped to the new shard.", currentMaxHighKey);
        //        int newHighKey = ConsoleUtils.ReadIntegerInput(
        //            string.Format("Enter the high key for the new range [default {0}]: ", defaultNewHighKey),
        //            defaultNewHighKey,
        //            input => input > currentMaxHighKey);

        //        Range<int> range = new Range<int>(currentMaxHighKey, newHighKey);

        //        Console.WriteLine();
        //        Console.WriteLine("Creating shard for range {0}", range);
        //        CreateShardSample.CreateShard(shardMap, range);
        //    }
        //    else
        //    {
        //        // Here we assume that the ranges start at 0, are contiguous, 
        //        // and are bounded (i.e. there is no range where HighIsMax == true)
        //        int currentMaxHighKey = 0;//shardMap.GetMappings().Max(m => m.Value.High);
        //        int defaultNewHighKey = currentMaxHighKey + 100;

        //        Console.WriteLine("A new range with low key {0} will be mapped to the new shard.", currentMaxHighKey);
        //        int newHighKey = ConsoleUtils.ReadIntegerInput(
        //            string.Format("Enter the high key for the new range [default {0}]: ", defaultNewHighKey),
        //            defaultNewHighKey,
        //            input => input > currentMaxHighKey);

        //        Range<int> range = new Range<int>(currentMaxHighKey, newHighKey);

        //        Console.WriteLine();
        //        Console.WriteLine("Creating shard for range {0}", range);
        //        CreateOrGetRangeShardMap();
        //        CreateShardSample.CreateShard(shardMap, range);
        //    }
        //}
        public static void CreateShard(RangeShardMap<int> shardMap, Range<int> rangeForNewShard, string databaseName)
        {
            config = SqlDatabaseUtils.GetConfigaration();
            shardMapManagerServerName = config["SQLServerName"];
            databaseName = databaseName + rangeForNewShard.Low + "";
            SqlDatabaseUtils.CreateDatabase(shardMapManagerServerName,databaseName);
            // Create a new shard, or get an existing empty shard (if a previous create partially succeeded).
            Shard shard = CreateOrGetEmptyShard(shardMap,databaseName);
           
            // Create a mapping to that shard.
            RangeMapping<int> mappingForNewShard = shardMap.CreateRangeMapping(rangeForNewShard, shard);

            Console.Write("Mapped range {0} to shard {1}", mappingForNewShard.Value, shard.Location.Database);
        }

        /// <summary>
        /// Script file that will be executed to initialize a shard.
        /// </summary>
        private const string InitializeShardScriptFile = "InitializeShard.sql";

        /// <summary>
        /// Format to use for creating shard name. {0} is the number of shards that have already been created.
        /// </summary>
        private const string ShardNameFormat = "SQLElasticDataBase_Shard{0}";

        /// <summary>
        /// Creates a new shard, or gets an existing empty shard (i.e. a shard that has no mappings).
        /// The reason why an empty shard might exist is that it was created and initialized but we 
        /// failed to create a mapping to it.
        /// </summary>
        private static Shard CreateOrGetEmptyShard(RangeShardMap<int> shardMap, string databaseName)
        {
            // Get an empty shard if one already exists, otherwise create a new one
            Shard shard = FindEmptyShard(shardMap);
            if (shard == null)
            {
                // No empty shard exists, so create one

                //// Choose the shard name
                //string databaseName = config["Shard01DatabaseName"];// //shardMap//string.Format(ShardNameFormat, shardMap.GetShards().Count());

                // Only create the database if it doesn't already exist. It might already exist if
                // we tried to create it previously but hit a transient fault.
                //if (!SqlDatabaseUtils.DatabaseExists(shardMapManagerServerName, databaseName))
                //{
                // SqlDatabaseUtils.CreateDatabase(shardMapManagerServerName, databaseName);
                //}

                // Create schema and populate reference data on that database
                // The initialize script must be idempotent, in case it was already run on this database
                // and we failed to add it to the shard map previously
                //SqlDatabaseUtils.ExecuteSqlScript(
                //    shardMapManagerServerName, databaseName, InitializeShardScriptFile);

                // Add it to the shard map
                ShardLocation shardLocation = new ShardLocation(shardMapManagerServerName, databaseName);
                shard = ShardManagementUtils.CreateOrGetShard(shardMap, shardLocation);

                string connstr = config["connectionString"];
                SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder(connstr);
                connStrBldr.DataSource = shardMapManagerServerName;
                connStrBldr.InitialCatalog = databaseName;

                // Go into a DbContext to trigger migrations and schema deployment for the new shard.
                // This requires an un-opened connection.
                using(var db = new ElasticScaleContext<int>(connStrBldr.ConnectionString))
                {
                    // Run a query to engage EF migrations
                    (from b in db.Blogs
                     select b).Count();
                }
            }

            return shard;
        }

        /// <summary>
        /// Finds an existing empty shard, or returns null if none exist.
        /// </summary>
        private static Shard FindEmptyShard(RangeShardMap<int> shardMap)
        {
            // Get all shards in the shard map
            IEnumerable<Shard> allShards = shardMap.GetShards();

            // Get all mappings in the shard map
            IEnumerable<RangeMapping<int>> allMappings = shardMap.GetMappings();

            // Determine which shards have mappings
            HashSet<Shard> shardsWithMappings = new HashSet<Shard>(allMappings.Select(m => m.Shard));

            // Get the first shard (ordered by name) that has no mappings, if it exists
            return allShards.OrderBy(s => s.Location.Database).FirstOrDefault(s => !shardsWithMappings.Contains(s));
        }

        //public SqlConnection OpenConnectionForKey<TKey>(TKey key, string connectionString)
        //{
        //    return OpenConnectionForKey(key, connectionString, ConnectionOptions.Validate);
        //}

        //public SqlConnection OpenConnectionForKey<TKey>(TKey key, string connectionString, SqlCredential secureCredential)
        //{
        //    return OpenConnectionForKey(key, connectionString, secureCredential, ConnectionOptions.Validate);
        //}

        //public SqlConnection OpenConnectionForKey<TKey>(TKey key, string connectionString, ConnectionOptions options)
        //{
        //    return OpenConnectionForKey(key, connectionString, null, options);
        //}

        //public SqlConnection OpenConnectionForKey<TKey>(TKey key, string connectionString, SqlCredential secureCredential, ConnectionOptions options)
        //{
        //    Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");
        //    using (new ActivityIdScope(Guid.NewGuid()))
        //    {
        //        IShardMapper<TKey> mapper = Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.GetMapper<TKey>();
        //        if (mapper == null)
        //        {
        //            throw new ArgumentException(StringUtils.FormatInvariant(Errors._ShardMap_OpenConnectionForKey_KeyTypeNotSupported, typeof(TKey), StoreShardMap.Name, ShardKey.TypeFromShardKeyType(StoreShardMap.KeyType)), "key");
        //        }

        //        return mapper.OpenConnectionForKey(key, new SqlConnectionInfo(connectionString, secureCredential), options);
        //    }
        //}

        //public Task<SqlConnection> OpenConnectionForKeyAsync<TKey>(TKey key, string connectionString)
        //{
        //    return OpenConnectionForKeyAsync(key, connectionString, ConnectionOptions.Validate);
        //}

        //public Task<SqlConnection> OpenConnectionForKeyAsync<TKey>(TKey key, string connectionString, SqlCredential secureCredential)
        //{
        //    return OpenConnectionForKeyAsync(key, connectionString, secureCredential, ConnectionOptions.Validate);
        //}

        //public Task<SqlConnection> OpenConnectionForKeyAsync<TKey>(TKey key, string connectionString, ConnectionOptions options)
        //{
        //    return OpenConnectionForKeyAsync(key, connectionString, null, options);
        //}

        //public Task<SqlConnection> OpenConnectionForKeyAsync<TKey>(TKey key, string connectionString, SqlCredential secureCredential, ConnectionOptions options)
        //{
        //    ExceptionUtils.DisallowNullArgument(connectionString, "connectionString");
        //    using (new ActivityIdScope(Guid.NewGuid()))
        //    {
        //        IShardMapper<TKey> mapper = GetMapper<TKey>();
        //        if (mapper == null)
        //        {
        //            throw new ArgumentException(StringUtils.FormatInvariant(Errors._ShardMap_OpenConnectionForKey_KeyTypeNotSupported, typeof(TKey), StoreShardMap.Name, ShardKey.TypeFromShardKeyType(StoreShardMap.KeyType)), "key");
        //        }

        //        return mapper.OpenConnectionForKeyAsync(key, new SqlConnectionInfo(connectionString, secureCredential), options);
        //    }
        //}

    }
}
