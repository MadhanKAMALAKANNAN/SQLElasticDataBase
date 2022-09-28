// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Configuration;
using SQLElasticDataBase;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System.Text;

namespace SQLElasticDataBase
{
    public static class ShardManagementUtils
    {
        /// <summary>
        /// Tries to get the ShardMapManager that is stored in the specified database.
        /// </summary>
        public static ShardMapManager TryGetShardMapManager(string shardMapManagerServerName, string shardMapManagerDatabaseName)
        {
            string shardMapManagerConnectionString = SqlDatabaseUtils.GetConfigaration()["connectionString"];
                     

            if (!SqlDatabaseUtils.DatabaseExists(shardMapManagerServerName, shardMapManagerDatabaseName))
            {
                // Shard Map Manager database has not yet been created
                return null;
            }

            ShardMapManager shardMapManager;
            bool smmExists = ShardMapManagerFactory.TryGetSqlShardMapManager(
                shardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy,
                out shardMapManager);

            if (!smmExists)
            {
                // Shard Map Manager database exists, but Shard Map Manager has not been created
                return null;
            }

            return shardMapManager;
        }

        /// <summary>
        /// Creates a shard map manager in the database specified by the given connection string.
        /// </summary>
        public static ShardMapManager CreateOrGetShardMapManager(string shardMapManagerConnectionString)
        {
            // Get shard map manager database connection string
            // Try to get a reference to the Shard Map Manager in the Shard Map Manager database. If it doesn't already exist, then create it.
            ShardMapManager shardMapManager;
            bool shardMapManagerExists = ShardMapManagerFactory.TryGetSqlShardMapManager(
                shardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy,
                out shardMapManager);

            if (shardMapManagerExists)
            {
                Console.WriteLine("Shard Map Manager already exists");
            }
            else
            {
                // The Shard Map Manager does not exist, so create it
                shardMapManager = ShardMapManagerFactory.CreateSqlShardMapManager(shardMapManagerConnectionString);
                Console.WriteLine("Created Shard Map Manager");
            }

            return shardMapManager;
        }

        /// <summary>
        /// Creates a new Range Shard Map with the specified name, or gets the Range Shard Map if it already exists.
        /// </summary>
        public static RangeShardMap<int> CreateOrGetRangeShardMap<T>(Sharding sharding,ShardMapManager shardMapManager, string shardMapName,int allowedMaxRangePerShard)
        {
            // Try to get a reference to the Shard Map.
            RangeShardMap<int> shardMap = null;
             
            Range<int> range = new Range<int>(0, allowedMaxRangePerShard);
            string shardMapName1 = shardMapName;
            if(shardMapManager.GetShardMaps().Count() == 0)
            {
                shardMapName = shardMapName + range.Low + "";
                // The Shard Map does not exist, so create it
                shardMap = shardMapManager.CreateRangeShardMap<int>(shardMapName);
                Console.WriteLine("Created Shard Map {0}", shardMap.Name);
            }
            else
            {
                shardMap = ShardManagementUtils.GetLastShardMap(sharding, shardMapName); //26/09/2022 //(RangeShardMap<int>)shardMapManager.GetShardMaps().OrderByDescending(x => x.Name).FirstOrDefault(); //.TryGetRangeShardMap(shardMapName, out shardMap);
                shardMapName = shardMap.Name;
            }

            if (shardMap!=null)
            {
                Shard shard = shardMap.GetShards().OrderByDescending(x => x.Location.Database).FirstOrDefault();
                Console.WriteLine("Shard Map {0} already exists", shardMap.Name);
                sharding.CrerateShardIfNotExists(shardMap,shard, allowedMaxRangePerShard, shardMapName1);
            }
            //else
            //{
            //    //// The Shard Map does not exist, so create it
            //    //shardMap = shardMapManager.CreateRangeShardMap<int>(shardMapName);
            //    //Console.WriteLine("Created Shard Map {0}", shardMap.Name);
            //}

            return shardMap;
        }
        public static RangeShardMap<int> GetLastShardMap(Sharding sharding, string shardMapeName)
        {  
            var smList = sharding.shardMapManager.GetShardMaps();
            var LastShard = (from s in smList select int.Parse(s.Name.Replace(shardMapeName, ""))).Max();
            shardMapeName = shardMapeName + LastShard; 
            return sharding.shardMapManager.GetRangeShardMap<int>(shardMapeName); 
        }

        /// <summary>
        /// Adds Shards to the Shard Map, or returns them if they have already been added.
        /// </summary>
        public static Shard CreateOrGetShard(ShardMap shardMap, ShardLocation shardLocation)
        {
            // Try to get a reference to the Shard
            Shard shard;
            bool shardExists = shardMap.TryGetShard(shardLocation, out shard);

            if (shardExists)
            {
                Console.WriteLine("Shard {0} has already been added to the Shard Map", shardLocation.Database);
            }
            else
            {
                // The Shard Map does not exist, so create it
                shard = shardMap.CreateShard(shardLocation);
                Console.WriteLine("Added shard {0} to the Shard Map", shardLocation.Database);
            }

            return shard;
        }
    }
}
