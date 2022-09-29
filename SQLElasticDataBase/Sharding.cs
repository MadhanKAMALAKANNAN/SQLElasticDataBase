// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//Modified: Madhan KAMALAKANNAN, 20/09/2022
 
using System.Data.SqlClient;
using System.Linq;
 
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Extensions.Configuration;
using Microsoft.ServiceBus;

namespace SQLElasticDataBase
{
    public class Sharding
    {
        public ShardMapManager shardMapManager { get; private set; }

        public ListShardMap<int> shardMap { get;  set; }
        public RangeShardMap<int> shardRangeMap { get;  set; }
        /// <summary>
        /// The shard map manager, or null if it does not exist. 
        /// It is recommended that you keep only one shard map manager instance in
        /// memory per AppDomain so that the mapping cache is not duplicated.
        /// </summary>

        static IConfiguration config = null;
        // Bootstrap Elastic Scale by creating a new shard map manager and a shard map on 
        // the shard map manager database if necessary.
        public Sharding()
        {

        }
        public Sharding(string smmserver, string smmdatabase, string smmconnstr,bool iSRangeShardMap)//Modified
        {
            config = SqlDatabaseUtils.GetConfigaration();
            // Connection string with administrative credentials for the root database
            SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder(smmconnstr);
            connStrBldr.DataSource = smmserver;
            connStrBldr.InitialCatalog = smmdatabase;

            // Deploy shard map manager.
            ShardMapManager smm;
            if (!ShardMapManagerFactory.TryGetSqlShardMapManager(connStrBldr.ConnectionString, ShardMapManagerLoadPolicy.Lazy, out smm))
            {
                this.shardMapManager = ShardMapManagerFactory.CreateSqlShardMapManager(connStrBldr.ConnectionString);
            }
            else
            {
                this.shardMapManager = smm;
            }

            if(!iSRangeShardMap)
            {
                ListShardMap<int> sm;
                if (!shardMapManager.TryGetListShardMap<int>("SQLElasticDataBaseListShardMap", out sm))
                {
                    this.shardMap = shardMapManager.CreateListShardMap<int>("SQLElasticDataBaseListShardMap");
                }
                else
                {
                    this.shardMap = sm;
                }
            }
            else
            {
                // Create shard map
                Range<int> range = null;
                int allowedMaxRangePerShard = 5000;
                allowedMaxRangePerShard = int.Parse(config["allowedMaxRangePerShard"]);
                
                shardRangeMap = ShardManagementUtils.CreateOrGetRangeShardMap<int>(this,shardMapManager, config["Shard00DatabaseName"], allowedMaxRangePerShard);
  
                if (!shardRangeMap.GetShards().Any())//if no shard yet
                {
                    range = new Range<int>(0, allowedMaxRangePerShard);
                    CreateRangeShard.CreateShard(shardRangeMap, range, config["Shard00DatabaseName"]);
                }
                else //check if need to create new shard and get the new range
                {
                    Shard shard = shardRangeMap.GetShards().OrderByDescending(x => x.Location.Database).FirstOrDefault();//only one shard per shardMap;
                    //checked if shard with above range exist
                    //int currentMinHighKey = shardRangeMap.GetMappings().Max(m => m.Value.Low) ;//22/09/2022
                    CrerateShardIfNotExists(shardRangeMap,shard, allowedMaxRangePerShard, config["Shard00DatabaseName"]);
                }

            }
        }
        public void CrerateShardIfNotExists(RangeShardMap<int> shardRangeMap,Shard shard,int allowedMaxRangePerShard, string shardMapName)
        {
            if (shardRangeMap != null && shard != null)
            {
                SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder(config["connectionString"]);
                connStrBldr.DataSource = shard.Location.DataSource;
                connStrBldr.InitialCatalog = shard.Location.Database;
                int maxRange = 0, minRange = 0;
                // Go into a DbContext to trigger migrations and schema deployment for the new shard.
                // This requires an un-opened connection.
                int cnt = 0;
                using (var db = new ElasticScaleContext<int>(connStrBldr.ConnectionString))
                {
                    cnt = (from b in db.Blogs select b).Count();
                    if (cnt > 0)
                    {
                        maxRange = (from b in db.Blogs select b).Max(m => m.BlogId);
                        minRange = (from b in db.Blogs select b).Min(m => m.BlogId);
                    }
                }
                int currentMaxHighKey = shardRangeMap.GetMappings().Max(m => m.Value.High);
                if (cnt >= allowedMaxRangePerShard) //create new shard//maxRange >= allowedMaxRangePerShard)//create new shard
                {

                    Range<int> range = new Range<int>(currentMaxHighKey, currentMaxHighKey + allowedMaxRangePerShard);
                    shardMapName = shardMapName + range.Low + "";
                    RangeShardMap<int> outshardMap;
                    // if the Shard Map does not exist, so create it
                    if (!shardMapManager.TryGetRangeShardMap<int>(shardMapName, out outshardMap))
                    {
                        shardRangeMap = shardMapManager.CreateRangeShardMap<int>(shardMapName);
                        CreateRangeShard.CreateShard(shardRangeMap, range, config["Shard00DatabaseName"]);
                    }

                }
            }
        }

        // Enter a new shard - i.e. an empty database - to the shard map, allocate a first tenant to it 
        // and kick off EF intialization of the database to deploy schema
        // public void RegisterNewShard(string server, string database, string user, string pwd, string appname, int key)
        //public void RegisterNewShard(string server, string database, string connstr, int key)
        //{
        //    Shard shard;
        //    ShardLocation shardLocation = new ShardLocation(server, database);

        //    if (!this.shardMap.TryGetShard(shardLocation, out shard))
        //    {
        //        shard = this.shardMap.CreateShard(shardLocation);
        //    }

        //    SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder(connstr);
        //    connStrBldr.DataSource = server;
        //    connStrBldr.InitialCatalog = database;

        //    // Go into a DbContext to trigger migrations and schema deployment for the new shard.
        //    // This requires an un-opened connection.
        //    using (var db = new ElasticScaleContext<int>(connStrBldr.ConnectionString))
        //    {
        //        // Run a query to engage EF migrations
        //        (from b in db.Blogs
        //         select b).Count();
        //    }

        //    // Register the mapping of the tenant to the shard in the shard map.
        //    // After this step, DDR on the shard map can be used
        //    PointMapping<int> mapping;
        //    if (!this.shardMap.TryGetMappingForKey(key, out mapping))
        //    {
        //        this.shardMap.CreatePointMapping(key, shard);
        //    }
        //}
    }
}
