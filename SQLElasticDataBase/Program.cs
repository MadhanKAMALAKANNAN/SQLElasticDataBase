// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//Modified: Madhan KAMALAKANNAN, 20/09/2022
using System;
using System.Data.SqlClient;
using System.Linq;
using SQLElasticDataBase;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Extensions.Configuration;
 
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
////////////////////////////////////////////////////////////////////////////////////////
// This sample follows the CodeFirstNewDatabase Blogging tutorial for EF.
// It illustrates the adjustments that need to be made to use EF in combination
// with the Entity Framewor to scale out  data tier across many databases and
// benefit from Elastic Scale capabilities for Data Dependent Routing and 
// Shard Map Management.
////////////////////////////////////////////////////////////////////////////////////////

namespace SQLElasticDataBase
{
    // This sample requires three pre-created empty SQL Server databases. 
    // The first database serves as the shard map manager database to store the Elastic Scale shard map.
    // The remaining two databases serve as shards to hold the data for the sample.
    public class Program
    {
        // You need to adjust the following settings to  database server and database names in Azure Db
        static string s_server = String.Empty;
         static string s_shardmapmgrdb = String.Empty;
        static string s_shard1 = String.Empty;
        static string s_shard2 = String.Empty;
        static string s_userName = String.Empty;
        static string s_password = String.Empty;
        static string s_applicationName = "s_applicationName";
        static string connectionString = String.Empty;
        static IConfiguration config = null;
        // Just two tenants for now.
        // Those we will allocate to shards.
        private static int s_tenantId1 = 05;
        private static int s_tenantId2 = 12;

        public static void Main()
        {
         
            config = SqlDatabaseUtils.GetConfigaration();
            Console.WriteLine($"connectionString: {connectionString}");
            connectionString = config["connectionString"];
           s_server = config["SQLServerName"];
           s_shardmapmgrdb = config["ShardMapManagerDatabaseName"];
           s_shard1 = config["Shard01DatabaseName"];
           s_shard2 = config["Shard02DatabaseName"];
           s_userName = config["sqlUserName"];
           s_password = config["sqlPassword"];
           s_applicationName = config["AppName"];

            int allowedMaxRangePerShard = int.Parse(config["allowedMaxRangePerShard"]);
            SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder
            {
                UserID = s_userName,
                Password = s_password,
                ApplicationName = s_applicationName
            };
            // Do work for tenant 1 :-)
            // Bootstrap the shard map manager, register shards, and store mappings of tenants to shards
            // Note that you can keep working with existing shard maps. There is no need to 
            // re-create and populate the shard map from scratch every time.
            Console.WriteLine("Checking for existing shard map and creating new shard map if necessary.");
            
            SqlDatabaseUtils.CreateDatabase(s_server,s_shardmapmgrdb);
            //SqlDatabaseUtils.CreateDatabase(s_server, s_shard1);
            //SqlDatabaseUtils.CreateDatabase(s_server, s_shard2);
            do
            {
                Sharding sharding = new Sharding(s_server, s_shardmapmgrdb, connectionString, true);

                // Console.WriteLine("Enter a name for a new Blog: ");
                var name = "Blog_"; 
                string shardMapeName = config["Shard00DatabaseName"];
                  
                sharding.shardRangeMap = ShardManagementUtils.GetLastShardMap(sharding, shardMapeName); //26/09/2022
                Shard shard = sharding.shardRangeMap.GetShards().OrderByDescending(x => x.Location.Database).FirstOrDefault();
              
                //SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder(config["connectionString"]);
                connStrBldr.DataSource = shard.Location.DataSource;
                connStrBldr.InitialCatalog = shard.Location.Database;

                int maxRange = 0;
                // Go into a DbContext to trigger migrations and schema deployment for the new shard.
                // This requires an un-opened connection.
                using (var db = new ElasticScaleContext<int>(connStrBldr.ConnectionString))
                {
                    int cnt = (from b in db.Blogs select b).Count();
                    if (cnt > 0)
                    { maxRange = (from b in db.Blogs select b).Max(m => m.BlogId); }
                }
                connStrBldr = new SqlConnectionStringBuilder
                {
                    UserID = s_userName,
                    Password = s_password,
                    ApplicationName = s_applicationName

                };
                SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
                {
                    int currentMaxHighKey = sharding.shardRangeMap.GetMappings().Max(m => m.Value.High);
               
                    int startRange = currentMaxHighKey - allowedMaxRangePerShard + maxRange + 1;
                    try
                    {
                        using (var db = new ElasticScaleContext<int>(sharding.shardRangeMap, startRange, connStrBldr.ConnectionString))
                        {
                            for (int i = startRange; i <= currentMaxHighKey; i++)
                            {

                                var blog = new Blog { BlogId = i, Name = name + i };
                                db.Blogs.Add(blog);
                                db.SaveChanges();

                            } 
                            // Display all Blogs for tenant 1
                            var query = from b in db.Blogs orderby b.Name select b;

                            Console.WriteLine("All blogs for tenant id {0}:", maxRange);
                            foreach (var item in query)
                            {
                                Console.WriteLine(item.Name);
                            }
                        }
                    }
                    catch (Exception ex)//catch exception if range out shardmapRange
                    {
                        //Create new range
                        Shard shard = sharding.shardRangeMap.GetShards().OrderByDescending(x => x.Location.Database).FirstOrDefault();
                        sharding.CrerateShardIfNotExists(sharding.shardRangeMap, shard, allowedMaxRangePerShard, config["Shard00DatabaseName"]);
                    }

                      
                    
                });

                //SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
                //{
                //    using (var db = new ElasticScaleContext<int>(sharding.shardRangeMap, maxRange, connStrBldr.ConnectionString))
                //    {
                //        // Display all Blogs for tenant 1
                //        var query = from b in db.Blogs
                //                    orderby b.Name
                //                    select b;

                //        Console.WriteLine("All blogs for tenant id {0}:", maxRange);
                //        foreach (var item in query)
                //        {
                //            Console.WriteLine(item.Name);
                //        }
                //    }
                //});

                //// Do work for tenant 2 :-)
                //SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
                //{
                //    using (var db = new ElasticScaleContext<int>(sharding.shardRangeMap, s_tenantId2, connStrBldr.ConnectionString))
                //    {
                //        // Display all Blogs from the database 
                //        var query = from b in db.Blogs
                //                    orderby b.Name
                //                    select b;

                //        Console.WriteLine("All blogs for tenant id {0}:", s_tenantId2);
                //        foreach (var item in query)
                //        {
                //            Console.WriteLine(item.Name);
                //        }
                //    }
                //});

                //// Create and save a new Blog 
                //Console.Write("Enter a name for a new Blog: ");
                //var name2 = Console.ReadLine();

                //SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
                //{
                //    using (var db = new ElasticScaleContext<int>(sharding.shardRangeMap, s_tenantId2, connStrBldr.ConnectionString))
                //    {
                //        var blog = new Blog { Name = name2 };
                //        db.Blogs.Add(blog);
                //        db.SaveChanges();
                //    }
                //});

                //SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
                //{
                //    using (var db = new ElasticScaleContext<int>(sharding.shardRangeMap, s_tenantId2, connStrBldr.ConnectionString))
                //    {
                //        // Display all Blogs from the database 
                //        var query = from b in db.Blogs
                //                    orderby b.Name
                //                    select b;

                //        Console.WriteLine("All blogs for tenant id {0}:", s_tenantId2);
                //        foreach (var item in query)
                //        {
                //            Console.WriteLine(item.Name);
                //        }
                //    }
                //});

                Console.WriteLine("Press y to continue else exit  ...");
            }while(Console.ReadKey().KeyChar == 'y');
        }
    }
}
