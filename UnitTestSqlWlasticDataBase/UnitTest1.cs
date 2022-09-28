//Create: Madhan KAMALAKANNAN, 26/09/2022

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SQLElasticDataBase;
using System.Data.SqlClient;
using System.Runtime.Intrinsics.X86;

namespace UnitTestSqlWlasticDataBase
{
    [TestClass]
    public class UTestSqlWlasticDataBase
    {
        int allowedMaxRangePerShard = 0;
        static string s_server = String.Empty;
        static string s_shardmapmgrdb = String.Empty;
        static string s_shard1 = String.Empty;
        static string s_shard2 = String.Empty;
        static string s_userName = String.Empty;
        static string s_password = String.Empty;
        static string s_applicationName = "s_applicationName";
        static string connectionString = String.Empty;
        static IConfiguration config = null;
        static string shardMapeName = String.Empty;

        [TestMethod]
        public void TestConfigDetails()
        {
             config =  SqlDatabaseUtils.GetConfigaration();
            Assert.IsNotNull(config);

            s_server = config["SQLServerName"];
            s_shardmapmgrdb = config["ShardMapManagerDatabaseName"];
            s_shard1 = config["Shard01DatabaseName"];
            s_shard2 = config["Shard02DatabaseName"];
            s_userName = config["sqlUserName"];
            s_password = config["sqlPassword"];
            s_applicationName = config["AppName"];
            connectionString = config["connectionString"];
            shardMapeName = config["Shard00DatabaseName"];
            allowedMaxRangePerShard = int.Parse(config["allowedMaxRangePerShard"]);
           
            Assert.IsTrue(s_server != String.Empty);
            Assert.IsTrue(s_shardmapmgrdb != String.Empty);
            Assert.IsTrue(s_shard1 != String.Empty);
            Assert.IsTrue(s_shard2 != String.Empty); 
            Assert.IsTrue(s_userName != String.Empty);
            Assert.IsTrue(s_password != String.Empty);
            Assert.IsTrue(s_applicationName != String.Empty);
            Assert.IsTrue(connectionString != String.Empty);
            Assert.IsTrue(shardMapeName != String.Empty);
            Assert.IsTrue(allowedMaxRangePerShard!=0);
        } 

        [TestMethod]
        public void TestSqlElasticDBCreation()
        {
            config = SqlDatabaseUtils.GetConfigaration();
            string s_server = config["SQLServerName"];
            string s_shardmapmgrdb = config["ShardMapManagerDatabaseName"];
            Assert.IsTrue(SqlDatabaseUtils.CreateDatabase(s_server, s_shardmapmgrdb) == "done");

        }
        [TestMethod]
        public void TestSqlElasticRangeShards()
        {
             config = SqlDatabaseUtils.GetConfigaration(); 
             allowedMaxRangePerShard = int.Parse(config["allowedMaxRangePerShard"]);
            s_server = config["SQLServerName"];
            s_shardmapmgrdb = config["ShardMapManagerDatabaseName"];
            s_shard1 = config["Shard01DatabaseName"];
            s_shard2 = config["Shard02DatabaseName"];
            s_userName = config["sqlUserName"];
            s_password = config["sqlPassword"];
            s_applicationName = config["AppName"];
        
            shardMapeName = config["Shard00DatabaseName"];
            allowedMaxRangePerShard = int.Parse(config["allowedMaxRangePerShard"]);

            var iterations = 0;
            do
            {
                Sharding sharding = new Sharding(config["SQLServerName"], config["ShardMapManagerDatabaseName"], config["connectionString"], true);
                var name = "Blog_";
                Assert.IsInstanceOfType(sharding, typeof(Sharding));

                sharding.shardRangeMap = ShardManagementUtils.GetLastShardMap(sharding, config["Shard00DatabaseName"]); //26/09/2022
                Assert.IsNotNull(sharding.shardRangeMap);
                Shard shard = sharding.shardRangeMap.GetShards().OrderByDescending(x => x.Location.Database).FirstOrDefault();
                Assert.IsNotNull(shard);

                SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder(connectionString);
                Assert.IsNotNull(connStrBldr);
                connStrBldr.DataSource = shard.Location.DataSource;
                connStrBldr.InitialCatalog = shard.Location.Database;
                Assert.IsTrue(connStrBldr.InitialCatalog.Contains(config["Shard00DatabaseName"]));
                int maxRange = 0; int cnt = 0;


                // Go into a DbContext to trigger migrations and schema deployment for the new shard.
                // This requires an un-opened connection.
                var db = new ElasticScaleContext<int>(connStrBldr.ConnectionString);
                {
                    cnt = (from b in db.Blogs select b).Count();

                    if (cnt > 0)
                    { maxRange = (from b in db.Blogs select b).Max(m => m.BlogId); }

                }


                Assert.IsNotNull(db);
                //Assert.AreEqual(cnt,allowedMaxRangePerShard);
                //Assert.


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
                        //Assert.IsTrue(startRange > currentMaxHighKey);
                        using (var db = new ElasticScaleContext<int>(sharding.shardRangeMap, startRange, connStrBldr.ConnectionString))
                        {
                            for (int i = startRange; i <= currentMaxHighKey; i++)
                            {

                                var blog = new Blog { BlogId = i, Name = name + i };
                                db.Blogs.Add(blog);
                                db.SaveChanges();

                            }
                            // Display all Blogs for tenant 1
                            var itemCnt = (from b in db.Blogs orderby b.Name select b).Count();
                            Assert.AreEqual(allowedMaxRangePerShard, itemCnt);

                        }
                    }
                    catch (Exception ex)//catch exception if range out shardmapRange
                    {
                        //Create new range
                        Shard shard = sharding.shardRangeMap.GetShards().OrderByDescending(x => x.Location.Database).FirstOrDefault();
                        sharding.CrerateShardIfNotExists(sharding.shardRangeMap, shard, int.Parse(config["allowedMaxRangePerShard"]), config["Shard00DatabaseName"]);
                        Assert.IsInstanceOfType(shard, typeof(Shard));
                    }

                });
                iterations++;
            } while (iterations <= 5);
        }
         
    }
}