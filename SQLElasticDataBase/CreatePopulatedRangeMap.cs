using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLElasticDataBase
{
    internal class CreatePopulatedRangeMap
    {
        public CreatePopulatedRangeMap(ShardMapManager smm, string mapName,string shardServer)
        {
            RangeShardMap<long> sm = null;

            // check if shardmap exists and if not, create it 
            if (!smm.TryGetRangeShardMap(mapName, out sm))
            {
                sm = smm.CreateRangeShardMap<long>(mapName);
            }

            Shard shard0 = null, shard1 = null;
            // check if shard exists and if not, 
            // create it (Idempotent / tolerant of re-execute) 
            if (!sm.TryGetShard(new ShardLocation(shardServer, "SQLElasticDataBaseShard0"), out shard0))
            {
                shard0 = sm.CreateShard(new ShardLocation(shardServer, "SQLElasticDataBaseShard0"));
            }

            if (!sm.TryGetShard(new ShardLocation(shardServer, "SQLElasticDataBaseShard1"), out shard1))
            {
                shard1 = sm.CreateShard(new ShardLocation(shardServer, "SQLElasticDataBaseShard1"));
            }

            RangeMapping<long> rmpg = null;

            // Check if mapping exists and if not,
            // create it (Idempotent / tolerant of re-execute) 
            if (!sm.TryGetMappingForKey(0, out rmpg))
            {
                sm.CreateRangeMapping(new RangeMappingCreationInfo<long>
                    (new Range<long>(0, 50), shard0, MappingStatus.Online));
            }

            if (!sm.TryGetMappingForKey(50, out rmpg))
            {
                sm.CreateRangeMapping(new RangeMappingCreationInfo<long>
                    (new Range<long>(50, 100), shard1, MappingStatus.Online));
            }

            if (!sm.TryGetMappingForKey(100, out rmpg))
            {
                sm.CreateRangeMapping(new RangeMappingCreationInfo<long>
                    (new Range<long>(100, 150), shard0, MappingStatus.Online));                                 
            }

            if (!sm.TryGetMappingForKey(150, out rmpg))
            {
                sm.CreateRangeMapping(new RangeMappingCreationInfo<long>
                    (new Range<long>(150, 200), shard1, MappingStatus.Online));
            }

            if (!sm.TryGetMappingForKey(200, out rmpg))
            {
                sm.CreateRangeMapping(new RangeMappingCreationInfo<long>
                    (new Range<long>(200, 300), shard0, MappingStatus.Online));
            }

            // List the shards and mappings 
            foreach (Shard s in sm.GetShards()
                                    .OrderBy(s => s.Location.DataSource)
                                    .ThenBy(s => s.Location.Database))
            {
                Console.WriteLine("shard: " + s.Location);
                
            }

            foreach (RangeMapping<long> rm in sm.GetMappings())
            {
                Console.WriteLine("range: [" + rm.Value.Low.ToString() + ":"
                        + rm.Value.High.ToString() + ")  ==>" + rm.Shard.Location);
            }
        }
    }
}
