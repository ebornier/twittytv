package twittytv.storm.topology;

import java.util.HashMap;
import java.util.Map;

import twittytv.storm.spout.TwitterStreamSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.CassandraBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;

/**
 * This is a basic example of a Storm topology.
 */
public class TwitterTvTopology {

    public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("worda", new TwitterStreamSpout(), 1);
    //builder.setBolt("exclaim1", new NormalizeTweetBolt(), 3).shuffleGrouping("worda");
    //builder.setBolt("exclaim2", new NormalizeTweetBolt(), 2).shuffleGrouping("exclaim1");

    TupleMapper<String, String, String> tupleMapper = new DefaultTupleMapper(StormCassandraConstants.CASSANDRA_KEYSPACE, "users", "VALUE");
    String configKey = "cassandra-config";
    CassandraBatchingBolt<String, String, String> cassandraBolt = new CassandraBatchingBolt<String, String, String>(configKey, tupleMapper);
    builder.setBolt("cassandra", cassandraBolt, 1).shuffleGrouping("worda");	
    
    
    Config conf = new Config();
   
    //Cassandra configuration
    Map<String, Object> cassandraConfig = new HashMap<String, Object>();
    cassandraConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
    cassandraConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, "testKeyspace");
    conf.put(configKey, cassandraConfig);    
    //end - Cassandra configuration
    
    
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
    	
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
