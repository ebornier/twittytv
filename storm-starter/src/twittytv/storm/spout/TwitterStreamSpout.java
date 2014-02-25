package twittytv.storm.spout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.simple.JSONObject;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.common.collect.Lists;
import com.rapportive.storm.scheme.SimpleJSONScheme;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterStreamSpout extends BaseRichSpout {

	private SpoutOutputCollector _collector;
	private Client _client;
	private BlockingQueue<String> _messages;
	
	private Authentication getAuthentication(){
		return new OAuth1("hyvyVOB57V5lWmIfG2Baw",
				"aM5usMpeLDfy3sDCKkatn5AXZjY8kPoNPKSTAGWqnKY",
                 "1285567680-GMn9MGHiT9FvnwvhLvo1SavTyocd1lPl7ru4Yn0", 
                 "Xaq5Kowr9r4438SAO8eIWo52TelMqzsmlH3pr0f904fKB");
	}
	
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		System.out.println("opennn");
		// TODO Auto-generated method stub
		 _collector = collector;
		 
	    // Connect to the filter endpoint, tracking the term "twitterapi"
	    Hosts host = new HttpHosts(Constants.STREAM_HOST);
	    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
	    endpoint.trackTerms(Lists.newArrayList("bachelor"));
	    	  
	    // Drop in the oauth credentials for your app, available on dev.twitter.com
	    Authentication auth = getAuthentication(); 
	    	  
	    // Initialize a queue to collect messages from the stream
	    _messages = new LinkedBlockingQueue<String>(100000);
	    	  
	    // Build a client and read messages until the connection closes.
	    ClientBuilder builder = new ClientBuilder()
	    	    .name("TwittyTv")
	    	    .hosts(host)
	    	    .authentication(auth)
	    	    .endpoint(endpoint)
	    	    .processor(new StringDelimitedProcessor(_messages));
	    
	    _client = builder.build();
	    _client.connect();
	     
	}

	@Override
	public void nextTuple() {
		while (!_client.isDone()) {
			String message;
	    	try {
	    		message = _messages.take();
	    		
	    		SimpleJSONScheme jsonScheme = new SimpleJSONScheme();
	    		List<Object> jsonObjects = jsonScheme.deserialize(message.getBytes());
	    		JSONObject jsonObject = (JSONObject) jsonObjects.get(0);
	    		
	    		String text = (String) jsonObject.get("text");
	    		
	    		_collector.emit(new Values(text));
				
	    	
	    	
	    	} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
		System.exit(0);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("text"));
	}

}
