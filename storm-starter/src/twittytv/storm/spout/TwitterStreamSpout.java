package twittytv.storm.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterStreamSpout extends BaseRichSpout {

	private SpoutOutputCollector _collector;
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		 _collector = collector;
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		 Utils.sleep(100);
		_collector.emit(new Values("mysentence"));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("word"));
	}

}
