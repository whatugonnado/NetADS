package kr.printf.spout;

import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class F5DetectSpout extends AbstractProtocolSpout {

	private static final long serialVersionUID = 11292847576L;
	private Jedis jedis;
	private static final Logger LOG = Logger.getLogger(F5DetectSpout.class);
	private SpoutOutputCollector _collector;
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		jedis = new Jedis("125.209.196.135", 6379);
		_collector = collector;
	}

	public void nextTuple() {
		String event = jedis.rpop("http");
		if(event == null)
			return;
		_collector.emit(new Values(event));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key"));
	}

}
