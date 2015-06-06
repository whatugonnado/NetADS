package kr.printf.spout;

import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import kr.printf.packet.Protocol;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ARPSpout extends AbstractProtocolSpout {

	private static final long serialVersionUID = 84865153L;
	private Jedis jedis;
	private static final Logger LOG = Logger.getLogger(ARPSpout.class);
	private SpoutOutputCollector _collector;
	
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub

		jedis = new Jedis("125.209.196.135", 6379);
		_collector = collector;
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		
		String event = jedis.rpop("http");
		if(event == null)
			return;
		_collector.emit(new Values(event));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
			declarer.declare(new Fields("key"));
	}
}
