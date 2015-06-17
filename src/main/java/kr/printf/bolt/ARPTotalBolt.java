package kr.printf.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import redis.clients.jedis.Jedis;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ARPTotalBolt extends BaseRichBolt {

	private Jedis jedis;
	
	private static final Logger LOG = Logger.getLogger(ARPTotalBolt.class);
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
	private static final int DEFAULT_COUNT = 10;

	private final int emitFrequencyInSeconds = 0;
	private final int count = 0;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub

		jedis = new Jedis("125.209.196.135", 6379);
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String key = input.getValue(0).toString();
		long count = (Long)	(input.getValue(1));
		
		if(count > 10){
			ObjectMapper mapper = new ObjectMapper();
			try {
				ObjectNode node = (ObjectNode)mapper.readTree(key);
				node.put("attack_type", "arp");
				String j = node.toString();
				jedis.rpush("event", j);
			}
			catch (Exception e){
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}
	

	
}
