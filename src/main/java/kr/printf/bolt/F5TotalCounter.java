package kr.printf.bolt;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class F5TotalCounter extends BaseRichBolt {

	private Jedis jedis;
	
	private static final Logger LOG = Logger
			.getLogger(F5TotalCounter.class);
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		jedis = new Jedis("125.209.196.135", 6379);
	}

	public void execute(Tuple input) {
		String key = input.getValue(0).toString();
		long count = (Long)(input.getValue(1));
		
		if(count > 10)
		{
			ObjectMapper mapper = new ObjectMapper();
			try {
				ObjectNode node = (ObjectNode)mapper.readTree(key);
				node.put("attack_type", "f5");
				String j = node.toString();
				jedis.rpush("event", j);
			} catch (Exception e){
			}
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
