package kr.printf.bolt;

import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class HTTPTotalCounter extends BaseRichBolt {

private Jedis jedis;
	
	private static final Logger LOG = Logger
			.getLogger(HTTPTotalCounter.class);
	
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
				node.put("attack_type", "login");
				String j = node.toString();
				jedis.rpush("event", j);
				System.out.println(j);
			} catch (Exception e){
			}
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
