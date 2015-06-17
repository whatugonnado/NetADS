package kr.printf.spout;

import java.util.Map;

import org.apache.log4j.Logger;
import org.mortbay.util.ajax.JSON;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class HTTPDetectSpout extends AbstractProtocolSpout {

	private static final long serialVersionUID = 11292847576L;
	private Jedis jedis;
	private static final Logger LOG = Logger.getLogger(HTTPDetectSpout.class);
	private SpoutOutputCollector _collector;
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		jedis = new Jedis("125.209.196.135", 6379);
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		
		String event = jedis.rpop("http-post");
		
		if(event == null)
			return;
		
		@SuppressWarnings("unchecked")
		Map<String,Object> packet = (Map<String,Object>)JSON.parse(event);
		 
		String path = (String) packet.get("path");
		
		//post 패킷을 받아 로그인 요청인지 판단해서
		// 로그인 요청이 아니면  drop, 로그인 요청이라면  emit
		if(!path.toLowerCase().contains("login") && !path.toLowerCase().contains("logon") && !path.toLowerCase().contains("signin"))
			return;
		
		//request body 떼 버리면 같은 word가 될 것.
		String param =(String)packet.get("request_body");
		packet.remove("request_body");
		
		//JSON->String (다시 key로 만든다)
		event = JSON.toString(packet);
		
		_collector.emit(new Values(event, param));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("key", "param"));
	}

}
