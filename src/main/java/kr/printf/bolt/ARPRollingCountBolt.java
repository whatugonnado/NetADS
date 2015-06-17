package kr.printf.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import kr.printf.packet.ARP;
import kr.printf.tools.NthLastModifiedTimeTracker;
import kr.printf.tools.SlidingWindowCounter;
import kr.printf.util.TupleHelpers;

import org.apache.log4j.Logger;
import org.mortbay.util.ajax.JSON;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ARPRollingCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = Logger.getLogger(ARPRollingCountBolt.class);
	private static final int NUM_WINDOW_CHUNKS = 10;
	private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS;
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS
			/ NUM_WINDOW_CHUNKS;
	private static final String WINDOW_LENGTH_WARNING_TEMPLATE = "Actual window length is %d seconds when it should be %d seconds"
			+ " (you can safely ignore this warning during the startup phase)";

	private final SlidingWindowCounter<Object> counter;
	private final int windowLengthInSeconds;
	private final int emitFrequencyInSeconds;
	private OutputCollector collector;
	private NthLastModifiedTimeTracker lastModifiedTracker;
	
	private final Map<Object, Object> FirstParamForObj;
	
	public ARPRollingCountBolt() {
		this(DEFAULT_SLIDING_WINDOW_IN_SECONDS,
				DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}

	public ARPRollingCountBolt(int windowLengthInSeconds, 
			int emitFrequencyInSeconds) {
		this.windowLengthInSeconds = windowLengthInSeconds;
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		counter = new SlidingWindowCounter<Object>(deriveNumWindowChunksFrom(
				this.windowLengthInSeconds, this.emitFrequencyInSeconds));
	
		FirstParamForObj = new HashMap<Object, Object>();
	}

	private int deriveNumWindowChunksFrom(int windowLengthInSeconds,
			int windowUpdateFrequencyInSeconds) {
		return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		lastModifiedTracker = new NthLastModifiedTimeTracker(
				deriveNumWindowChunksFrom(this.windowLengthInSeconds,
						this.emitFrequencyInSeconds));
	
	}

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		if(TupleHelpers.isTickTuple(tuple)){
			emitCurrentWindowCounts();
		}
		else{
			countObjAndAck(tuple);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("key", "count"));
	}

	private void emitCurrentWindowCounts() {
		Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
		emit(counts);
	}

	private void emit(Map<Object, Long> counts) {
		for (Entry<Object, Long> entry : counts.entrySet()) {
			Object obj = entry.getKey();
			Long count = entry.getValue();
			
			Map<String, Object> packet = (Map<String, Object>)JSON.parse((String)obj);
			packet.put("src_mac", FirstParamForObj.get(obj));
			
			if(count==0){
				FirstParamForObj.remove(obj);
				continue;
			}
			
			obj = JSON.toString(packet);
			
			collector.emit(new Values(obj, count));
		}
	}

	private void countObjAndAck(Tuple tuple) {
		Object obj = tuple.getValue(0);
		counter.incrementCount(obj);
		
		Object param = tuple.getValue(1);
		if(!FirstParamForObj.containsKey(obj))
			FirstParamForObj.put(obj, param);
		
		collector.ack(tuple);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 0.2);
		return conf;
	}

}
