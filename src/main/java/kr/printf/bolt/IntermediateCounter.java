package kr.printf.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import kr.printf.tools.NthLastModifiedTimeTracker;
import kr.printf.tools.SlidingWindowCounter;
import kr.printf.tools.SlotBasedCounter;
import kr.printf.util.TupleHelpers;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class IntermediateCounter extends BaseRichBolt {

	private static final long serialVersionUID = 12356876876L;
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 1;

	private static final Logger LOG = Logger
			.getLogger(IntermediateCounter.class);

	private final SlotBasedCounter<Object> clock;
	private final int emitFrequencyInSeconds;
	private OutputCollector collector;

	public IntermediateCounter() {
		this(DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}
	
	public IntermediateCounter(int emitFrequencyInSeconds) {
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		clock = new SlotBasedCounter<Object>(1);
	}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		if (TupleHelpers.isTickTuple(tuple)) {
			emitCurrentWindowCounts();
		} else {
			countObjAndAck(tuple);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "count"));
	}
	
	private void emitCurrentWindowCounts() {
		Map<Object, Long> counts = clock.getCounts();
		emit(counts);
	}

	private void emit(Map<Object, Long> counts) {
		for (Entry<Object, Long> entry : counts.entrySet()) {
			Object obj = entry.getKey();
			Long count = entry.getValue();
			collector.emit(new Values(obj, count));
		}
		clock.wipeSlot(0);
		clock.wipeZeros();
	}

	private void countObjAndAck(Tuple tuple) {
		Object obj = tuple.getValue(0);
		clock.addCount(obj, 0, (Long)(tuple.getValue(1)));
		collector.ack(tuple);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
		return conf;
	}

}
