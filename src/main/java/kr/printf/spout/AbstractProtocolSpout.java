package kr.printf.spout;

import java.util.Map;

import kr.printf.packet.Protocol;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

public abstract class AbstractProtocolSpout extends BaseRichSpout {

	abstract public void open(Map conf, TopologyContext context,
							  SpoutOutputCollector collector);

	abstract public void nextTuple();

	abstract public void declareOutputFields(OutputFieldsDeclarer declarer);
}
