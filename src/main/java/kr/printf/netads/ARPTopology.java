package kr.printf.netads;

import kr.printf.bolt.ARPRollingCountBolt;
import kr.printf.spout.ARPSpout;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class ARPTopology {

	private static final String topologyName = "arptopology";
	private static final Logger LOG = Logger.getLogger(ARPTopology.class);
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
	private static final int TOP_N = 5;

	private final TopologyBuilder builder;
	private final Config topologyConfig;
	private final int runtimeInSeconds;

	public ARPTopology() {
		builder = new TopologyBuilder();
	    topologyConfig = createTopologyConfiguration();
	    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(true);
		return conf;
	}
	
	private void wireTopology(){
	    String spoutId = "arpspout";
	    String counterId = "arpcount";
	    String intermediatId = "arpintermediate";
	    String totalid = "arptotal";
	    
	    builder.setSpout(spoutId, new ARPSpout(), 3);
	    builder.setBolt(counterId, new ARPRollingCountBolt(), 4);
	}
	
	public static void main(String[] args) {
		String topologyName = "ARPTopology";
		if (args.length >= 1) {
			topologyName = args[0];
		}
		boolean runLocally = true;
		if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
			runLocally = false;
		}
		LOG.info("Topology name: " + topologyName);
	}
}
