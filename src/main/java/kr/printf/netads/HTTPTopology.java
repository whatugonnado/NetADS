package kr.printf.netads;

import kr.printf.bolt.HTTPDetectRollingCountBolt;
import kr.printf.bolt.HTTPTotalCounter;
import kr.printf.bolt.IntermediateCounter;
import kr.printf.spout.HTTPDetectSpout;
import kr.printf.util.StormRunner;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class HTTPTopology {

	private static final Logger LOG = Logger.getLogger(HTTPTopology.class);
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
	
	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;

	public HTTPTopology(String topologyName) throws InterruptedException {
		builder = new TopologyBuilder();
		this.topologyName = topologyName;
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

		wireTopology();
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
//		conf.setDebug(true);
		conf.setDebug(false);
		return conf;
	}

	private void wireTopology() throws InterruptedException {
		String spoutId = "HTTPDetectSpout";
		String counterId = "counter";
		String intermediateRankerId = "HTTPDetectIntermdeiateCounter";
		String totalRankerId = "finalCounter";

		builder.setSpout(spoutId, new HTTPDetectSpout(), 1);
		builder.setBolt(counterId, new HTTPDetectRollingCountBolt(), 1).shuffleGrouping(spoutId);
		builder.setBolt(totalRankerId, new HTTPTotalCounter(), 1).shuffleGrouping(counterId);
	}

	public void runLocally() throws InterruptedException {
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName,
				topologyConfig, runtimeInSeconds);
	}

	public void runRemotely() throws Exception {
		StormRunner.runTopologyRemotely(builder.createTopology(), topologyName,
				topologyConfig);
	}

	public static void main(String[] args) throws Exception {
		String topologyName = "HTTPDetectTopology";
		if (args.length >= 1) {
			topologyName = args[0];
		}
		boolean runLocally = true;
		if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
			runLocally = false;
		}
		LOG.info("Topology name: " + topologyName);
		HTTPTopology htp = new HTTPTopology(topologyName);
		if(runLocally) {
			LOG.info("Running in local mode");
			htp.runLocally();
		}
		else {
			LOG.info("Running in remote mode");
			htp.runRemotely();
		}
	}
}
