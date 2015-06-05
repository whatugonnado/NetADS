package kr.printf.netads;

import kr.printf.bolt.F5DetectRollingCountBolt;
import kr.printf.bolt.IntermediateCounter;
import kr.printf.bolt.F5TotalCounter;
import kr.printf.spout.F5DetectSpout;
import kr.printf.util.StormRunner;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class F5DetectTopology {

	private static final Logger LOG = Logger.getLogger(F5DetectTopology.class);
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
	
	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;

	public F5DetectTopology(String topologyName) throws InterruptedException {
		builder = new TopologyBuilder();
		this.topologyName = topologyName;
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

		wireTopology();
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(false);
		return conf;
	}

	private void wireTopology() throws InterruptedException {
		String spoutId = "F5DetectSpout";
		String counterId = "counter";
		String intermediateRankerId = "F5DetectIntermdeiateCounter";
		String totalRankerId = "finalCounter";

		builder.setSpout(spoutId, new F5DetectSpout(), 1);
		builder.setBolt(counterId, new F5DetectRollingCountBolt(), 1).shuffleGrouping(spoutId);
		//builder.setBolt(intermediateRankerId, new IntermediateCounter(), 1).shuffleGrouping(counterId);
		builder.setBolt(totalRankerId, new F5TotalCounter(), 1).shuffleGrouping(counterId);
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
		String topologyName = "F5DetectTopology";
		if (args.length >= 1) {
			topologyName = args[0];
		}
		boolean runLocally = true;
		if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
			runLocally = false;
		}
		LOG.info("Topology name: " + topologyName);
		F5DetectTopology htp = new F5DetectTopology(topologyName);
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
