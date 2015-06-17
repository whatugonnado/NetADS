package kr.printf.netads;

import kr.printf.bolt.ARPRollingCountBolt;
import kr.printf.bolt.ARPTotalBolt;
import kr.printf.spout.ARPSpout;
import kr.printf.util.StormRunner;

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
	    
	    wireTopology();
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(true);
		return conf;
	}
	
	private void wireTopology(){
	    String spoutId = "arpspout";
	    String counterId = "arpcount";
	    //String intermediatId = "arpintermediate";
	    String totalid = "arptotal";
	    
	    builder.setSpout(spoutId, new ARPSpout(), 1);
	    builder.setBolt(counterId, new ARPRollingCountBolt(), 1);
	    builder.setBolt(totalid, new ARPTotalBolt(), 1).shuffleGrouping(counterId);
	}
	
	public void runLocally() throws InterruptedException{
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName, 
				topologyConfig, runtimeInSeconds);
	}
	
	public void RunRemotely() throws Exception{
		StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, 
				topologyConfig);
	}
	
	public static void main(String[] args) throws Exception {
		String topologyName = "ARPTopology";
		if (args.length >= 1) {
			topologyName = args[0];
		}
		boolean runLocally = true;
		if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
			runLocally = false;
		}
		LOG.info("Topology name: " + topologyName);

		ARPTopology arp = new ARPTopology();
		if(runLocally){
			LOG.info("Running in local mode");
			arp.runLocally();
		}
		else{
			LOG.info("Running in remote mode");
			arp.RunRemotely();
		}
	}
}
