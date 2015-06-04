package kr.printf.netads;

import org.apache.log4j.Logger;

public class HTTPTopology {

	private static final Logger LOG = Logger.getLogger(HTTPTopology.class);
	
	public static void main(String[] args) {
		String topologyName = "HTTPTopology";
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
