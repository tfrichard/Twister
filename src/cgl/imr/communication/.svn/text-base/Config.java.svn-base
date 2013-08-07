package cgl.imr.communication;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import cgl.imr.deployment.QuickDeployment;

import cgl.imr.config.TwisterConfigurations;

/**
 * nodes information with ID from -1(driver), 0 to daemon_num-1
 * 
 * @author zhangbj
 * 
 */
public class Config {
	private Map<Integer, NodeInfo> nodes;
	private int selfID;
	private static Config config;

	private Config() throws Exception {
		loadNodeInfoFromConfigFiles();
	}

	public static synchronized Config getInstance() throws Exception {
		if (config == null) {
			config = new Config();
		}
		return config;
	}

	private void loadNodeInfoFromConfigFiles() throws Exception {
		TwisterConfigurations configs = TwisterConfigurations.getInstance();
		int daemonPortBase = configs.getDaemonPortBase();
		int numDaemonsPerNode = configs.getDamonsPerNode();
		nodes = Collections
				.synchronizedSortedMap(new TreeMap<Integer, NodeInfo>());
		int daemonID = 0;
		String line = null;
		BufferedReader reader = new BufferedReader(new FileReader(
				configs.getNodeFile()));
		while ((line = reader.readLine()) != null) {
			for (int i = 0; i < numDaemonsPerNode; i++) {
				// port = daemonPortBase + daemonID
				nodes.put(daemonID, new NodeInfo(line, daemonPortBase
						+ daemonID));
				daemonID++;
			}
		}
		reader.close();
		reader = new BufferedReader(new FileReader(
				QuickDeployment.getTwisterHomePath() + "bin/driver_node"));
		line = reader.readLine();
		if (line != null) {
			// give driver a negative diff to daemonPortBase
			nodes.put(-1, new NodeInfo(line, daemonPortBase - 1));
		}
		reader.close();
	}

	public synchronized Map<Integer, NodeInfo> getNodesInfo() {
		return this.nodes;
	}

	public synchronized void setSelfID(int nodeID) {
		this.selfID = nodeID;
	}

	public synchronized int getSelfID() {
		return this.selfID;
	}
}
