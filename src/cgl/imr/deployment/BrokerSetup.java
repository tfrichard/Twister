package cgl.imr.deployment;

import java.util.ArrayList;
import java.util.List;

abstract class BrokerSetup {
	private int brokerNum;
	private String driverBroker;
	private List<String> nodeBrokers; // broker node IPs
	private TwisterNodes nodes;
	private boolean isBrokerOnNFS;

	BrokerSetup() {
		this.brokerNum = 1;
		this.nodeBrokers = new ArrayList<String>();
		this.nodes = null;
	}

	BrokerSetup(int brokerNum, TwisterNodes twisterNodes, boolean isBrokerOnNFS) {
		this.brokerNum = brokerNum;
		this.driverBroker = twisterNodes.getDriver();
		this.nodeBrokers = new ArrayList<String>();
		this.nodes = twisterNodes;
		this.isBrokerOnNFS = isBrokerOnNFS;
	}

	final boolean configure() {
		if (this.brokerNum != 1 && this.isBrokerOnNFS) {
			System.err
					.println("Errors happen in checking broker number settings.");
			return false;
		}
		// select brokers
		boolean status = true;
		status = selectBrokers();
		if (!status) {
			System.err.println("Errors happen in selecting brokers.");
			return false;
		}
		// distribute brokers as required
		if (this.brokerNum != 1) {
			System.out.println("Distribute brokers...");
			status = distributeBrokers();
			if (!status) {
				System.err
						.println("Errors happen in distributing broker packages");
				return false;
			}
		}
		// setup files in broker servers and broker clients
		status = setupBrokerServerFiles();
		if (!status) {
			System.err.println("Errors happen in setuping broker server files");
			return false;
		}
		status = setupBrokerClientFiles();
		if (!status) {
			System.err.println("Errors happen in setuping broker client files");
			return false;
		}
		status = setupBrokerNodesFile();
		if (!status) {
			System.err.println("Errors happen in setuping broker node files");
			return false;
		}
		return status;
	}

	/**
	 * Randomly Select broker nodes where the number is corresponding to
	 * brokerNum
	 */
	private boolean selectBrokers() {
		boolean status = true;
		// driver broker is already counted
		int count = 1;
		// randomly select brokers
		ArrayList<String> tmpNodeList = new ArrayList<String>(
				this.nodes.getDaemonNodes());
		// Potentially driver can show in node list, remove it
		if (this.nodes.isDriverInNodes()) {
			tmpNodeList.remove(this.nodes.getDriver());
		}
		// we just need brokers for nodes
		while (count < this.brokerNum) {
			String potentialBroker = tmpNodeList
					.remove((int) (Math.random() * tmpNodeList.size()));
			this.nodeBrokers.add(potentialBroker);
			count++;
			if (tmpNodeList.size() == 0) {
				break; // no more nodes available
			}
		}
		if (count < this.brokerNum) {
			status = false;
		}
		return status;
	}

	abstract protected boolean distributeBrokers();

	abstract protected boolean setupBrokerServerFiles();

	abstract protected boolean setupBrokerClientFiles();

	/**
	 * write broker addresses to broker nodes file
	 */
	private boolean setupBrokerNodesFile() {
		List<String> allBrokers = new ArrayList<String>(this.nodeBrokers);
		allBrokers.add(this.driverBroker);
		return QuickDeployment.writeToFile(QuickDeployment.broker_nodes_file,
				allBrokers);
	}

	protected int getBrokerNum() {
		return brokerNum;
	}

	protected String getDriverBroker() {
		return this.driverBroker;
	}

	protected List<String> getNodeBrokers() {
		return this.nodeBrokers;
	}

	protected boolean isBrokerOnNFS() {
		return this.isBrokerOnNFS;
	}

	protected TwisterNodes getTwisterNodes() {
		return this.nodes;
	}
}
