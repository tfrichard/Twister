package cgl.imr.deployment;

import java.util.ArrayList;
import java.util.List;

class TwisterNodes {
	private String driver;
	private List<String> daemonNodes;
	private boolean isDriverInNodes;

	TwisterNodes(String driverIP) {
		driver = driverIP;
		daemonNodes = new ArrayList<String>();
		isDriverInNodes = false;
	}

	String getDriver() {
		return driver;
	}

	List<String> getDaemonNodes() {
		return daemonNodes;
	}

	void setIsDriverInNodes(boolean isInNodes) {
		isDriverInNodes = isInNodes;
	}

	boolean isDriverInNodes() {
		return isDriverInNodes;
	}
}
