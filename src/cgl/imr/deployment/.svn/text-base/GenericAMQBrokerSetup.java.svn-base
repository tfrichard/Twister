package cgl.imr.deployment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.jdom.xpath.XPath;

class GenericAMQBrokerSetup extends BrokerSetup {
	private int brokerMinMem = 256; // unit MB, default min mem in ActiveMQ
									// script
	private int brokerMaxMem = 512; // unit MB
	private String protocalName = "tcp"; // for performance
	private String protocalParameter = "?wireFormat.maxInactivityDuration=300000";

	// private HashMap<String, Integer>brokerMap;
	// private boolean isTwisterOnNFS;

	GenericAMQBrokerSetup(int brokerNum, TwisterNodes twisterNodes,
			boolean isTwisterOnNFS, boolean isBrokerOnNFS) {
		super(brokerNum, twisterNodes, isBrokerOnNFS);
		// brokerMap = new HashMap<String, Integer>();
		// this.isTwisterOnNFS = isTwisterOnNFS;
	}

	/**
	 * Similar to Twister package distribution, build, copy and untar
	 */
	protected boolean distributeBrokers() {
		boolean status = false;
		// get dir name and package name
		StringBuffer activeMQHome = new StringBuffer(
				QuickDeployment.getActiveMQHomePath());
		if (activeMQHome.lastIndexOf("/") == activeMQHome.length() - 1) {
			activeMQHome.deleteCharAt(activeMQHome.length() - 1);
		}
		int slashPos = activeMQHome.lastIndexOf("/");
		String srcDir = activeMQHome.substring(0, slashPos);
		String packageName = activeMQHome.substring(slashPos + 1);
		System.out.println("ActiveMQ package replication: srcDir: " + srcDir
				+ " packageName: " + packageName);

		// Assume srcDir is equal to destDir and it exists there
		String destDir = srcDir;
		// first, build tar package
		status = QuickDeployment.buildPackageTar(srcDir, packageName, destDir);
		if (!status) {
			return false;
		}
		// enter home folder and then execute tar copy and untar
		for (int i = 0; i < this.getNodeBrokers().size(); i++) {
			status = QuickDeployment.copyandTarx(destDir + "/" + packageName
					+ ".tar.gz", this.getNodeBrokers().get(i),
					destDir.toString());
			if (!status) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Setup ActiveMQ broker server. modify run script modify configuration file
	 * generate new activemq script with proper memory size and write to local
	 * replace the original one
	 */
	protected boolean setupBrokerServerFiles() {
		// System.out.println("Start to steup broker server file. ");
		boolean status = true;
		// generate the script and write back
		status = generateActiveMQExeScript();
		if (!status) {
			return false;
		}
		// copy executable script
		String brokerAddress = null;
		String dest = null;
		for (int i = 0; i < this.getNodeBrokers().size(); i++) {
			brokerAddress = this.getNodeBrokers().get(i);
			System.out.println("Distribute ActiveMQ script to " +brokerAddress);
			dest = brokerAddress + ":" + QuickDeployment.activemq_home + "bin/";
			status = QuickDeployment.copyFile(QuickDeployment.amq_script, dest);
			if (!status) {
				return false;
			}
		}
		// generate a new conf and copy conf file to node brokers
		for (int i = 0; i < this.getNodeBrokers().size(); i++) {
			brokerAddress = this.getNodeBrokers().get(i);
			System.out.println("Generate ActiveMQ conf for " + brokerAddress);
			status = generateActiveMQConf(brokerAddress);
			if (!status) {
				return false;
			}
			dest = brokerAddress + ":" + QuickDeployment.activemq_home
					+ "conf/";
			brokerAddress = this.getNodeBrokers().get(i);
			System.out.println("Distribute ActiveMQ conf to " + brokerAddress);
			status = QuickDeployment.copyFile(QuickDeployment.amq_conf_xml,
					dest);
			if (!status) {
				return false;
			}
		}
		// generate a new conf to driver broker
		// this is executed as the last one, then it won't be replaced by conf
		// files for other nodes
		status = generateActiveMQConf(this.getDriverBroker());
		if (!status) {
			return false;
		}
		return status;
	}

	private boolean generateActiveMQExeScript() {
		boolean status = true;
		StringBuffer contents = new StringBuffer();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					QuickDeployment.amq_script));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (line.matches("ACTIVEMQ_OPTS_MEMORY=\"-Xms[0-9]*M -Xmx[0-9]*M\"")) {
					int max_mem = (int) (QuickDeployment.getTotalMem() * 0.1);
					// an assumption that -Xms is 256M, max mem should be larger
					// than min mem
					if (max_mem > this.brokerMinMem) {
						brokerMaxMem = max_mem;
					} else {
						brokerMaxMem = this.brokerMinMem;
					}
					line = line.replaceFirst("-Xmx[0-9]*M", "-Xmx"
							+ brokerMaxMem + "M");
					System.out.println("Change ActiveMQ broker max memory to "
							+ brokerMaxMem + " MB");
				}
				if (line.contains("-Dorg.apache.activemq.UseDedicatedTaskRunner=true")) {
					line = line
							.replaceFirst(
									"-Dorg.apache.activemq.UseDedicatedTaskRunner=true",
									"-Dorg.apache.activemq.UseDedicatedTaskRunner=false");
				}
				contents.append(line + "\n");
			}
			reader.close();
		} catch (Exception e) {
			status = false;
			e.printStackTrace();
		}
		if (!status) {
			return false;
		}
		QuickDeployment.writeToFile(QuickDeployment.amq_script, new String(
				contents));
		File newAmqScriptFile = new File(QuickDeployment.amq_script);
		newAmqScriptFile.setExecutable(true);
		return status;
	}

	/**
	 * get the current local configuration file, here jdom is used to do
	 * modification on XML file
	 * 
	 * @param brokerAddress
	 */
	private boolean generateActiveMQConf(String brokerAddress) {
		// assume success, if not return flase
		boolean status = true;
		Document doc = null;
		String filePath = QuickDeployment.amq_conf_xml;
		SAXBuilder sb = new SAXBuilder();
		try {
			// get the current local configuration file
			doc = sb.build(new File(filePath));
			/*
			 * namespaces are important here, broker is beans' child, but they
			 * have different namespaces.
			 */
			Namespace root = Namespace.getNamespace("root",
					"http://www.springframework.org/schema/beans");
			// search for broker node by XPATH
			XPath xpath = XPath.newInstance("/root:beans/amq:broker");
			xpath.addNamespace(root);
			Element broker = (Element) xpath.selectSingleNode(doc);
			if (broker != null) {
				String brokerName = "broker-" + brokerAddress.replace(".", "-");
				broker.setAttribute("brokerName", brokerName);
				// see http://activemq.apache.org/broker-uri.html
				broker.removeAttribute("destroyApplicationContextOnStop");
				broker.setAttribute("useShutdownHook", "true");
				// remove existing networkConnectors, these were used in broker
				// networks
				broker.removeChildren("networkConnectors",
						broker.getNamespace());
				// remove existing systemUsage
				broker.removeChildren("systemUsage", broker.getNamespace());
				// build new systemUsage
				Element systemUsage = new Element("systemUsage",
						broker.getNamespace());
				Element systemUsageInstance = new Element("systemUsage",
						broker.getNamespace());
				Element memoryUsage = new Element("memoryUsage",
						broker.getNamespace());
				Element memoryUsageInstance = new Element("memoryUsage",
						broker.getNamespace());
				// the values here are based on brokerMaxMem, the JVM mem value
				memoryUsageInstance.setAttribute("limit", brokerMaxMem / 2
						+ " mb");
				memoryUsage.addContent(memoryUsageInstance);
				Element storeUsage = new Element("storeUsage",
						broker.getNamespace());
				Element storeUsageInstance = new Element("storeUsage",
						broker.getNamespace());
				storeUsageInstance.setAttribute("limit", brokerMaxMem * 10
						/ 1024 + " gb");
				storeUsage.addContent(storeUsageInstance);
				Element tempUsage = new Element("tempUsage",
						broker.getNamespace());
				Element tempUsageInstance = new Element("tempUsage",
						broker.getNamespace());
				tempUsageInstance.setAttribute("limit", brokerMaxMem * 5 / 1024
						+ " gb");
				tempUsage.addContent(tempUsageInstance);
				systemUsageInstance.addContent(memoryUsage);
				systemUsageInstance.addContent(storeUsage);
				systemUsageInstance.addContent(tempUsage);
				systemUsage.addContent(systemUsageInstance);
				// modify transportConnector
				xpath = XPath
						.newInstance("/root:beans/amq:broker/amq:transportConnectors/amq:transportConnector");
				xpath.addNamespace(root);
				Element transportConnector = (Element) xpath
						.selectSingleNode(doc);
				transportConnector.setAttribute("uri", protocalName + "://"
						+ brokerAddress + ":61616" + protocalParameter);
				// modify amq:kahaDB, settings are from ActiveMQ throughput
				// settings.
				xpath = XPath
						.newInstance("/root:beans/amq:broker/amq:persistenceAdapter/amq:kahaDB");
				xpath.addNamespace(root);
				Element kahaDB = (Element) xpath.selectSingleNode(doc);
				kahaDB.setAttribute("enableJournalDiskSyncs", "false");
				kahaDB.setAttribute("indexWriteBatchSize", "10000");
				kahaDB.setAttribute("indexCacheSize", "1000");
				// remove memlimit in producerFlowControl
				xpath = XPath
						.newInstance("/root:beans/amq:broker/amq:destinationPolicy/amq:policyMap/amq:policyEntries/amq:policyEntry");
				xpath.addNamespace(root);
				Element policyEntry = (Element) xpath.selectSingleNode(doc);
				// producerFlowControl can block producer in sync mode
				policyEntry.setAttribute("producerFlowControl", "false");
				policyEntry.removeAttribute("memoryLimit");
				// remove import of carmel and jetty
				doc.getRootElement().removeChild("import", root);
				// get amq:persistenceAdapter, add new systemUsage
				xpath = XPath
						.newInstance("/root:beans/amq:broker/amq:persistenceAdapter");
				xpath.addNamespace(root);
				Element persistenceAdapter = (Element) xpath
						.selectSingleNode(doc);
				broker.addContent(broker.indexOf(persistenceAdapter) + 1,
						systemUsage);
			}
			// write to a temp file
			XMLOutputter xp = new XMLOutputter(Format.getPrettyFormat());
			xp.output(doc, new FileWriter(filePath));
		} catch (JDOMException e) {
			status = false;
			e.printStackTrace();
		} catch (IOException e) {
			status = false;
			e.printStackTrace();
		}
		return status;
	}

	/**
	 * setup client, provide link url to all the brokers specified every node
	 * now has connection to all brokers
	 */
	protected boolean setupBrokerClientFiles() {
		boolean status = false;
		// brokerMap records the distribution of clients on brokers
		// only brokers on nodes are put here
		Map<String, Integer> brokerMap = new HashMap<String, Integer>();
		for (String brokerAddress : this.getNodeBrokers()) {
			brokerMap.put(brokerAddress, 0);
		}
		// firstly, daemon nodes, we set driver node later
		for (String node : this.getTwisterNodes().getDaemonNodes()) {
			// if driver shows in daemon list, we decide it later
			if (node.equals(this.getTwisterNodes().getDriver())) {
				continue;
			}
			if (this.getNodeBrokers().contains(node)) {
				status = generateActiveMQClientURI(node, 1, brokerMap);
			} else {
				status = generateActiveMQClientURI(node, 2, brokerMap);
			}
			if (!status) {
				return false;
			}
			// properties generated is under twister_home/bin/, copy to remote
			String dest = node + ":" + QuickDeployment.getTwisterHomePath()
					+ "bin/";
			QuickDeployment.copyFile(QuickDeployment.amq_properties, dest);
		}
		// then we set broker connection on driver
		status = generateActiveMQClientURI(this.getTwisterNodes().getDriver(),
				0, brokerMap);
		if (!status) {
			return false;
		}
		// do print
		for (String brokerAddress : brokerMap.keySet()) {
			System.out.println("Broker: " + brokerAddress + " Count: "
					+ brokerMap.get(brokerAddress));
		}
		return true;
	}

	/**
	 * load amq.properties and generate a new one. Let's keep the order of URIs
	 * be random., Nodes are arranged in 3 types. 1. daemon node with broker 2.
	 * daemon node without broker, 0. driver node
	 * 
	 * for connection list, connection 0 is driver broker connection connection
	 * 1 is prime broker connection (for daemon nodes) ...
	 * 
	 * @param nodeAddress
	 * @param isNodeAsBroker
	 */
	private boolean generateActiveMQClientURI(String nodeAddress, int nodeType,
			Map<String, Integer> brokerMap) {
		boolean status = true;
		// open amq.properties file
		Properties properties = new Properties();
		try {
			properties.load(new FileReader(QuickDeployment.amq_properties));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			status = false;
		} catch (IOException e) {
			e.printStackTrace();
			status = false;
		}
		if (!status) {
			return false;
		}
		properties.clear();
		// get the prime broker, it is not the driver broker!
		ArrayList<String> tmpBrokerList = new ArrayList<String>(
				this.getNodeBrokers());
		String primeBroker = null;
		if (nodeType == 0) {
			// driver node, we use driver broker, no prime broker
		} else if (nodeType == 1) {
			primeBroker = nodeAddress;
			brokerMap.put(primeBroker, brokerMap.get(primeBroker) + 1);
		} else if (nodeType == 2) {
			primeBroker = generatePrimeBroker(brokerMap);
		}
		// index 0 is driver broker
		int index = QuickDeployment.key_uri_index_start;
		String value = "failover:(" + protocalName + "://"
				+ this.getDriverBroker() + ":61616" + protocalParameter
				+ ")?randomize=false";
		properties.setProperty(QuickDeployment.key_uri_base + index, value);
		System.out.println(nodeAddress + ": Prime ActiveMQ URI " + " = "
				+ properties.getProperty(QuickDeployment.key_uri_base + index));
		index++;
		// index 1 is prime broker
		if (primeBroker != null) {
			value = "failover:(" + protocalName + "://" + primeBroker
					+ ":61616" + protocalParameter + ")?randomize=false";
			properties.setProperty(QuickDeployment.key_uri_base + index, value);
			System.out.println(nodeAddress
					+ ": Prime ActiveMQ URI "
					+ " = "
					+ properties.getProperty(QuickDeployment.key_uri_base
							+ index));
			// remove the prime broker
			tmpBrokerList.remove(primeBroker);
		}
		// if no prime broker, we skip index 1.
		index++;
		// set the rest connections
		while (!tmpBrokerList.isEmpty()) {
			value = "failover:("
					+ protocalName
					+ "://"
					+ tmpBrokerList.remove((int) (Math.random() * tmpBrokerList
							.size())) + ":61616" + protocalParameter
					+ ")?randomize=false";
			properties.setProperty(QuickDeployment.key_uri_base + index, value);
			index++;
		}
		String comments = QuickDeployment
				.readPropertyComments(QuickDeployment.amq_properties);
		if (comments == null) {
			return false;
		}
		try {
			properties.store(new FileWriter(QuickDeployment.amq_properties),
					comments);
		} catch (IOException e) {
			e.printStackTrace();
			status = false;
		}
		if (!status) {
			return false;
		}
		return true;
	}

	private String generatePrimeBroker(Map<String, Integer> brokerMap) {
		String primeBroker = null;
		// no choices for prime broker, it means there is only one driver broker
		if (brokerMap.size() == 0) {
			return null;
		}
		// there are two brokers, 1 for driver, 1 for daemons
		if (brokerMap.size() == 1) {
			primeBroker = new ArrayList<String>(brokerMap.keySet()).get(0);
			return primeBroker;
		}
		// for multiple daemon brokers...
		int count = -1;
		for (String brokerAddress : brokerMap.keySet()) {
			if (count == -1 && primeBroker == null) {
				count = brokerMap.get(brokerAddress);
				primeBroker = brokerAddress;
				continue;
			}
			if (count > brokerMap.get(brokerAddress)) {
				count = brokerMap.get(brokerAddress);
				primeBroker = brokerAddress;
			}
		}
		brokerMap.put(primeBroker, brokerMap.get(primeBroker) + 1);
		return primeBroker;
	}
}
