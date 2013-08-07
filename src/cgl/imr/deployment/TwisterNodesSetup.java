package cgl.imr.deployment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import java.util.Properties;

class TwisterNodesSetup {
	// driver IP address
	private String driverIP;
	// daemon nodes IPs
	private List<String> nodes;
	// private boolean isDriverInNodes;
	private boolean isTwisterOnNFS;

	TwisterNodesSetup(TwisterNodes twisterNodes, boolean isTwisterOnNFS) {
		this.driverIP = twisterNodes.getDriver();
		this.nodes = twisterNodes.getDaemonNodes();
		this.isTwisterOnNFS = isTwisterOnNFS;
	}

	boolean configure() {
		boolean status = false;
		System.out.println("Setup nodes file...");
		status = setupNodesFile();
		if (!status) {
			System.err.println("Errors in writing to nodes file.");
			return false;
		}
		System.out.println("Setup driver file...");
		status = setupDriverFile();
		if (!status) {
			System.err.println("Errors in writing to driver_node file.");
			return false;
		}
		System.out.println("Setup twister.properties...");
		status = setupTwisterProperties();
		if (!status) {
			System.err
					.println("Errors in writing to twister.properties file.");
			return false;
		}
		System.out.println("Setup stimr.sh...");
		status = setupStImr();
		if (!status) {
			System.err
					.println(" Errors in writing to twister.properties file.");
			return false;
		}
		if (!this.isTwisterOnNFS) {
			System.out.println("Peplicating packages to all the nodes...");
			status = replicateConfiguration();
			if (!status) {
				System.err.println("Errors in replicating Twister packages.");
				return false;
			}
		}
		return status;
	}

	/**
	 * write back node IPs to nodes
	 * 
	 * @return
	 */
	private boolean setupNodesFile() {
		return QuickDeployment.writeToFile(QuickDeployment.nodes_file,
				this.nodes);
	}

	/**
	 * write driver IP to driver_node
	 */
	private boolean setupDriverFile() {
		return QuickDeployment.writeToFile(QuickDeployment.driver_node_file,
				this.driverIP);
	}

	/**
	 * set Twister.properties no change to daemon port
	 */
	private boolean setupTwisterProperties() {
		boolean status = true;
		Properties properties = new Properties();
		try {
			String twisterHome = QuickDeployment.twister_home;
			properties.load(new FileReader(QuickDeployment.twister_properties));

			// set nodes file location
			String nodes_file_value = twisterHome + "bin/nodes";
			properties.setProperty(QuickDeployment.key_nodes_file,
					nodes_file_value);
			System.out.println("nodes_file="
					+ properties.getProperty(QuickDeployment.key_nodes_file));

			// set daemons per node
			properties.setProperty(QuickDeployment.key_daemons_per_node, "1");
			System.out.println("daemons_per_node="
					+ properties
							.getProperty(QuickDeployment.key_daemons_per_node));

			// set workers per node, we set it to a smaller value than all the
			// cores available
			// int numWorkers = (int) Math.ceil(Runtime.getRuntime()
			// .availableProcessors() / (double) 2);
			int numWorkers = Runtime.getRuntime().availableProcessors();
			properties.setProperty(QuickDeployment.key_workers_per_daemon,
					numWorkers + "");
			System.out
					.println("workers_per_daemon="
							+ properties
									.getProperty(QuickDeployment.key_workers_per_daemon));

			// set pubsub_broker name
			properties.setProperty(QuickDeployment.key_pubsub_broker,
					"ActiveMQ");
			System.out
					.println("pubsub_broker="
							+ properties
									.getProperty(QuickDeployment.key_pubsub_broker));

			// set app dir
			String app_dir_value = twisterHome + "apps";
			properties.setProperty(QuickDeployment.key_app_dir, app_dir_value);
			System.out.println("app_dir="
					+ properties.getProperty(QuickDeployment.key_app_dir));

			// set data_dir
			String dir = createTwisterDataDir();
			if (dir == null) {
				dir = "";
				status = false;
			}
			properties.setProperty(QuickDeployment.key_data_dir, dir);
			System.out.println("data_dir="
					+ properties.getProperty(QuickDeployment.key_data_dir));

			// read comments and write
			String comments = QuickDeployment
					.readPropertyComments(QuickDeployment.twister_properties);
			if (comments == null) {
				comments = "";
				status = false;
			}
			properties.store(
					new FileWriter(QuickDeployment.twister_properties),
					comments);
		} catch (Exception e) {
			status = false;
			e.printStackTrace();
		}
		return status;
	}

	private String createTwisterDataDir() {
		System.out.println("Create common data dir, please wait...");
		boolean status = true;
		// first level dir
		String data_dir1 = null;
		if (this.isTwisterOnNFS) {
			String userHome = QuickDeployment.getUserHome();
			if (userHome == null) {
				return null;
			}
			data_dir1 = userHome + "/";
		} else {
			String username = QuickDeployment.getUserName();
			if (username == null) {
				return null;
			}
			data_dir1 = "/tmp/" + username + "/";
			status = QuickDeployment.createDir(data_dir1);
			if (status) {
				System.out.println("Directory path " + data_dir1
						+ " are created on all nodes.");
			} else {
				return null;
			}
		}

		// second level dir
		String data_dir2 = data_dir1 + "data/";
		status = QuickDeployment.createDir(data_dir2);
		if (status) {
			System.out.println("Directory path " + data_dir2
					+ " are created on all nodes.");
		} else {
			return null;
		}

		return data_dir2;
	}

	/**
	 * read stimr, modify then write back
	 * 
	 * @return
	 */
	private boolean setupStImr() {
		boolean status = false;
		StringBuffer contents = new StringBuffer();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					QuickDeployment.stimr_sh));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("java")) {
					/*
					double level = Math.log(QuickDeployment.getTotalMem())
							/ Math.log(2);
					double floorLevel = Math.floor(level);
					double adjustedLevel = floorLevel;
					if ((level - floorLevel) < 0.2) {
						adjustedLevel = floorLevel - 1;
					}
					int max_mem = (int) (Math.pow(2, adjustedLevel));
					*/
					int max_mem = (int) (QuickDeployment.getTotalMem() * 0.8);
					int min_mem = 256;
					if (max_mem < 256) {
						min_mem = max_mem;
					}
					line = line.replaceFirst("-Xmx[0-9]*m", "-Xmx" + max_mem
							+ "m");
					System.out.println("Change max memory to " + max_mem
							+ " MB");
					line = line.replaceFirst("-Xms[0-9]*m", "-Xms" + min_mem
							+ "m");
					System.out.println("Change min memory to " + min_mem
							+ " MB");
					status = true;
				}
				contents.append(line + "\n");
			}
			reader.close();
		} catch (Exception e) {
			status = false;
		}
		if (!status) {
			return false;
		}

		status = QuickDeployment.writeToFile(QuickDeployment.stimr_sh,
				new String(contents));
		File newstimrFile = new File(QuickDeployment.stimr_sh);
		newstimrFile.setExecutable(true);
		return status;
	}

	/**
	 * package all the things under Twister Home, and then send to remote folder
	 * and tarx them.
	 */
	boolean replicateConfiguration() {
		boolean status = false;
		// get dir name and package name
		StringBuffer twisterHome = new StringBuffer(
				QuickDeployment.getTwisterHomePath());
		if (twisterHome.lastIndexOf("/") == twisterHome.length() - 1) {
			twisterHome.deleteCharAt(twisterHome.length() - 1);
		}
		int slashPos = twisterHome.lastIndexOf("/");
		String srcDir = twisterHome.substring(0, slashPos);
		String packageName = twisterHome.substring(slashPos + 1);
		System.out.println("Twister package replication: srcDir: " + srcDir
				+ " packageName: " + packageName);

		// Assume srcDir is equal to destDir and it exists there
		String destDir = srcDir;
		// first, build tar package
		status = QuickDeployment.buildTwisterTar(srcDir, packageName, destDir);
		if (!status) {
			return false;
		}
		// enter home folder and then execute tar copy and untar
		for (int i = 0; i < this.nodes.size(); i++) {
			// if driver is also used as daemon nodes, skip
			if (this.nodes.get(i).equals(this.driverIP)) {
				continue;
			}
			status = QuickDeployment.copyandTarx(destDir + "/" + packageName
					+ ".tar.gz", this.nodes.get(i), destDir.toString());
			if (!status) {
				return false;
			}
		}
		return true;
	}
}
