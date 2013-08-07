/*
 * Software License, Version 1.0
 *
 *  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 *  the list of authors in the original source code, this list of conditions and
 *  the disclaimer listed in this license;
 * 2) All redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the disclaimer listed in this license in
 *  the documentation and/or other materials provided with the distribution;
 * 3) Any documentation included with all redistributions must include the
 *  following acknowledgement:
 *
 * "This product includes software developed by the Community Grids Lab. For
 *  further information contact the Community Grids Lab at
 *  http://communitygrids.iu.edu/."
 *
 *  Alternatively, this acknowledgement may appear in the software itself, and
 *  wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or Twister,
 *  shall not be used to endorse or promote products derived from this software
 *  without prior written permission from Indiana University.  For written
 *  permission, please contact the Advanced Research and Technology Institute
 *  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 * 5) Products derived from this software may not be called Twister,
 *  nor may Indiana University or Community Grids Lab or Twister appear
 *  in their name, without prior written permission of ARTI.
 *
 *
 *  Indiana University provides no reassurances that the source code provided
 *  does not infringe the patent or any other intellectual property rights of
 *  any other entity.  Indiana University disclaims any liability to any
 *  recipient for claims brought by any other entity based on infringement of
 *  intellectual property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */

package cgl.imr.deployment;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * Select suitable setup mode, and execute setup works of ActiveMQ only
 * 
 * @author zhangbj
 * 
 */
public class QuickDeployment {
	final static String java_home = getJavaHome();
	final static String twister_home = getTwisterHome();
	final static String nodes_file = twister_home + "bin/nodes";
	final static String driver_node_file = twister_home + "bin/driver_node";
	final static String twister_properties = twister_home
			+ "bin/twister.properties";
	final static String key_nodes_file = "nodes_file";
	final static String key_daemons_per_node = "daemons_per_node";
	final static String key_workers_per_daemon = "workers_per_daemon";
	final static String key_pubsub_broker = "pubsub_broker";
	final static String key_app_dir = "app_dir";
	final static String key_data_dir = "data_dir";
	final static String stimr_sh = twister_home + "bin/stimr.sh";
	final static String activemq_home = getActiveMQHome();
	final static String broker_nodes_file = twister_home + "bin/broker_nodes";
	final static String amq_properties = twister_home + "bin/amq.properties";
	final static String key_uri_base = "uri_";
	final static int key_uri_index_start = 0;
	final static String amq_conf_xml = activemq_home + "conf/activemq.xml";
	final static String amq_script = activemq_home + "bin/activemq";

	/**
	 * read "nodes" file under the current directory, remove the duplicates, but
	 * keep the order
	 * 
	 */
	private static TwisterNodes getNodeAdresses(String driverIP) {
		TwisterNodes nodes = new TwisterNodes(driverIP);
		String firstIPByte = null;
		List<String> nodesList = nodes.getDaemonNodes();
		boolean status = true;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					nodes_file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (QuickDeployment.isIP(line)) {
					if (!nodesList.contains(line)) {
						nodesList.add(line); // no repeat
						// check if they are possible from two different
						// interfaces
						if (firstIPByte == null) {
							firstIPByte = line.substring(0, line.indexOf("."));
						} else if (!firstIPByte.equals(line.substring(0,
								line.indexOf(".")))) {
							status = false;
						}
						// check if driver IP used here
						if (QuickDeployment.isLocalIP(line)) {
							if (!nodes.getDriver().equals(line)) {
								status = false;
							} else {
								nodes.setIsDriverInNodes(true);
							}
						}
					}
				} else {
					status = false;
				}
			}
			reader.close();
		} catch (Exception e) {
			nodesList.clear();
			System.err.println("Errors happen in reading Node File. ");
		}

		if (!status) {

		}
		return nodes;
	}

	private static BrokerSetup getBrokerSetupInstance(String brokerType,
			int brokerNum, TwisterNodes nodes, boolean isTwisterOnNFS,
			boolean isBrokerOnNFS) {
		System.out.println("Use Generic ActiveMQ Setup.");
		BrokerSetup brokerSetup = new GenericAMQBrokerSetup(brokerNum, nodes,
				isTwisterOnNFS, isBrokerOnNFS);
		return brokerSetup;
	}

	// retrieve output of the command execution
	private static CMDOutput executeCMDandReturn(String[] cmd) {
		CMDOutput cmdOutput = new CMDOutput();
		List<String> output = cmdOutput.getExecutionOutput();
		try {
			Process q = Runtime.getRuntime().exec(cmd);
			q.waitFor();
			InputStream is = q.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);

			String line;
			while ((line = br.readLine()) != null) {
				output.add(line);
			}
			br.close();
			isr.close();
			is.close();
			if (q.exitValue() != 0) {
				cmdOutput.setExecutionStatus(false);
				output.clear();
			}
		} catch (Exception e) {
			// e.printStackTrace();
			System.err.println("Errors happen in executing " + cmd);
			cmdOutput.setExecutionStatus(false);
			output.clear();
		}
		return cmdOutput;
	}

	/**
	 * just do execution. The output is not returned, it shows on the screen
	 * directly
	 * 
	 * @param cmd
	 */
	private static CMDOutput executeCMDandForward(String[] cmd) {
		CMDOutput cmdOutput = new CMDOutput();
		try {
			Process q = Runtime.getRuntime().exec(cmd);
			q.waitFor();
			if (q.exitValue() != 0) {
				cmdOutput.setExecutionStatus(false);
			}
		} catch (Exception e) {
			// e.printStackTrace();
			System.err.println("Errors happen in executing " + cmd);
			cmdOutput.setExecutionStatus(false);
		}
		return cmdOutput;
	}

	// probably we need a executeCMD without return command
	static String getPWD() {
		String cmdstr[] = { "pwd" };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
		String pwd = null;
		if (cmdOutput.getExecutionStatus()) {
			pwd = cmdOutput.getExecutionOutput().get(0).replace(" ", "\\ ");
		}
		return pwd;
	}

	static String getUserHome() {
		String cmdstr[] = { "bash", "-c", "echo $HOME" };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
		String home = null;
		if (cmdOutput.getExecutionStatus()) {
			home = cmdOutput.getExecutionOutput().get(0).replace(" ", "\\ ");
		}
		return home;
	}

	private static String getJavaHome() {
		// ZBJ: it seems the only way to execute echo command
		String cmdstr[] = { "bash", "-c", "echo $JAVA_HOME" };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
		String java_home = null;
		if (cmdOutput.getExecutionStatus()) {
			// Home directory is returned with "/" at the end
			java_home = cmdOutput.getExecutionOutput().get(0) + "/";
			java_home = java_home.replace("//", "/");
			java_home = java_home.replace(" ", "\\ ");
		}
		return java_home;
	}

	private static String getTwisterHome() {
		// ZBJ: it seems the only way to execute echo command
		String cmdstr[] = { "bash", "-c", "echo $TWISTER_HOME" };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
		String twister_home = null;
		if (cmdOutput.getExecutionStatus()) {
			// Home directory is returned with "/" at the end
			twister_home = cmdOutput.getExecutionOutput().get(0) + "/";
			twister_home = twister_home.replace("//", "/");
			twister_home = twister_home.replace(" ", "\\ ");
		}
		return twister_home;
	}
	
	public static String getJavaHomePath() {
		return java_home;
	}

	public static String getTwisterHomePath() {
		return twister_home;
	}

	static String getActiveMQHome() {
		// ZBJ: it seems the only way to execute echo command
		String cmdstr[] = { "bash", "-c", "echo $ACTIVEMQ_HOME" };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
		String activemq_home = null;
		if (cmdOutput.getExecutionStatus()) {
			activemq_home = cmdOutput.getExecutionOutput().get(0) + "/";
			activemq_home = activemq_home.replace("//", "/");
			activemq_home = activemq_home.trim();
			// translate space to "\ "
			activemq_home = activemq_home.replace(" ", "\\ ");
		}
		return activemq_home;
	}

	public static String getActiveMQHomePath() {
		return activemq_home;
	}

	public static String getActiveMQURIBaseKey() {
		return key_uri_base;
	}

	public static int getActiveMQURIIndexStart() {
		return key_uri_index_start;
	}

	static int getTotalMem() {
		String mem = "0";
		String cmdstr[] = { "cat", "/proc/meminfo" };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
		if (cmdOutput.getExecutionStatus()) {
			for (int i = 0; i < cmdOutput.getExecutionOutput().size(); i++) {
				if (cmdOutput.getExecutionOutput().get(i).contains("MemTotal:")) {
					mem = cmdOutput.getExecutionOutput().get(i)
							.replace("MemTotal:", "").replace("kB", "").trim();
					break;
				}
			}
		}
		double memory = Double.parseDouble(mem);
		int memMB = (int) (memory / (double) 1024);
		return memMB;
	}

	static boolean createDir(String path) {
		String twisterSh = twister_home + "bin/twister.sh";
		File twisterShFile = new File(twisterSh);
		twisterShFile.setExecutable(true);
		String cmdstr[] = { twisterSh, "initdir", path };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandForward(cmdstr);
		return cmdOutput.getExecutionStatus();
	}

	static String getUserName() {
		String cmdstr[] = { "whoami" };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
		String userName = null;
		if (cmdOutput.getExecutionStatus()) {
			userName = cmdOutput.getExecutionOutput().get(0);
		}
		return userName;
	}

	static boolean doConfigureSh() {
		String configureSh = twister_home + "bin/configure.sh";
		File configureShFile = new File(configureSh);
		configureShFile.setExecutable(true);
		String cmdstr[] = { configureSh };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandForward(cmdstr);
		return cmdOutput.getExecutionStatus();
	}

	static boolean copyFile(String file, String dest) {
		String copySh = twister_home + "bin/scpfile.sh";
		File copyShFile = new File(copySh);
		copyShFile.setExecutable(true);
		String cmdstr[] = { copySh, file, dest };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandForward(cmdstr);
		return cmdOutput.getExecutionStatus();
	}

	/**
	 * build Twister tar package
	 * 
	 * @param srcDir
	 * @param packageName
	 * @param destDir
	 * @return
	 */
	static boolean buildTwisterTar(String srcDir, String packageName,
			String destDir) {
		String tarSh = twister_home + "bin/build_twister_distribution.sh";
		return QuickDeployment.buildTar(tarSh, srcDir, packageName, destDir);
	}
	
	/**
	 * build java, ActiveMQ (packages other than Twister) tar package
	 * 
	 * @param srcDir
	 * @param packageName
	 * @param destDir
	 * @return
	 */
	static boolean buildPackageTar(String srcDir, String packageName,
			String destDir) {
		String tarSh = twister_home + "bin/build_package_distribution.sh";
		return QuickDeployment.buildTar(tarSh, srcDir, packageName, destDir);
	}

	static boolean buildTar(String tarSh, String srcDir, String packageName,
			String destDir) {
		File tarShFile = new File(tarSh);
		tarShFile.setExecutable(true);
		String cmdstr[] = { tarSh, srcDir, packageName, destDir };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandForward(cmdstr);
		return cmdOutput.getExecutionStatus();
	}

	/**
	 * Copy tar package to remote and extract
	 * 
	 * @param file
	 * @param destIP
	 * @param destDir
	 * @return
	 */
	static boolean copyandTarx(String filePath, String destIP, String destDir) {
		String copyAndTarxSh = twister_home + "bin/scptarxfile.sh";
		File copyAndTarxShFile = new File(copyAndTarxSh);
		copyAndTarxShFile.setExecutable(true);
		String cmdstr[] = { copyAndTarxSh, filePath, destIP, destDir };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandForward(cmdstr);
		return cmdOutput.getExecutionStatus();
	}

	/**
	 * Only write one line to the file
	 * 
	 * @param filePath
	 * @param oneLine
	 */
	static boolean writeToFile(String filePath, String oneLine) {
		List<String> contents = new ArrayList<String>();
		contents.add(oneLine);
		return writeToFile(filePath, contents);
	}

	/**
	 * Write a list of strings to lines
	 * 
	 * @param filePath
	 * @param contents
	 */
	static boolean writeToFile(String filePath, List<String> contents) {
		boolean status = true;
		// delete the original file
		File file = new File(filePath);
		if (file.exists()) {
			file.delete();
		}
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
			for (int i = 0; i < contents.size(); i++) {
				writer.write(contents.get(i));
				// if it is not the last line, enter
				if (i < (contents.size() - 1)) {
					writer.newLine();
				}
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			// e.printStackTrace();
			System.err.println("Errors happen in writing " + filePath);
			status = false;
		}
		return status;
	}

	/**
	 * if output is null, we think there is error...
	 * 
	 * @param properties_filename
	 * @return
	 */
	static String readPropertyComments(String properties_filename) {
		StringBuffer comments = new StringBuffer();
		String commentsString = null;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					properties_filename));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("#")) {
					comments.append(line.replace("#", "") + "\n");
				}
			}
			reader.close();
		} catch (Exception e) {
			System.err.println("Errors happen in reading "
					+ properties_filename);
			comments = null;
		}
		if (comments != null) {
			commentsString = new String(comments);
		}
		return commentsString;
	}

	/**
	 * check if it is an ip
	 * 
	 * @param ip
	 * @return
	 */
	static boolean isIP(String ip) {
		if (ip == null) {
			return false;
		}
		String[] parts = ip.split("\\.");
		if (parts.length != 4) {
			return false;
		}
		for (String part : parts) {
			int i = Integer.parseInt(part);
			if ((i < 0) || (i > 255)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Test if the driver ip is the current local IP
	 * 
	 * @param ip
	 * @return
	 */
	static boolean isLocalIP(String ip) {
		boolean status = false;
		try {
			Enumeration<NetworkInterface> netInterfaces = NetworkInterface
					.getNetworkInterfaces();
			for (NetworkInterface netInterface : Collections
					.list(netInterfaces)) {
				Enumeration<InetAddress> inetAddresses = netInterface
						.getInetAddresses();
				for (InetAddress inetAddress : Collections.list(inetAddresses)) {
					if (ip.equals(inetAddress.getHostAddress())) {
						status = true;
						break;
					}
				}
			}
		} catch (SocketException e) {
			e.printStackTrace();
			System.out
					.println("Errors happen in checking if Driver IP is local...");
		}
		return status;
	}

	private static boolean isLocalDir(String path) {
		boolean status = true;
		String cmdstr[] = { "bash", "-c", "stat -f -L -c %T " + path };
		CMDOutput cmdOutput = QuickDeployment.executeCMDandReturn(cmdstr);
		if (cmdOutput.getExecutionStatus()) {
			if (cmdOutput.getExecutionOutput().get(0).equals("nfs")) {
				status = false;
			}
		}
		return status;
	}
	
	/**
	 * Similar to Twister package distribution, build, copy and untar
	 */
	private static boolean distributeJava(TwisterNodes twisterNodes) {
		boolean status = false;
		// get dir name and package name
		StringBuffer javaHome = new StringBuffer(
				QuickDeployment.getJavaHomePath());
		if (javaHome.lastIndexOf("/") == javaHome.length() - 1) {
			javaHome.deleteCharAt(javaHome.length() - 1);
		}
		int slashPos = javaHome.lastIndexOf("/");
		String srcDir = javaHome.substring(0, slashPos);
		String packageName = javaHome.substring(slashPos + 1);
		System.out.println("Java package replication: srcDir: " + srcDir
				+ " packageName: " + packageName);
		// Assume srcDir is equal to destDir and it exists there
		String destDir = srcDir;
		// first, build tar package
		status = QuickDeployment.buildPackageTar(srcDir, packageName, destDir);
		if (!status) {
			return false;
		}
		// enter home folder and then execute tar copy and untar
		for (int i = 0; i < twisterNodes.getDaemonNodes().size(); i++) {
			status = QuickDeployment.copyandTarx(destDir + "/" + packageName
					+ ".tar.gz", twisterNodes.getDaemonNodes().get(i),
					destDir.toString());
			if (!status) {
				return false;
			}
		}
		return true;
	}

	/**
	 * invoked with the following arguments [-broker_num][ -broker_tar_path]
	 * [-driver_ip]
	 * 
	 * The process is that assuming I have a local broker and a local twister,
	 * then I distribute them and configure them
	 */
	public static void main(String[] args) {
		String brokerType = "ActiveMQ";
		int brokerNum = 1;
		String driverIP = "";
		boolean invalidJavaHome = false;
		boolean invalidTwisterHome = false;
		boolean invalidBrokerHome = false;
		boolean invalidOption = false;
		boolean invalidNodes = false;
		boolean isJavaOnNFS = false;
		boolean isTwisterOnNFS = false;
		boolean isBrokerOnNFS = false;
		boolean configurationStatus = false;
		// Home setting detection
		// java home
		if (QuickDeployment.getJavaHomePath() == null) {
			invalidJavaHome = true;
		} else {
			File javaHomeFile = new File(QuickDeployment.getJavaHomePath());
			if (!javaHomeFile.exists()) {
				invalidJavaHome = true;
			}
		}
		if (invalidJavaHome) {
			System.err.println("Java Home is not set properly...");
			System.exit(1);
		}
		// twister home
		if (QuickDeployment.getTwisterHomePath() == null) {
			invalidTwisterHome = true;
		} else {
			File twisterHomeFile = new File(
					QuickDeployment.getTwisterHomePath());
			if (!twisterHomeFile.exists()) {
				invalidTwisterHome = true;
			}
		}
		if (invalidTwisterHome) {
			System.err.println("Twister Home is not set properly...");
			System.exit(1);
		}
		// ActiveMQ home
		if (QuickDeployment.getActiveMQHomePath() == null) {
			invalidBrokerHome = true;
		} else {
			File brokerHomeFile = new File(
					QuickDeployment.getActiveMQHomePath());
			if (!brokerHomeFile.exists()) {
				invalidBrokerHome = true;
			}
		}
		if (invalidBrokerHome) {
			System.err.println("Broker Home is not set properly...");
			System.exit(1);
		}
		// a simple parse for command line
		// load options
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-broker_num")) {
				if (args[i + 1].matches("[0-9]*")) {
					brokerNum = Integer.parseInt(args[i + 1]);
				} else {
					System.err.println("Invalid Broker Number... "
							+ args[i + 1]);
					invalidOption = true;
					break;
				}
			} else if (args[i].equals("-driver_ip")) {
				if (isIP(args[i + 1]) && isLocalIP(args[i + 1])) {
					driverIP = args[i + 1];
				} else {
					System.err.println("Invalid Driver IP: " + driverIP);
					invalidOption = true;
					break;
				}
			} else {
				/*
				 * this part we don't care, it may be the args[i+1] or something
				 * wrong
				 */
			}
		}
		if (invalidOption) {
			System.exit(2);
		}
		// check if driver ip and nodes
		if (driverIP.equals("")) {
			System.err.println(" Driver IP is required to be provided.");
			invalidNodes = true;
		}
		TwisterNodes twisterNodes = getNodeAdresses(driverIP);
		if (twisterNodes.getDaemonNodes().size() == 0) {
			System.err
					.println(" No nodes or wrong nodes file format is provided.");
			invalidNodes = true;
		}
		if (invalidNodes) {
			System.exit(3);
		}
		// check JAva, Twister and ActiveMQ location,
		if (!isLocalDir(QuickDeployment.getJavaHomePath())) {
			isJavaOnNFS = true;
		}
		// if Twister is on NFS, no matter
		// where ActiveMQ is, we limit its number to 1
		if (!isLocalDir(QuickDeployment.getTwisterHomePath())) {
			isTwisterOnNFS = true;
			brokerNum = 1;
		}
		// if Broker is on NFS, set broker number to 1
		if (!isLocalDir(QuickDeployment.getActiveMQHomePath())) {
			isBrokerOnNFS = true;
			brokerNum = 1;
		}
		// check the broker number is more than the max limit or equal to 0
		if (brokerNum > (twisterNodes.getDaemonNodes().size() + 1)) {
			brokerNum = twisterNodes.getDaemonNodes().size() + 1;
		}
		if (brokerNum <= 0) {
			brokerNum = 1;
		}
		// output the final settings
		if (isJavaOnNFS) {
			System.out.println("Java Home (on NFS): "
					+ QuickDeployment.getJavaHomePath());
		} else {
			System.out.println("Java Home (on Local): "
					+ QuickDeployment.getJavaHomePath());
		}
		if (isTwisterOnNFS) {
			System.out.println("Twister Home (on NFS): "
					+ QuickDeployment.getTwisterHomePath());
		} else {
			System.out.println("Twister Home (on Local): "
					+ QuickDeployment.getTwisterHomePath());
		}
		if (isBrokerOnNFS) {
			System.out.println("Broker Home (on NFS): "
					+ QuickDeployment.getActiveMQHomePath());
		} else {
			System.out.println("Broker Home (on Local): "
					+ QuickDeployment.getActiveMQHomePath());
		}
		System.out.println("Broker Type: " + brokerType);
		System.out.println("Broker Number: " + brokerNum);
		System.out.println("Driver IP: " + driverIP);
		// start deployment
		if (!isJavaOnNFS) {
			System.out.println("Start deploying Java.");
			configurationStatus = distributeJava(twisterNodes);
		}
		if (!configurationStatus) {
			System.err.println(" Errors happen in deploying Java package.");
			System.exit(4);
		}
		// change to start node setup first, then create broker setup
		System.out.println("Start deploying Twister nodes.");
		TwisterNodesSetup nodesSetup = new TwisterNodesSetup(twisterNodes,
				isTwisterOnNFS);
		configurationStatus = nodesSetup.configure();
		if (!configurationStatus) {
			System.err.println(" Errors happen in configuring Twister nodes.");
			System.exit(5);
		}
		System.out.println("Start deploying broker nodes.");
		BrokerSetup brokerSetup = QuickDeployment.getBrokerSetupInstance(
				brokerType, brokerNum, twisterNodes, isTwisterOnNFS,
				isBrokerOnNFS);
		configurationStatus = brokerSetup.configure();
		if (!configurationStatus) {
			System.err.println(" Errors happen in configuring broker nodes.");
			System.exit(6);
		}
		System.out.println("Quick deployment is done.");
	}
}