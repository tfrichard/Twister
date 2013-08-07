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

package cgl.imr.config;

import java.util.Properties;

import cgl.imr.base.TwisterConstants;
import cgl.imr.util.PropertyLoader;

/**
 * Wrapper class for Twister related properties. These properties are read from
 * a file named twister.properties that is available in the classpath.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 *         11/24/2009
 * 
 */
public class TwisterConfigurations {
	private static TwisterConfigurations configurations; // Singleton.
	// final keys
	final static String KEY_DAEMON_PORT_BASE = "daemon_port";
	final static String KEY_LOCAL_APP_JAR_DIR = "app_dir";
	final static String KEY_LOCAL_DATA_DIR = "data_dir";
	final static String KEY_NODES_FILE = "nodes_file";
	final static String KEY_DAEMONS_PER_NODE = "daemons_per_node";
	final static String KEY_WORKERS_PER_DAEMON = "workers_per_daemon";
	final static String KEY_PUBSUB_BROKER = "pubsub_broker";
	// settings
	private int daemonPortBase;
	private String localAppJarDir;
	private String localDataDir;
	private String nodeFile;
	private int damonsPerNode;
	private int workersPerDaemon;
	private String pubsubBroker;

	private TwisterConfigurations() throws ConfigurationException {
		this(TwisterConstants.PROPERTIES_FILE);
	}

	private TwisterConfigurations(String propertiesFile)
			throws ConfigurationException { // Used for tests
		try {
			// System.out.println("Start loading configurations: " + propertiesFile);
			Properties properties = PropertyLoader
					.loadProperties(propertiesFile);
			this.nodeFile = properties.getProperty(KEY_NODES_FILE);
			this.damonsPerNode = Integer.parseInt(properties
					.getProperty(KEY_DAEMONS_PER_NODE));
			this.workersPerDaemon = Integer.parseInt(properties
					.getProperty(KEY_WORKERS_PER_DAEMON));
			this.localAppJarDir = properties.getProperty(KEY_LOCAL_APP_JAR_DIR);
			this.localDataDir = properties.getProperty(KEY_LOCAL_DATA_DIR);
			this.pubsubBroker = properties.getProperty(KEY_PUBSUB_BROKER);
			this.daemonPortBase = Integer.parseInt(properties
					.getProperty(KEY_DAEMON_PORT_BASE));

			// Check for not null
			if (nodeFile == null || localAppJarDir == null
					|| localDataDir == null || pubsubBroker == null
					|| daemonPortBase == 0) {
				throw new ConfigurationException("Invalid properties.");
			}

		} catch (Exception e) {
			throw new ConfigurationException(
					"Could not load the propeties correctly.", e);
		}
	}

	public synchronized static TwisterConfigurations getInstance()
			throws ConfigurationException {
		if (configurations == null) {
			configurations = new TwisterConfigurations();
		}
		return configurations;
	}

	public int getDaemonPortBase() {
		return daemonPortBase;
	}

	public String getLocalAppJarDir() {
		return localAppJarDir;
	}

	public String getLocalDataDir() {
		return localDataDir;
	}

	public String getNodeFile() {
		return nodeFile;
	}

	public String getPubsubBroker() {
		return pubsubBroker;
	}

	public int getDamonsPerNode() {
		return damonsPerNode;
	}

	public int getWorkersPerDaemon() {
		return workersPerDaemon;
	}

}
