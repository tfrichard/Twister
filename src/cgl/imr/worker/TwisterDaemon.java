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

package cgl.imr.worker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import cgl.imr.base.TwisterException;
import cgl.imr.client.DaemonInfo;
import cgl.imr.communication.DataManager;
import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.util.DataHolder;
import cgl.imr.util.TwisterCommonUtil;

/**
 * TwisterDaemon that is responsible for all the server side processing in
 * Twister framework. Once started, every daemon starts listening to a given
 * port which can be used to stop the daemons from running at the end of the
 * MapReduce computations or abruptly in the middle of a computation. This
 * method has no relation to the pub-sub broker communications and hence can be
 * used even in the case of pub-sub broker failures.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 *         Correct several control logics.
 * @author zhangbj
 * 
 */
public class TwisterDaemon {
	
	private static Logger logger = Logger.getLogger(TwisterDaemon.class);
	private int daemonNo;
	private int daemonPort;
	private int workWindowSize;
	private Map<Integer, DaemonInfo> daemonInfo;
	private ConcurrentHashMap<String, DataHolder> dataCache;
	private DaemonWorker daemonWorker;
	private Executor serverTaskExecutor;
	private Executor clientTaskExecutor;

	public TwisterDaemon(int daemonNo, int numMapWorkers, String host)
			throws ConfigurationException, IOException, TwisterException {
		super();
		workWindowSize = numMapWorkers;
		serverTaskExecutor = Executors.newFixedThreadPool(workWindowSize);
		clientTaskExecutor = Executors.newFixedThreadPool((int) Math
				.ceil(workWindowSize / (double) 2));
		dataCache = new ConcurrentHashMap<String, DataHolder>();
		this.daemonNo = daemonNo;
		// This is how daemon port is calculated
		TwisterConfigurations configs = TwisterConfigurations.getInstance();
		int daemonPortBase = configs.getDaemonPortBase();
		this.daemonPort = daemonPortBase + daemonNo;
		this.daemonInfo = TwisterCommonUtil
				.getDaemonInfoFromConfigFiles(configs);
		// create daemon worker
		this.daemonWorker = new DaemonWorker(daemonNo, numMapWorkers,
				dataCache, daemonPort, host, clientTaskExecutor, daemonInfo);
		// initialize data manager
		DataManager.getInstance();
	}

	/**
	 * Handles the socket based communication, which is use to stop the
	 * TwisterDaemon.
	 */
	public void run() {
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(daemonPort);
		} catch (Exception exp) {
			serverSocket = null;
			logger.error("TwisterDaemon No" + daemonNo
					+ "quiting due to error.", exp);
		}
		if (serverSocket == null) {
			System.exit(-1);
		}
		Socket socket = null;
		while (true) {
			try {
				socket = serverSocket.accept();
			} catch (IOException e) {
				socket = null;
				e.printStackTrace();
			}
			if (socket != null) {
				CMDProssesor sender = new CMDProssesor(socket, dataCache,
						daemonWorker, daemonNo, daemonInfo, clientTaskExecutor);
				serverTaskExecutor.execute(sender);
			}
		}
	}

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out
					.println("Usage: cgl.mr.worker.TwisterDaemon [Daemon No][Number Workers Threads = number of CPU cores (typically)][host]");
			return;
		}
		int daemonNo = Integer.valueOf(args[0]).intValue();
		int numWorkerThreads = Integer.valueOf(args[1]).intValue();
		TwisterDaemon daemon = null;
		try {
			daemon = new TwisterDaemon(daemonNo, numWorkerThreads, args[2]);
		} catch (Exception e) {
			daemon = null;
			logger.error("TwisterDaemon No" + daemonNo
					+ "failed to initialize due to error.", e);
			System.exit(-1);
		}
		// make sure everything is ready before starting
		if (daemon != null) {
			daemon.run();
		}
	}
}
