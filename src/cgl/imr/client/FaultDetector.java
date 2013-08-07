/**
 * Software License, Version 1.0
 * 
 * Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 * 
 *
 *Redistribution and use in source and binary forms, with or without 
 *modification, are permitted provided that the following conditions are met:
 *
 *1) All redistributions of source code must retain the above copyright notice,
 * the list of authors in the original source code, this list of conditions and
 * the disclaimer listed in this license;
 *2) All redistributions in binary form must reproduce the above copyright 
 * notice, this list of conditions and the disclaimer listed in this license in
 * the documentation and/or other materials provided with the distribution;
 *3) Any documentation included with all redistributions must include the 
 * following acknowledgement:
 *
 *"This product includes software developed by the Community Grids Lab. For 
 * further information contact the Community Grids Lab at 
 * http://communitygrids.iu.edu/."
 *
 * Alternatively, this acknowledgement may appear in the software itself, and 
 * wherever such third-party acknowledgments normally appear.
 * 
 *4) The name Indiana University or Community Grids Lab or Twister, 
 * shall not be used to endorse or promote products derived from this software 
 * without prior written permission from Indiana University.  For written 
 * permission, please contact the Advanced Research and Technology Institute 
 * ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 *5) Products derived from this software may not be called Twister, 
 * nor may Indiana University or Community Grids Lab or Twister appear
 * in their name, without prior written permission of ARTI.
 * 
 *
 * Indiana University provides no reassurances that the source code provided 
 * does not infringe the patent or any other intellectual property rights of 
 * any other entity.  Indiana University disclaims any liability to any 
 * recipient for claims brought by any other entity based on infringement of 
 * intellectual property rights or otherwise.  
 *
 *LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO 
 *WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 *NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF 
 *INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS. 
 *INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS", 
 *"VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.  
 *LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR 
 *ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION 
 *GENERATED USING SOFTWARE.
 */

package cgl.imr.client;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterMessage;
import cgl.imr.message.DaemonStatusMessage;

/**
 * We synchronize every access to fault detector
 * @author zhangbj
 *
 */
class FaultDetector {
	private static Logger logger = Logger.getLogger(FaultDetector.class);
	private Map<Integer, DaemonInfo> daemonInfo;
	private Map<Integer, DaemonStatus> daemonStatus;
	// current running daemon IDs, critical region
	private List<Integer> workingDaemons;
	private Timer timerTaskExecutor;
	private FaultDetectorWorker worker;
	// task executor for handle status
	private ExecutorService taskExecutor;
	private AtomicBoolean isFaultDetectorWorkerStarted;

	private class FaultDetectorWorker extends TimerTask {
		private FaultDetector faultDetector;

		FaultDetectorWorker(FaultDetector faultDetector) {
			this.faultDetector = faultDetector;
		}

		public void run() {
			this.faultDetector.detectFailures();
		}
	}

	/**
	 * From daemonInfo, build daemonStatus, from daemonStatus, build working daemon
	 * 
	 * @param daemonInfo
	 */
	FaultDetector(Map<Integer, DaemonInfo> daemonInfo) {
		this.daemonInfo = daemonInfo;
		this.daemonStatus = new ConcurrentHashMap<Integer, DaemonStatus>();
		// fault detector assumes daemons are running at the beginning
		for (int daemonID : this.daemonInfo.keySet()) {
			this.daemonStatus.put(daemonID,
					new DaemonStatus(true, System.currentTimeMillis()));
		}
		this.workingDaemons = new ArrayList<Integer>(this.daemonStatus.keySet());
		this.worker = null;
		this.timerTaskExecutor = null;
		this.taskExecutor = null;
		this.isFaultDetectorWorkerStarted = new AtomicBoolean(false);
	}

	/**
	 * find working daemons. Initially the range includes all the daemons, Then
	 * when fault happens, it tries to identify not working daemons from working
	 * daemons
	 */
	private synchronized void updateWorkingDaemons() {
		boolean failure = false;
		for (int daemonID : this.workingDaemons) {
			DaemonInfo info = this.daemonInfo.get(daemonID);
			String host = info.getIp();
			int port = info.getPort();
			DataOutputStream dout = null;
			Socket socket  = null;
			try {
				InetAddress addr = InetAddress.getByName(host);
				SocketAddress sockaddr = new InetSocketAddress(addr, port);
				socket = new Socket();
				int timeoutMs = TwisterConstants.CONNECT_MAX_WAIT_TIME;
				// if daemon is lost, it won't take long time to detect...
				socket.connect(sockaddr, timeoutMs);
				dout = new DataOutputStream(socket.getOutputStream());
				dout.writeByte(TwisterConstants.FAULT_DETECTION);
				dout.flush();
				dout.close();
				socket.close();
			} catch (Exception e) {
				System.out.println("Connection is refused on Daemon ID: "
						+ daemonID + ", Host IP:" + host);
				// e.printStackTrace();
				failure = true;
			}
			if (failure) {
				// close connection
				if (dout != null) {
					try {
						dout.close();
						dout = null;
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if (socket != null) {
					try {
						socket.close();
						socket = null;
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				// set status
				this.daemonStatus.get(daemonID).setRunning(false);
				this.daemonStatus.get(daemonID).setLastDetectionTime(
						System.currentTimeMillis());
			} else {
				this.daemonStatus.get(daemonID).setRunning(true);
				this.daemonStatus.get(daemonID).setLastDetectionTime(
						System.currentTimeMillis());
			}
			failure = false;
		}
		// find working daemons in original working daemons list
		List<Integer> newWorkingDaemons = new ArrayList<Integer>();
		for (int daemonID : this.daemonStatus.keySet()) {
			if (this.daemonStatus.get(daemonID).isRunning()
					&& this.workingDaemons.contains(daemonID)) {
				newWorkingDaemons.add(daemonID);
			}
		}
		this.workingDaemons = newWorkingDaemons;
		// sort the daemon No. in ascending order
		Collections.sort(this.workingDaemons);
	}

	/**
	 * Maintain starting states
	 */
	synchronized void start() {
		if (!this.isFaultDetectorWorkerStarted.get()) {
			updateWorkingDaemons();
			// probably due to cancelled task cannot restart
			// we create a new timer every time since starts.
			this.worker = new FaultDetectorWorker(this);
			this.timerTaskExecutor = new Timer();
			this.timerTaskExecutor.scheduleAtFixedRate(worker,
					TwisterConstants.FAULT_DETECTION_INTERVAL,
					TwisterConstants.FAULT_DETECTION_INTERVAL);
			// new task executor
			this.taskExecutor = Executors.newFixedThreadPool(Runtime
					.getRuntime().availableProcessors());
		}
		this.isFaultDetectorWorkerStarted.set(true);
	}

	/**
	 * Maintain closing states
	 */
	synchronized void close() {
		if (this.isFaultDetectorWorkerStarted.get()) {
			this.isFaultDetectorWorkerStarted.set(false);
			// we clean the task here
			this.worker.cancel();
			this.timerTaskExecutor.purge();
			this.worker = null;
			this.timerTaskExecutor = null;
			// Disable new tasks from being submitted
			this.taskExecutor.shutdown();
			try {
				// processing task status should not be long
				if (!this.taskExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
					System.out.println("stop handling task status");
					// Cancel currently executing tasks
					this.taskExecutor.shutdownNow();
					// Wait a while for tasks to respond to being cancelled
					if (!this.taskExecutor
							.awaitTermination(5, TimeUnit.SECONDS)) {
						System.out
								.println("Handling task status did not terminate");
					}
				}
			} catch (InterruptedException e) {
				// (Re-)Cancel if current thread also interrupted
				this.taskExecutor.shutdownNow();
				// Preserve interrupt status
				Thread.currentThread().interrupt();
			}
			System.out.println("FaultDetector Task Executor: isShutDown? "
					+ this.taskExecutor.isShutdown() + " isTerminated? "
					+ this.taskExecutor.isTerminated());
		}
	}

	/**
	 * synchronize local daemon status and remote daemon real status Firstly
	 * close current detection and daemon status receiving Secondly restart the
	 * service to update daemon status
	 */
	synchronized void syncWorkingDaemons() {
		this.close();
		this.start();
	}
	
	/**
	 * return a view of working daemons by the fault detector, to present its
	 * own knowledge
	 * 
	 * @return
	 */
	synchronized List<Integer> getCurrentWorkingDaemonsView() {
		return new ArrayList<Integer>(this.workingDaemons);
	}

	synchronized boolean isHasFault() {
		if (!this.isFaultDetectorWorkerStarted.get()) {
			return false;
		}
		for (int daemonID : this.workingDaemons) {
			if (!this.daemonStatus.get(daemonID).isRunning()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * detect failures in working daemons which are the ones still running by
	 * detector's view
	 */
	private synchronized void detectFailures() {
		long detectionStartTime = System.currentTimeMillis();
		for (int daemonNo : this.workingDaemons) {
			DaemonStatus status = daemonStatus.get(daemonNo);
			if (status.isRunning()
					&& (System.currentTimeMillis() - status
							.getLastDetectionTime()) > TwisterConstants.MAX_WAIT_TIME_FOR_FAULT) {
				// check if the daemon really died
				DaemonInfo info = this.daemonInfo.get(daemonNo);
				String host = info.getIp();
				int port = info.getPort();
				int tryCount = 0;
				boolean fault = false;
				// final failure
				boolean failure = false;
				Socket socket =null;
				DataOutputStream dout = null;
				do {
					try {
						InetAddress addr = InetAddress.getByName(host);
						SocketAddress sockaddr = new InetSocketAddress(addr,
								port);
						socket = new Socket();
						int timeoutMs = TwisterConstants.CONNECT_MAX_WAIT_TIME;
						socket.connect(sockaddr, timeoutMs);
						dout = new DataOutputStream(socket.getOutputStream());
						dout.writeByte(TwisterConstants.FAULT_DETECTION);
						dout.flush();
						dout.close();
						socket.close();
						fault = false;
					} catch (Exception e) {
						e.printStackTrace();
						// tried all but no success
						if (tryCount < TwisterConstants.SEND_TRIES) {
							fault = true;
							tryCount++;
						} else {
							failure = true;
						}
					}
					// close connection first
					if (fault) {
						if (dout != null) {
							try {
								dout.close();
								dout = null;
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						if (socket != null) {
							try {
								socket.close();
								socket = null;
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				} while (fault && !failure);
				if (failure) {
					System.out.println("FAILURE DETECTED ### Daemon "
							+ daemonNo);
					status.setRunning(false);
					status.setLastDetectionTime(System.currentTimeMillis());
				} else {
					status.setRunning(true);
					status.setLastDetectionTime(System.currentTimeMillis());
				}
			}
		}
		long detectionTime = System.currentTimeMillis() - detectionStartTime;
		if (detectionTime > 60000) {
			System.out.println("Total detection Time: " + detectionTime
					+ " milliseconds");
		}
	}

	/**
	 * we use a thread here, to prevent daemonStatus is stopped by fault
	 * detection and block onEvent queue
	 * 
	 * @param status
	 */
	void handleDaemonStatus(TwisterMessage status) {
		if (this.isFaultDetectorWorkerStarted.get()) {
			HandleTaskStatus thread = new HandleTaskStatus(status);
			this.taskExecutor.execute(thread);
		}
	}

	private class HandleTaskStatus implements Runnable {
		private TwisterMessage msg;

		private HandleTaskStatus(TwisterMessage status) {
			msg = status;
		}

		@Override
		public void run() {
			processDaemonStatus(msg);
		}
	}
	
	/**
	 * set  daemon status from daemon report, 
	 * Assume msgType == TwisterConstants.DAEMON_STATUS and it is already been
	 * read
	 */
	synchronized void processDaemonStatus(TwisterMessage message) {
		try {
			DaemonStatusMessage status = new DaemonStatusMessage(message);
			// System.out.println("Status :" + status.getDaemonNo() + " "
			// + status.getHostIP());
			DaemonStatus daemonStatus = this.daemonStatus.get(status
					.getDaemonNo());
			if (daemonStatus != null) {
				daemonStatus.setRunning(true);
				// this could be delayed by other operations
				daemonStatus.setLastDetectionTime(System.currentTimeMillis());
			} else {
				logger.error("Invalid daemon no. Inconsistant runtime state.");
			}
		} catch (SerializationException e) {
			logger.error(e);
		}
	}
}
