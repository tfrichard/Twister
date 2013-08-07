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

package cgl.imr.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.message.CombineInput;
import cgl.imr.message.TaskStatus;

/**
 * Monitor the progress of MapReduce computation tasks.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 * @author zhangbj
 * 
 */
public class TwisterMonitorBasic implements TwisterConstants, TwisterMonitor {
	private static Logger logger = Logger.getLogger(TwisterMonitorBasic.class);
	private TwisterDriver driver;
	private Map<Integer, DaemonInfo> daemonInfo;
	private JobConf jobConf;
	private JobStatus jobStatus;
	// map monitoring
	private Set<Integer> mapDaemons;
	private long mapStartTime;
	private long mapEndTime;
	private long mapMaxTime;
	private int mapMaxDaemon;
	private Set<Integer> receivedMapTasks;
	private Map<Integer, AtomicInteger> mapDistribution;
	private long mapMinTime;
	private int mapMinDaemon;
	private long totalSequentialMapExecutionTime; // milliseconds
	// shuffle monitoring
	private int numShuffleTasks;
	private Set<Integer> shuffleDaemons;
	private AtomicBoolean isShuffleFailed;
	private long shuffleStartTime;
	private long shuffleEndTime;
	// reduce monitoring
	private Map<Integer, Integer> reduceInputMap;
	private Set<Integer> reduceDaemons;
	private long reduceStartTime;
	private long reduceEndTime;
	private long reduceMaxTime;
	private long reduceMinTime;
	private long totalSequentialReduceExecutionTime; // milliseconds
	// combine monitoring
	private int numCombineInput;
	private AtomicBoolean isCombineFailed;
	private long combineStartTime;
	private long combineEndTime;
	// task executor
	private ExecutorService taskExecutor;
	// monitoring states
	@SuppressWarnings("unused")
	private boolean hasMonitoringException;
	private TwisterException monitoringException;
	private AtomicBoolean isStarted;

	TwisterMonitorBasic(JobConf jobConf, Map<Integer, DaemonInfo> daemons,
			TwisterDriver driver) {
		this.driver = driver;
		this.daemonInfo = daemons;
		this.jobConf = jobConf;
		this.taskExecutor = null;
		this.resetMonitor();
	}

	/**
	 * start receiving message
	 */
	synchronized void start() {
		if (this.isStarted.get()) {
			return;
		}
		this.resetMonitor();
		this.mapStartTime = System.currentTimeMillis();
		this.isStarted.set(true);
	}

	/**
	 * end receiving message
	 */
	synchronized void close() {
		if (!this.isStarted.get()) {
			return;
		}
		this.isStarted.set(false);
	}

	private void resetMonitor() {
		this.jobStatus = new JobStatus();
		this.mapDaemons = Collections.synchronizedSet(new HashSet<Integer>());
		this.mapStartTime = 0;
		this.mapEndTime = 0;
		this.mapMaxTime = 0;
		this.mapMaxDaemon = -1;
		this.receivedMapTasks = new HashSet<Integer>();
		this.mapDistribution = new ConcurrentHashMap<Integer, AtomicInteger>();
		for (int i = 0; i < this.daemonInfo.size(); i++) {
			this.mapDistribution.put(i, new AtomicInteger(0));
		}
		this.mapMinTime = 0;
		this.mapMinDaemon = -1;
		this.totalSequentialMapExecutionTime = 0;
		this.numShuffleTasks = 0;
		this.shuffleDaemons = Collections
				.synchronizedSet(new HashSet<Integer>());
		this.isShuffleFailed = new AtomicBoolean(false);
		this.shuffleStartTime = 0;
		this.shuffleEndTime = 0;
		// This is the map to mark the reducerNo and reduceInput count
		this.reduceInputMap = new ConcurrentHashMap<Integer, Integer>();
		for (int i = 0; i < this.jobConf.getNumReduceTasks(); i++) {
			reduceInputMap.put(i, 0);
		}
		this.reduceDaemons = Collections
				.synchronizedSet(new HashSet<Integer>());
		this.reduceStartTime = 0;
		this.reduceEndTime = 0;
		this.reduceMaxTime = 0;
		this.reduceMinTime = 0;
		this.totalSequentialReduceExecutionTime = 0;
		this.numCombineInput = 0;
		this.isCombineFailed = new AtomicBoolean(false);
		this.combineStartTime = 0;
		this.combineEndTime = 0;
		if (this.taskExecutor != null) {
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
			System.out.println("Mointor Task Executor: isShutDown? "
					+ this.taskExecutor.isShutdown() + " isTerminated? "
					+ this.taskExecutor.isTerminated());
		}
		// new task executor
		this.taskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime()
				.availableProcessors());
		// monitoring states
		this.hasMonitoringException = false;
		this.monitoringException = null;
		this.isStarted = new AtomicBoolean(false);
	}

	/**
	 * Monitors till the completion of the job. check driver status, set driver
	 * and and do action here holds the main workflow while not complete sleep
	 * hasfault->repair // check condition and act
	 * 
	 * 
	 * @return job status.
	 * @throws TwisterException
	 */
	public JobStatus monitorTillCompletion() throws TwisterException {
		if (!this.isStarted.get()) {
			return this.jobStatus;
		}
		while (!this.driver.isRunMapReduceCompleted()) {
			try {
				// much longer than the former version
				Thread.sleep(TwisterConstants.MONITOR_SLEEP_TIME);
			} catch (InterruptedException e) {
				logger.error(e);
			}
			// this includes all the detailed processing
			// increase iteration, rest monitor and handle failures
			driver.checkAndHandleFailureInMonitoring();
			// check and act
			this.checkConditionAndAct();
		}
		System.out.println("Monitoring is completed.");
		this.driver.setIterationCompleted();
		return this.jobStatus;
	}

	/**
	 * Wait till the completion of the job or till the given number of minutes.
	 * 
	 * @param maxMinutes
	 *            - Maximum number of minutes to wait.
	 * @return job status
	 * @throws TwisterException
	 */
	public JobStatus monitorTillCompletion(int maxMinutes)
			throws TwisterException {
		if (!this.isStarted.get()) {
			return this.jobStatus;
		}
		while (!(this.driver.isRunMapReduceCompleted() || getElapsedTimeInMinutes() > maxMinutes)) {
			try {
				Thread.sleep(TwisterConstants.MONITOR_SLEEP_TIME);
			} catch (InterruptedException e) {
				logger.error(e);
			}
			// this includes all the detailed processing
			// increase iteration, rest monitor and handle failures
			// tempory disable
			//driver.checkAndHandleFailureInMonitoring();
			// check and act
			this.checkConditionAndAct();
		}
		if (this.driver.isRunMapReduceCompleted()) {
			this.driver.setIterationCompleted();
		} else {
			this.hasMonitoringException = true;
			this.monitoringException = new TwisterException(
					"Monitoring time exceeded");
			throw this.monitoringException;
		}
		System.out.println("Monitoring is completed.");
		return this.jobStatus;
	}

	private long getElapsedTimeInMinutes() {
		return System.currentTimeMillis() - this.mapStartTime;
	}

	/**
	 * check condition and do actions
	 * 
	 * @throws TwisterException
	 */
	synchronized void checkConditionAndAct() throws TwisterException {
		if (jobStatus.getNumSuccessfulMapTasks() == jobConf.getNumMapTasks()
				&& this.driver.isMapRunning()) {
			this.driver.setMapCompleted();
			System.out.println("Average MapTask Execution Time: "
					+ this.totalSequentialMapExecutionTime
					/ jobConf.getNumMapTasks() + " milliseconds.");
			System.out.println("Max Map Time: " + this.mapMaxTime + " "
					+ " Daemon ID: " + this.mapMaxDaemon);
			System.out.println("Min Map Time: " + this.mapMinTime + " "
					+ " Daemon ID: " + this.mapMinDaemon);
			this.mapEndTime = System.currentTimeMillis();
			System.out.println("Total Map Phrase Time: "
					+ (this.mapEndTime - this.mapStartTime) + " milliseconds.");
			if (this.jobConf.isHasReduceClass()) {
				this.numShuffleTasks = this.mapDaemons.size();
				this.shuffleStartTime = System.currentTimeMillis();
				this.driver.startShuffle();
			}
		} else if (this.jobStatus.getNumFailedMapTasks() > 0) {
			this.hasMonitoringException = true;
			this.monitoringException = new TwisterException("Map tasks failed");
			throw this.monitoringException;
		}
		// the daemons where map run should be the daemons where shuffle run
		if (this.numShuffleTasks == this.shuffleDaemons.size()
				&& this.driver.isShuffleRunning()) {
			this.driver.setShuffleCompleted();
			this.shuffleEndTime = System.currentTimeMillis();
			System.out
					.println("Total Shuffle Phrase Time (No Data Transfers): "
							+ (this.shuffleEndTime - this.shuffleStartTime)
							+ " milliseconds.");
			// we start reducers here
			this.reduceStartTime = System.currentTimeMillis();
			/*
			 * System.out.println("Reduce Input Map"); for (int key :
			 * reduceInputMap.keySet()) { System.out.println("Reducer No: " +
			 * key + " Input Size: " + reduceInputMap.get(key)); }
			 */
			this.driver.startReduce(reduceInputMap);
		} else if (this.isShuffleFailed.get()) {
			this.hasMonitoringException = true;
			this.monitoringException = new TwisterException(
					"Shuffle tasks failed");
			throw this.monitoringException;
		}
		if (jobStatus.getNumSuccessfulReduceTasks() == jobConf
				.getNumReduceTasks() && this.driver.isReducRunning()) {
			this.driver.setReduceCompleted();
			// reduce task status monitoring
			System.out.println("Average ReduceTask Execution Time: "
					+ this.totalSequentialReduceExecutionTime
					/ jobConf.getNumReduceTasks() + " milliseconds.");
			System.out.println("Max Reduce Time: " + this.reduceMaxTime + " "
					+ " Min Reduce Time: " + this.reduceMinTime);
			this.reduceEndTime = System.currentTimeMillis();
			System.out.println("Total Reduce Phrase Time: "
					+ (this.reduceEndTime - this.reduceStartTime));
			System.out.println("Total Shuffle&Reduce Phrase Time: "
					+ (this.reduceEndTime - this.shuffleStartTime)
					+ " milliseconds.");
			// If we have combiner, try to gather
			if (this.jobConf.isHasCombinerClass()) {
				this.numCombineInput = this.reduceDaemons.size();
				// this is for MPI Gather operation. Give each daemon a rank.
				List<Integer> daemons = (new ArrayList<Integer>(
						this.reduceDaemons));
				Collections.sort(daemons);
				Map<Integer, Integer> daemonRankMap = new HashMap<Integer, Integer>();
				int idCount = 0;
				for (int daemonID : daemons) {
					daemonRankMap.put(idCount, daemonID);
					idCount++;
				}
				this.combineStartTime = System.currentTimeMillis();
				driver.startGather(daemonRankMap,
						TwisterConstants.GATHER_IN_DIRECT);
			}
		} else if (this.jobStatus.getNumFailedReduceTasks() > 0) {
			this.hasMonitoringException = true;
			this.monitoringException = new TwisterException(
					"Reduce tasks failed");
			throw this.monitoringException;
		}
		if (this.numCombineInput == jobStatus.getNumCombineInputsReceived()
				&& this.driver.isGatherRunning()) {
			this.driver.setGatherCompleted();
			this.combineEndTime = System.currentTimeMillis();
			System.out.println("Total Combine Phrase Time: "
					+ (this.combineEndTime - this.combineStartTime)
					+ " milliseconds.");
		} else if (this.isCombineFailed.get()) {
			this.hasMonitoringException = true;
			this.monitoringException = new TwisterException(
					"Combiner encountered erros.");
			throw this.monitoringException;
		}
	}

	/**
	 * since all access to this object is synchronized, we don't want to hang
	 * there. when check and act is invoked, it is waiting for work-response.
	 * But work-response and task-status is in the same queue, but task-status
	 * is stopped by checkAndAct due to synchronization.
	 * 
	 * So as a result, checkAndAct waiting for response, response waiting for
	 * status, status waiting for checkAndAct
	 * 
	 * @param status
	 */
	void handleTaskStatus(TwisterMessage status) {
		HandleTaskStatus thread = new HandleTaskStatus(status);
		this.taskExecutor.execute(thread);
	}

	private class HandleTaskStatus implements Runnable {
		private TwisterMessage msg;

		private HandleTaskStatus(TwisterMessage status) {
			msg = status;
		}

		@Override
		public void run() {
			processTaskStatus(msg);
		}
	}

	/**
	 * This method receives all the monitoring related events and handles them
	 * appropriately.
	 * 
	 * 07/21/2011 ZBJ: I guess there is a bug caused by output from old
	 * iterations The code is tricky, in normal, during the monitoring, the
	 * client has a larger iteration number because once runmapreduce is
	 * executed, iteration++, but the tasks are still using the old iteration
	 * number I make some change to keep iteration number be consistent, and
	 * ignore the old one
	 */
	private synchronized void processTaskStatus(TwisterMessage message) {
		if (!this.isStarted.get()) {
			return;
		}
		TaskStatus status = null;
		try {
			status = new TaskStatus(message);
		} catch (SerializationException e) {
			status = null;
			this.monitoringException = new TwisterException(
					"Monitor encoutered errors.", e);
			e.printStackTrace();
		}
		if (status == null) {
			return;
		}
		if (!this.driver.isCurrentIteration(status.getIteration())) {
			String taskType = null;
			if (status.getTaskType() == MAP_TASK) {
				taskType = "MAP_TASK";
			} else if (status.getTaskType() == SHUFFLE_TASK) {
				taskType = "SHUFFLE_TASK";
			} else if (status.getTaskType() == REDUCE_TASK) {
				taskType = "REDUCE_TASK";
			} else {
				taskType = "UNKNOWN_TASK";
			}
			System.out.println(taskType + " response from iteration: "
					+ status.getIteration() + ". Ignore...");
			return;
		}
		if (status.getTaskType() == MAP_TASK) {
			checkMapTaskStatus(status);
		} else if (status.getTaskType() == SHUFFLE_TASK) {
			checkShuffleTaskStatus(status);
		} else if (status.getTaskType() == REDUCE_TASK) {
			checkReduceTaskStatus(status);
		}
	}

	private void checkMapTaskStatus(TaskStatus status) {
		if (status.getStatus() == SUCCESS) {
			// make sure no duplication
			if (!receivedMapTasks.contains(status.getTaskNo())) {
				receivedMapTasks.add(status.getTaskNo());
			} else {
				System.out.println("Duplicated Map Task Report!");
				return;
			}
			if (this.mapMaxTime < status.getExecuationTime()) {
				this.mapMaxTime = status.getExecuationTime();
				this.mapMaxDaemon = status.getDaemonNo();
			}
			if (this.mapMinTime == 0) {
				this.mapMinTime = status.getExecuationTime();
				this.mapMinDaemon = status.getDaemonNo();
			} else if (this.mapMinTime > status.getExecuationTime()) {
				this.mapMinTime = status.getExecuationTime();
				this.mapMinDaemon = status.getDaemonNo();
			}
			// set successful map task distribution
			AtomicInteger mapTaskOnDaemon = this.mapDistribution.get(status
					.getDaemonNo());
			if (mapTaskOnDaemon != null) {
				mapTaskOnDaemon.addAndGet(1);
			}
			this.mapDistribution.put(status.getDaemonNo(), mapTaskOnDaemon);
			// time monitoring
			this.totalSequentialMapExecutionTime += status.getExecuationTime();
			this.jobStatus.incrementSuccessfulMapTasks();
			this.mapDaemons.add(status.getDaemonNo());
			// show progress
			List<Integer> milestones = new ArrayList<Integer>();
			int numMileStones = 10;
			if (jobConf.getNumMapTasks() < numMileStones) {
				numMileStones = jobConf.getNumMapTasks();
			}
			for (int i = 1; i <= numMileStones; i++) {
				milestones.add(i * jobConf.getNumMapTasks() / numMileStones);
			}
			for (int milestone : milestones) {
				if (jobStatus.getNumSuccessfulMapTasks() == milestone) {
					long elapse = System.currentTimeMillis()
							- this.mapStartTime;
					System.out.println("Map Phrase current progress.  Time: "
							+ elapse + " milliseconds. " + " Percentage: "
							+ (double) jobStatus.getNumSuccessfulMapTasks()
							/ (double) jobConf.getNumMapTasks() * (double) 100
							+ " % ");
					/*
					 * System.out .println(
					 * "Slow Daemons. The Number of Current Successful Tasks: "
					 * + jobStatus.getNumSuccessfulMapTasks()); int
					 * avergeNumTasks = jobStatus.getNumSuccessfulMapTasks() /
					 * this.mapDistribution.keySet().size(); for (int id :
					 * this.mapDistribution.keySet()) { if
					 * (this.mapDistribution.get(id).get() < avergeNumTasks) {
					 * System.out.println("DaemonID: " + id +
					 * " Successful Map Tasks: " +
					 * this.mapDistribution.get(id).get()); } }
					 */
					break;
				}
			}
		} else if (status.getStatus() == FAILED) {
			this.jobStatus.incrementFailedMapTasks();
		}
		this.jobStatus.addMapTaskStatus(status);
	}

	/**
	 * get status, set reduceMap
	 */
	private void checkShuffleTaskStatus(TaskStatus status) {
		if (status.getStatus() == SUCCESS) {
			Map<Integer, Integer> reduceInputMapPerDaemon = status
					.getReduceInputMap();
			// add to current reduceInputMap
			for (int key : reduceInputMapPerDaemon.keySet()) {
				// we definitely can get the value, because, the map per daemon
				// is a small set of the whole map
				int value = reduceInputMap.get(key);
				value = value + reduceInputMapPerDaemon.get(key).intValue();
				reduceInputMap.put(key, value);
			}
			this.shuffleDaemons.add(status.getDaemonNo());
		} else if (status.getStatus() == FAILED) {
			this.isShuffleFailed.set(true);
		}
	}

	private void checkReduceTaskStatus(TaskStatus status) {
		if (status.getStatus() == SUCCESS) {
			if (this.reduceMaxTime < status.getExecuationTime()) {
				this.reduceMaxTime = status.getExecuationTime();
			}
			if (this.reduceMinTime == 0) {
				this.reduceMinTime = status.getExecuationTime();
			} else if (this.reduceMinTime > status.getExecuationTime()) {
				this.reduceMinTime = status.getExecuationTime();
			}
			this.totalSequentialReduceExecutionTime += status
					.getExecuationTime();
			this.jobStatus.incrementSuccessfulReduceTasks();
			this.reduceDaemons.add(status.getDaemonNo());
			// show progress
			List<Integer> milestones = new ArrayList<Integer>();
			int numMileStones = 10;
			if (jobConf.getNumReduceTasks() < numMileStones) {
				numMileStones = jobConf.getNumReduceTasks();
			}
			for (int i = 1; i <= numMileStones; i++) {
				milestones.add(i * jobConf.getNumReduceTasks() / numMileStones);
			}
			for (int milestone : milestones) {
				if (jobStatus.getNumSuccessfulReduceTasks() == milestone) {
					long elapse = System.currentTimeMillis()
							- this.reduceStartTime;
					System.out
							.println("Reduce Phrase current progress.  Time: "
									+ elapse
									+ " milliseconds. "
									+ " Percentage: "
									+ (double) jobStatus
											.getNumSuccessfulReduceTasks()
									/ (double) jobConf.getNumReduceTasks()
									* (double) 100 + " % ");
					break;
				}
			}
		} else if (status.getStatus() == FAILED) {
			this.jobStatus.incrementFailedReduceTasks();
		}
		this.jobStatus.addReduceTaskStatus(status);
	}

	/**
	 * combinetInput are processed by gatherer, so we get related information
	 * from there
	 * 
	 * @param combineInput
	 */
	synchronized void combinerInputReceived(CombineInput combineInput) {
		this.jobStatus.incrementCombineInputs();
	}

	/**
	 * let the monitor knows that the combine operation failed
	 */
	synchronized void combineInputFailed() {
		this.isCombineFailed.set(true);
	}
}
