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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.Combiner;
import cgl.imr.base.Key;
import cgl.imr.base.KeyValuePair;
import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.SerializationException;
import cgl.imr.base.Subscribable;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.base.impl.PubSubFactory;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.data.DataPartitionException;
import cgl.imr.data.file.FileData;
import cgl.imr.data.file.FileDataPartitioner;
import cgl.imr.data.file.PartitionFile;
import cgl.imr.message.EndJobRequest;
import cgl.imr.message.MapTaskRequest;
import cgl.imr.message.MapperRequest;
import cgl.imr.message.MemCacheClean;
import cgl.imr.message.MemCacheInput;
import cgl.imr.message.NewJobRequest;
import cgl.imr.message.PubSubMessage;
import cgl.imr.message.ReducerRequest;
import cgl.imr.message.StartGatherMessage;
import cgl.imr.message.StartReduceMessage;
import cgl.imr.message.StartShuffleMessage;
import cgl.imr.message.WorkerResponse;
import cgl.imr.types.IntValue;
import cgl.imr.types.MemCacheAddress;
import cgl.imr.types.StringKey;
import cgl.imr.util.TwisterCommonUtil;

/**
 * Client side driver for the MapReduce computations. This is a very important
 * class in Twister framework. Many extensions possible to add features such as
 * fault tolerance etc..
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 *         Fully rewrite and update
 * @author zhangbj
 * 
 */
public class TwisterDriver implements Subscribable, TwisterConstants,
		TwisterModel {
	private static Logger logger = Logger.getLogger(TwisterDriver.class);
	// Twister configuration
	private TwisterConfigurations mrConfig;
	// static daemon info and settings
	private Map<Integer, DaemonInfo> daemonInfo;
	// Twister driver shutdown hooker
	private ShutdownHook shutDownHook;
	// fault detector
	private FaultDetector faultDetector;
	// topics
	private String reduceTopicBase;
	private String combineTopic;
	private String responseTopic;
	// response map is used to as a mail box to put messages received
	private Map<String, WorkerResponse> responseMap;
	private PubSubService pubSubService;
	// job settings
	private JobConf jobConf;
	private JobState jobState;
	// current execution history and plan in this iteration
	private ExecutionPlan execPlan;
	// job related components
	private TwisterMonitorBasic monitor;
	private BroadCaster broadcaster;
	private Gatherer gatherer;
	private Combiner currentCombiner;
	// iteration control
	private AtomicInteger iterationCount;

	/**
	 * Constructor for the TwisterDriver. Takes the JobConf as the input and
	 * establishes a connection with the broker network. Then it proceeds to
	 * subscribe into the necessary topics and initialize various data
	 * structures necessary.
	 * 
	 * @param jobConf
	 *            - JobConf object relevant to this job.
	 * @throws TwisterException
	 */
	public TwisterDriver(JobConf jobConf) throws TwisterException {
		// parameter checking
		if (jobConf == null) {
			throw new TwisterException("Job Configuration cannot be null.");
		}
		// load local Twister Configuration and nodes information
		try {
			this.mrConfig = TwisterConfigurations.getInstance();
			this.daemonInfo = TwisterCommonUtil
					.getDaemonInfoFromConfigFiles(this.mrConfig);
		} catch (ConfigurationException e) {
			throw new TwisterException(
					"Could not load the Twister configurations. Please check if "
							+ TwisterConstants.PROPERTIES_FILE
							+ " exists in the classpath.", e);
		} catch (IOException e) {
			throw new TwisterException(
					"Can not read the nodes file. Please check if  the nodes file exists in the classpath.",
					e);
		}
		// output daemon info
		System.out.println("###Daemon Infomation###");
		for (int daemonID : this.daemonInfo.keySet()) {
			System.out.println("Daemon ID: " + daemonID + " IP: "
					+ this.daemonInfo.get(daemonID).getIp() + " "
					+ this.daemonInfo.get(daemonID).getPort());
		}
		System.out.println("###Daemon Infomation###");
		// add a shutdown hook, in case the program is canceled
		this.shutDownHook = new ShutdownHook(this);
		Runtime.getRuntime().addShutdownHook(new Thread(shutDownHook));
		// fault detector tool
		this.faultDetector = new FaultDetector(this.daemonInfo);
		// pubsub related service
		this.responseTopic = TwisterConstants.RESPONSE_TOPIC_BASE + "/"
				+ jobConf.getJobId();
		this.reduceTopicBase = TwisterConstants.REDUCE_TOPIC_BASE + "/"
				+ jobConf.getJobId();
		this.combineTopic = TwisterConstants.COMBINE_TOPIC_BASE
				+ jobConf.getJobId();
		this.responseMap = new ConcurrentHashMap<String, WorkerResponse>();
		// pub/sub service initialization
		try {
			// random entity ID for the driver
			// it starts from 100000 and above
			int entityId = new Random(System.currentTimeMillis()).nextInt() * 1000000;
			this.pubSubService = PubSubFactory.getPubSubService(mrConfig,
					EntityType.DRIVER, entityId);
			this.pubSubService.setSubscriber(this);
			this.pubSubService.subscribe(responseTopic);
			this.pubSubService.subscribe(combineTopic);
			if (jobConf.isFaultTolerance()) {
				this.pubSubService
						.subscribe(TwisterConstants.DAEMON_STATUS_TOPIC);
				this.faultDetector.start();
			}
			this.pubSubService.start();
		} catch (PubSubException e) {
			e.printStackTrace();
			throw new TwisterException(
					"Can not start Publish/Subscribe service.", e);
		}
		System.out.println("Twister Job ID: " + jobConf.getJobId());
		// job settings initialization
		this.jobConf = jobConf;
		// job state initialization
		this.jobState = JobState.NOT_CONFIGURED;
		this.execPlan = new ExecutionPlan();
		// monitor initialization
		this.monitor = new TwisterMonitorBasic(jobConf, this.daemonInfo, this);
		// broadcasting tool for bcast data
		this.broadcaster = new BroadCaster(jobConf, daemonInfo, faultDetector,
				pubSubService, responseMap);
		// gather tool to collect combineInput
		// we set it with combiner
		this.gatherer = null;
		// this.gatherer = new Gatherer(jobConf, daemonInfo, monitor, this);
		// iteration control
		this.iterationCount = new AtomicInteger(0);
		// initialize the job
		sendNewJobRequest();
	}

	/**
	 * Send a new job. At daemon side, this will create a class loader for the
	 * job. We throw exceptions to let the driver program handles it.
	 * 
	 * @param jobConf
	 * @throws TwisterException
	 */
	private void sendNewJobRequest() throws TwisterException {
		if (this.jobState.ordinal() >= JobState.INITIATED.ordinal()) {
			throw new TwisterException("Maps can not be configured twice.");
		}
		logger.info("Send a new job.");
		this.jobState = JobState.INITIALIZING;
		SendRecvResponse response = sendNewJobRequestInternal();
		this.jobState = JobState.INITIATED;
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in sending a new job.");
		}
		// with exception, probably remote code goes wrong
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"Remote daemon exceptions in sending a new job.");
		}
		// for fault, we clean and re-do.
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return;
		}
		// timeout and no fault found, throw to user
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException(
					"No responses from remote daemons in sending a new job.");
		}
	}

	/**
	 * Here notes the difference between outside and internal function. The
	 * internal one focuses on the operation. the outside one focuses on the
	 * accessing state checking and leaving state checking.
	 * 
	 * @return
	 */
	private SendRecvResponse sendNewJobRequestInternal() {
		NewJobRequest jobRequest = new NewJobRequest(this.jobConf.getJobId(),
				this.jobConf.getFriendJobList(), this.responseTopic);
		SendRecvResponse response = this.broadcaster
				.bcastNewJobRequestsAndReceiveResponses(jobRequest);
		return response;
	}

	/**
	 * Configure Map tasks without any parameters, we set the core task in
	 * configureMapsInternal. But check entering and exiting states here.
	 * 
	 * Here shows the basic idea of fault recovering, firstly we do it, if
	 * failure detected, we redo it.
	 * 
	 * @see cgl.imr.client.TwisterModel#configureMaps()
	 */
	public void configureMaps() throws TwisterException {
		if (this.jobState.ordinal() >= JobState.MAP_CONFIGURING.ordinal()) {
			throw new TwisterException("Maps can not be configured twice.");
		}
		if (this.jobState.ordinal() < JobState.INITIATED.ordinal()) {
			throw new TwisterException("Job is not initialized yet..");
		}
		logger.info("Configure Mappers.");
		this.jobState = JobState.MAP_CONFIGURING;
		SendRecvResponse response = configureMapsInternal();
		// execution record
		this.execPlan.setMapConfigured();
		this.jobState = JobState.MAP_CONFIGURED;
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in configureMaps().");
		}
		// Exception from remote code
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"ConfigureMaps produced errors at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException("Time out  in configureMaps().");
		}
		logger.info("Configuring Mappers through the partition file is completed. ");
	}

	/**
	 * the internal configuring Maps for configureMaps()
	 * 
	 * @return
	 * @throws TwisterException
	 */
	private SendRecvResponse configureMapsInternal() throws TwisterException {
		if (!this.jobConf.isHasMapClass()) {
			throw new TwisterException("No map class available.");
		}
		if (this.jobConf.getNumMapTasks() <= 0) {
			throw new TwisterException("No map tasks available.");
		}
		List<Integer> availableDaemons = faultDetector
				.getCurrentWorkingDaemonsView();
		int numAvailableDaemons = availableDaemons.size();
		// throw if no daemons available
		if (numAvailableDaemons == 0) {
			throw new TwisterException("No daemons available.");
		}
		Map<Integer, TaskAssignment> mapTasksMap = new HashMap<Integer, TaskAssignment>();
		for (int m = 0; m < this.jobConf.getNumMapTasks(); m++) {
			// conf->request->assignment
			MapperConf mapperConf = new MapperConf(m);
			MapperRequest mapperRequest = new MapperRequest(this.jobConf,
					mapperConf, iterationCount.get());
			// topic base settings
			mapperRequest.setReduceTopicBase(this.reduceTopicBase);
			mapperRequest.setResponseTopic(this.responseTopic);
			// simply balance the task workload
			TaskAssignment mapAssignment = new TaskAssignment(mapperRequest,
					availableDaemons.get(m % numAvailableDaemons));
			mapTasksMap.put(m, mapAssignment);
		}
		// when the re-execution is necessary, we just re-do this function
		// and update the map directly.
		this.execPlan.setMapTasksMap(mapTasksMap);
		SendRecvResponse sendRecvResponse = this.broadcaster
				.sendAllRequestsAndReceiveResponses(mapTasksMap);
		return sendRecvResponse;
	}

	/**
	 * configure Maps through partition file
	 */
	public void configureMaps(String partitionFile) throws TwisterException {
		if (this.jobState.ordinal() >= JobState.MAP_CONFIGURING.ordinal()) {
			throw new TwisterException("Maps can not be configured twice.");
		}
		if (this.jobState.ordinal() < JobState.INITIATED.ordinal()) {
			throw new TwisterException("Job is not initialized yet..");
		}
		logger.info("Configure Mappers through the partition file, please wait.....");
		this.jobState = JobState.MAP_CONFIGURING;
		SendRecvResponse response = configureMapsInternal(partitionFile);
		// execution record
		this.execPlan.setMapConfigured();
		this.execPlan.setPartitionFile(partitionFile);
		this.jobState = JobState.MAP_CONFIGURED;
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in configureMaps(String partitionFile).");
		}
		// Exception from remote code, clean and terminate remote job,
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"configureMaps(String partitionFile) produced exceptions at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException(
					"Time out  in configureMaps(String partitionFile).");
		}
		logger.info("Configuring Mappers through the partition file is completed. ");
	}

	/**
	 * Configure Maps internal from partition file
	 * 
	 * @param partitionFile
	 * @return
	 * @throws TwisterException
	 */
	private SendRecvResponse configureMapsInternal(String partitionFile)
			throws TwisterException {
		if (!this.jobConf.isHasMapClass()) {
			throw new TwisterException("No map class available.");
		}
		if (this.jobConf.getNumMapTasks() <= 0) {
			throw new TwisterException("No map tasks available.");
		}
		List<Integer> availableDaemons = faultDetector
				.getCurrentWorkingDaemonsView();
		// throw if no daemons available
		if (availableDaemons.size() == 0) {
			throw new TwisterException("No working daemons available.");
		}
		// load partition file
		PartitionFile partitions = null;
		try {
			partitions = new PartitionFile(partitionFile);
		} catch (DataPartitionException e) {
			throw new TwisterException(e);
		}
		// check if the partition is equal to the number of Map tasks
		if (this.jobConf.getNumMapTasks() != partitions.getNumberOfFiles()) {
			throw new TwisterException(
					"Number of maps should be equal to the number of data partions.");
		}
		// set partition and daemons mapping
		Map<String, Integer> partitionsAndDaemons = null;
		try {
			partitionsAndDaemons = FileDataPartitioner
					.assignPartitionsToDaemons(availableDaemons, partitions);
		} catch (DataPartitionException e) {
			throw new TwisterException(e);
		}
		// check if we mapped all the partitions and daemons
		if (partitionsAndDaemons.size() != partitions.getNumberOfFiles()) {
			throw new TwisterException(
					"Not all partitions are mapped with daemons.");
		}
		int taskNo = 0;
		Map<Integer, TaskAssignment> mapTasksMap = new HashMap<Integer, TaskAssignment>();
		// mapperConf -> mapperRequest -> mapAssignment
		for (String dataFile : partitionsAndDaemons.keySet()) {
			int assignedDaemon = partitionsAndDaemons.get(dataFile);
			FileData fileData = new FileData(dataFile);
			MapperConf mapperConf = new MapperConf(taskNo, fileData);
			MapperRequest mapperRequest = new MapperRequest(jobConf,
					mapperConf, iterationCount.get());
			mapperRequest.setReduceTopicBase(reduceTopicBase);
			mapperRequest.setResponseTopic(responseTopic);
			TaskAssignment mapAssignment = new TaskAssignment(mapperRequest,
					assignedDaemon);
			mapTasksMap.put(taskNo, mapAssignment);
			taskNo++;
		}
		this.execPlan.setMapTasksMap(mapTasksMap);
		SendRecvResponse sendRecvResponse = this.broadcaster
				.sendAllRequestsAndReceiveResponses(mapTasksMap);
		return sendRecvResponse;
	}

	/**
	 * configure Maps with a List of values
	 */
	public void configureMaps(List<Value> values) throws TwisterException {
		if (this.jobState.ordinal() >= JobState.MAP_CONFIGURING.ordinal()) {
			throw new TwisterException("Maps can not be configured twice.");
		}
		if (this.jobState.ordinal() < JobState.INITIATED.ordinal()) {
			throw new TwisterException("Job is not initialized yet..");
		}
		logger.info("Configure Mappers through the values ");
		this.jobState = JobState.MAP_CONFIGURING;
		SendRecvResponse response = configureMapsInternal(values);
		this.execPlan.setMapConfigured();
		this.execPlan.setMapConfigurations(values);
		this.jobState = JobState.MAP_CONFIGURED;
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in configureMaps(Value[] values).");
		}
		// Exception from remote code, clean and terminate remote job,
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"configureMaps(List<Value>) produced exceptions at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException(
					"Time out  in configureMaps(String partitionFile).");
		}
		logger.info("Configuring Mappers through List<Value> is completed. ");
	}

	/**
	 * configure Map task internal
	 * 
	 * @see cgl.imr.client.TwisterModel#configureMaps(cgl.imr.base.Value[])
	 */
	private SendRecvResponse configureMapsInternal(List<Value> values)
			throws TwisterException {
		if (!this.jobConf.isHasMapClass()) {
			throw new TwisterException("No map class available.");
		}
		if (this.jobConf.getNumMapTasks() <= 0) {
			throw new TwisterException("No map tasks available.");
		}
		List<Integer> avaiableDaemons = faultDetector
				.getCurrentWorkingDaemonsView();
		int numAvailableDaemons = avaiableDaemons.size();
		if (numAvailableDaemons == 0) {
			throw new TwisterException("No working daemons available.");
		}
		if (values.size() != this.jobConf.getNumMapTasks()) {
			throw new TwisterException(
					"Number of values should be equal to the number of map tasks.");
		}
		Map<Integer, TaskAssignment> mapTasksMap = new HashMap<Integer, TaskAssignment>();
		for (int m = 0; m < this.jobConf.getNumMapTasks(); m++) {
			MapperConf mapperConf = new MapperConf(m, values.get(m));
			MapperRequest mapperRequest = new MapperRequest(jobConf,
					mapperConf, iterationCount.get());
			mapperRequest.setReduceTopicBase(this.reduceTopicBase);
			mapperRequest.setResponseTopic(this.responseTopic);
			TaskAssignment mapAssignment = new TaskAssignment(mapperRequest,
					avaiableDaemons.get(m % numAvailableDaemons));
			mapTasksMap.put(m, mapAssignment);
		}
		this.execPlan.setMapTasksMap(mapTasksMap);
		/*
		 * SendRecvResponse sendRecvResponse = this.broadcaster
		 * .sendAllRequestsAndReceiveResponses(mapTasksMap);
		 */
		// since value object could be large
		SendRecvResponse sendRecvResponse = this.broadcaster
				.sendAllRequestsAndReceiveResponsesByTCP(mapTasksMap,
						TwisterConstants.MAPPER_REQUEST);
		return sendRecvResponse;
	}

	/**
	 * configure reducers check if we can access the method, and check the exit
	 * status
	 * 
	 * @see cgl.imr.client.TwisterModel#configureReduce(cgl.imr.base.Value[])
	 */
	public void configureReduce(List<Value> values) throws TwisterException {
		if (this.jobState.ordinal() >= JobState.REDUCE_CONFIGURING.ordinal()) {
			throw new TwisterException("Reducers can not be configured twice.");
		}
		if (this.jobState.ordinal() < JobState.MAP_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"Maps haven't been configured yet, cannot go to reduce configuration");
		}
		logger.info("Configure Reducers");
		jobState = JobState.REDUCE_CONFIGURING;
		SendRecvResponse response = configureReduceInternal(values);
		execPlan.setReduceConfigured();
		execPlan.setReduceConfigurations(values);
		jobState = JobState.REDUCE_CONFIGURED;
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in configureReduce(List<Value>).");
		}
		// Exception from remote code, clean and terminate remote job,
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"configureReduce(List<Value>) produced exceptions at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException(
					"Time out  in configureReduce(List<Value>).");
		}
		logger.info("Configuring Mappers through List<Value> is completed. ");
	}

	/**
	 * Used for configuring reduce tasks. Possible bug: Reduce assignment is
	 * done based on the daemon no. Then take the available daemons to use %.
	 * Since reduceNo is distributed among daemons. no 1 is on daemon 1, no 2 is
	 * on daemon 2.... then bcastReducer through share the same topic
	 * jobConf.getRowBCastTopic() +
	 * reduceRequest.getReduceConf().getReduceTaskNo() /
	 * jobConf.getSqrtReducers(); they are on different nodes, is this correct?
	 * 
	 * @param values
	 * @throws TwisterException
	 */
	private SendRecvResponse configureReduceInternal(List<Value> values)
			throws TwisterException {
		if (values != null) {
			if (values.size() != this.jobConf.getNumReduceTasks()) {
				throw new TwisterException(
						"Number of values should be equal to the number of reduce tasks.");
			}
		}
		if (!this.jobConf.isHasReduceClass()) {
			throw new TwisterException("No reducer class is available.");
		}
		if (this.jobConf.getNumReduceTasks() <= 0) {
			throw new TwisterException("No reduce tasks available.");
		}
		List<Integer> avaiableDaemons = faultDetector
				.getCurrentWorkingDaemonsView();
		int numAvailableDaemons = avaiableDaemons.size();
		Map<Integer, TaskAssignment> reduceTasksMap = new HashMap<Integer, TaskAssignment>();
		for (int reduceTaskNo = 0; reduceTaskNo < this.jobConf
				.getNumReduceTasks(); reduceTaskNo++) {
			ReducerConf reducerConf = null;
			if (values != null) {
				reducerConf = new ReducerConf(reduceTaskNo,
						values.get(reduceTaskNo));
			} else {
				reducerConf = new ReducerConf(reduceTaskNo);
			}
			String topicForReduceTask = reduceTopicBase + reduceTaskNo;
			ReducerRequest reudceExecutorRequest = new ReducerRequest(jobConf,
					reducerConf, topicForReduceTask, responseTopic,
					combineTopic, iterationCount.get());
			TaskAssignment reduceAssignment = new TaskAssignment(
					reudceExecutorRequest, avaiableDaemons.get(reduceTaskNo
							% numAvailableDaemons));
			reduceTasksMap.put(reduceTaskNo, reduceAssignment);
		}
		this.execPlan.setReduceTasksMap(reduceTasksMap);
		SendRecvResponse sendRecvResponse = this.broadcaster
				.sendAllRequestsAndReceiveResponses(reduceTasksMap);
		return sendRecvResponse;
	}

	/**
	 * Configure the current combiner to use with this MapReduce computation.
	 * 
	 * @throws TwisterException
	 */
	private void configureCombiner() throws TwisterException {
		if (this.jobState.ordinal() >= JobState.COMBINE_CONFIGURING.ordinal()) {
			throw new TwisterException("Reducers can not be configured twice.");
		}
		if (this.jobState.ordinal() < JobState.REDUCE_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"Reducers haven't been configured yet, cannot go to combiner configuration");
		}
		logger.info("Configure combiner");
		jobState = JobState.COMBINE_CONFIGURING;
		configureCombinerInternal();
		execPlan.setCombinerConfigured();
		jobState = JobState.COMBINE_CONFIGURED;
	}

	/**
	 * the internal of combiner configuration
	 * 
	 * @throws TwisterException
	 */
	private void configureCombinerInternal() throws TwisterException {
		if (!this.jobConf.isHasCombinerClass()) {
			throw new TwisterException("No combiner class is available");
		}
		try {
			Class<?> combinerClass = Class.forName(jobConf.getCombinerClass());
			this.currentCombiner = (Combiner) combinerClass.newInstance();
			this.currentCombiner.configure(jobConf);
			configureGathererInternal();
		} catch (Exception e) {
			throw new TwisterException("Could not load combiner class.", e);
		}
	}
	
	private void configureGathererInternal() {
		this.gatherer = new Gatherer(this.jobConf, this.daemonInfo,
				this.currentCombiner, this.monitor, this);
	}

	/**
	 * add a value object to distributed memory cache, we still call
	 * addToMemCache(List<Value> values) internal
	 * 
	 * @see cgl.imr.client.TwisterModel#addToMemCache(cgl.imr.base.Value)
	 */
	public MemCacheAddress addToMemCache(Value value) throws TwisterException {
		List<Value> values = new ArrayList<Value>();
		values.add(value);
		return addToMemCache(values);
	}

	/**
	 * Add a list of values to MemCache
	 */
	public MemCacheAddress addToMemCache(List<Value> values)
			throws TwisterException {
		if (this.jobState.ordinal() >= JobState.MAP_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.MAP_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"Adding memcache can not be done during map configuration.");
		}
		if (this.jobState.ordinal() >= JobState.REDUCE_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.REDUCE_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"Adding memcache can not be done during reduce configuration.");
		}
		if (this.jobState.ordinal() >= JobState.COMBINE_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.COMBINE_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"Adding memcache can not be done during combine configuration.");
		}
		if (this.jobState.ordinal() >= JobState.MAP_STARTED.ordinal()
				&& jobState.ordinal() < JobState.COMBINE_COMPLETE.ordinal()) {
			throw new TwisterException(
					"Adding memcache can not be done during MapReduce job running.");
		}
		if (this.jobState.ordinal() < JobState.INITIATED.ordinal()) {
			throw new TwisterException(
					"Job hasn't been initialized yet, cannot go to MemCache adding");
		}
		if (this.execPlan.isMemCacheAdded()) {
			throw new TwisterException("MemCache is already been added.");
		}
		logger.info("Adding MemCache");
		// create a MemCache map
		// the key is MemCache address, the value is the data
		Map<String, Value> keyValues = new HashMap<String, Value>();
		String keyBase = UUIDGenerator.getInstance().generateRandomBasedUUID()
				.toString();
		int count = 0;
		for (Value value : values) {
			String key = keyBase + count;
			keyValues.put(key, value);
			count++;
		}
		// create a MemCache Address object
		MemCacheAddress address = new MemCacheAddress(keyBase, 0, values.size());
		SendRecvResponse response = addToMemCacheInternal(address, keyValues);
		execPlan.setMemCacheAdded(true);
		execPlan.setMemCacheAddress(address);
		execPlan.setMemCachedData(keyValues);
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in AddToMemCache.");
		}
		// Exception from remote code, clean and terminate remote job,
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"AddToMemCache produced exceptions at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return address;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException("Time out in AddToMemCache.");
		}
		logger.info("AddToMemCache is completed. ");
		return address;
	}

	/**
	 * we need to to change this function to support a list of values
	 * 
	 * @param keyValues
	 * @param methodType
	 * @return
	 * @throws TwisterException
	 */
	private SendRecvResponse addToMemCacheInternal(MemCacheAddress address,
			Map<String, Value> keyValues) throws TwisterException {
		List<PubSubMessage> cacheInputs = new ArrayList<PubSubMessage>();
		for (String key : keyValues.keySet()) {
			MemCacheInput cacheInput = new MemCacheInput(jobConf.getJobId(),
					address.getMemCacheKeyBase(), address.getRange(), key,
					keyValues.get(key));
			cacheInput.setResponseTopic(responseTopic);
			cacheInputs.add(cacheInput);
		}
		SendRecvResponse response = this.broadcaster
				.bcastRequestsAndReceiveResponses(cacheInputs);
		return response;
	}

	/**
	 * clean MemCache added
	 * 
	 * @see cgl.imr.client.TwisterModel#cleanMemCache(java.lang.String)
	 */
	public void cleanMemCache() throws TwisterException {
		if (this.jobState.ordinal() < JobState.INITIATED.ordinal()) {
			throw new TwisterException(
					"Job hasn't been initialized yet, cannot go to MemCache cleaning");
		}
		if (this.jobState.ordinal() >= JobState.MAP_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.MAP_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"Adding memcache can not be done during map configuration.");
		}
		if (this.jobState.ordinal() >= JobState.REDUCE_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.REDUCE_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"Adding memcache can not be done during reduce configuration.");
		}
		if (this.jobState.ordinal() >= JobState.COMBINE_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.COMBINE_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"Adding memcache can not be done during combine configuration.");
		}
		if (this.jobState.ordinal() >= JobState.MAP_STARTED.ordinal()
				&& jobState.ordinal() < JobState.COMBINE_COMPLETE.ordinal()) {
			throw new TwisterException(
					"Adding memcache can not be done during MapReduce job running.");
		}
		logger.info("Cleaning MemCache");
		SendRecvResponse response = cleanMemCacheInternal();
		execPlan.setMemCacheAdded(false);
		execPlan.setMemCacheAddress(null);
		execPlan.setMemCachedData(null);
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in cleanMemCache.");
		}
		// Exception from remote code, clean and terminate remote job,
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"cleanMemCache produced exceptions at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			// in re-execution, since we just set memcache be null
			// we just don't add memcache again, not redo the process of adding
			// and deleting
			this.reSubmitByPlan();
			return;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException("Time out in cleanMemCache.");
		}
		logger.info("cleanMemCache is completed. ");
	}

	private SendRecvResponse cleanMemCacheInternal() throws TwisterException {
		MemCacheClean cacheClean = new MemCacheClean(jobConf.getJobId(),
				this.responseTopic);
		SendRecvResponse response = this.broadcaster
				.bcastRequestsAndReceiveResponses(cacheClean);
		return response;
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see cgl.imr.client.TwisterModel#runMapReduce()
	 */
	public TwisterMonitor runMapReduce() throws TwisterException {
		// entrance permission
		if (this.jobState.ordinal() < JobState.MAP_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"Map tasks havn't been configured yet, cannot run MapReduce job.");
		}
		if (this.jobState.ordinal() >= JobState.REDUCE_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.REDUCE_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"runMapReduce can not be done during reduce configuration.");
		}
		if (this.jobState.ordinal() >= JobState.COMBINE_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.COMBINE_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"runMapReduce can not be done during combine configuration.");
		}
		if (this.jobState.ordinal() >= JobState.MAP_STARTING.ordinal()
				&& this.jobState.ordinal() <= JobState.COMBINE_COMPLETE
						.ordinal()) {
			throw new TwisterException(
					"Job has been submitted, cannot run it twice.");
		}
		// see if reducer and combiner configured
		if (this.jobConf.isHasReduceClass()
				&& !this.execPlan.isReduceConfigured()) {
			configureReduce(null);
		}
		if (this.jobConf.isHasCombinerClass()
				&& !this.execPlan.isCombinerConfigured()) {
			configureCombiner();
		}
		logger.info("Starting Map Tasks.");
		this.jobState = JobState.MAP_STARTING;
		this.monitor.start();
		SendRecvResponse response = runMapReduceInternal();
		jobState = JobState.MAP_STARTED;
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in runMapReduce().");
		}
		// Exception from remote code
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"Sending Map tasks produced errors at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return this.monitor;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException("Time out  in runMapReduce().");
		}
		logger.info("Starting Map tasks  is completed. ");
		return this.monitor;
	}

	private SendRecvResponse runMapReduceInternal() {
		Map<Integer, TaskAssignment> mapTasksMap = new HashMap<Integer, TaskAssignment>();
		for (int i = 0; i < this.jobConf.getNumMapTasks(); i++) {
			MapTaskRequest mapRequest = new MapTaskRequest(i,
					iterationCount.get());
			mapRequest.addKeyValue(new StringKey(jobConf.getJobId() + i),
					new IntValue(i));
			mapRequest.setJobId(this.jobConf.getJobId());
			mapRequest.setResponseTopic(this.responseTopic);
			TaskAssignment mapAssignment = new TaskAssignment(mapRequest,
					this.execPlan.getMapTasksMap().get(i));
			mapTasksMap.put(i, mapAssignment);
		}
		SendRecvResponse response = this.broadcaster
				.sendAllRequestsAndReceiveResponses(mapTasksMap);
		return response;
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see cgl.imr.client.TwisterModel#runMapReduce(java.util.List)
	 */
	public TwisterMonitor runMapReduce(List<KeyValuePair> pairs)
			throws TwisterException {
		// entrance permission
		if (this.jobState.ordinal() < JobState.MAP_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"Map tasks havn't been configured yet, cannot run MapReduce job.");
		}
		if (this.jobState.ordinal() >= JobState.REDUCE_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.REDUCE_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"runMapReduce(List<KeyValuePair>) can not be done during reduce configuration.");
		}
		if (this.jobState.ordinal() >= JobState.COMBINE_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.COMBINE_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"runMapReduce(List<KeyValuePair>) can not be done during combine configuration.");
		}
		if (this.jobState.ordinal() >= JobState.MAP_STARTING.ordinal()
				&& this.jobState.ordinal() <= JobState.COMBINE_COMPLETE
						.ordinal()) {
			throw new TwisterException(
					"Job has been submitted, cannot run it twice.");
		}
		// see if reducer and combiner configured
		if (this.jobConf.isHasReduceClass()
				&& !this.execPlan.isReduceConfigured()) {
			configureReduce(null);
		}
		if (this.jobConf.isHasCombinerClass()
				&& !this.execPlan.isCombinerConfigured()) {
			configureCombiner();
		}
		logger.info("Starting Map Tasks.");
		this.jobState = JobState.MAP_STARTING;
		this.monitor.start();
		SendRecvResponse response = runMapReduceInternal(pairs);
		this.execPlan.setKeyValuePair(pairs);
		jobState = JobState.MAP_STARTED;
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in runMapReduce(List<KeyValuePair>).");
		}
		// Exception from remote code
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"Sending Map tasks produced errors at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return this.monitor;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException(
					"Time out  in runMapReduce(List<KeyValuePair>).");
		}
		logger.info("Starting Map tasks  is completed. ");
		return this.monitor;
	}

	private SendRecvResponse runMapReduceInternal(List<KeyValuePair> pairs) {
		List<Map<Key, Value>> keyValueGroups = partitionKeyValuesToMapTasks(pairs);
		Map<Integer, TaskAssignment> mapTasksMap = new HashMap<Integer, TaskAssignment>();
		for (int i = 0; i < this.jobConf.getNumMapTasks(); i++) {
			MapTaskRequest mapRequest = new MapTaskRequest(i,
					iterationCount.get());
			mapRequest.setJobId(this.jobConf.getJobId());
			mapRequest.setKeyValues(keyValueGroups.get(i));
			mapRequest.setResponseTopic(this.responseTopic);
			TaskAssignment mapAssignment = new TaskAssignment(mapRequest,
					this.execPlan.getMapTasksMap().get(i));
			mapTasksMap.put(i, mapAssignment);
		}
		SendRecvResponse response = this.broadcaster
				.sendAllRequestsAndReceiveResponsesByTCP(mapTasksMap,
						TwisterConstants.MAP_TASK_REQUEST);
		return response;
	}

	/**
	 * Try to partition tasks to available nodes. The logic simply try to assign
	 * tasks to nodes equally or nearly equally.
	 * 
	 * @param values
	 *            - Array of values.
	 * @return
	 */
	private List<Map<Key, Value>> partitionKeyValuesToMapTasks(
			List<KeyValuePair> pairs) {
		List<Map<Key, Value>> keyValueGroups = new ArrayList<Map<Key, Value>>();
		int numMapTasks = this.jobConf.getNumMapTasks();
		int numPairs = pairs.size();
		int perMap = numPairs / numMapTasks;
		int remainder = numPairs % numMapTasks;
		int currentMapSize = perMap;
		int offset = 0;
		Map<Key, Value> perMapTask = null;
		KeyValuePair pair = null;
		for (int i = 0; i < numMapTasks; i++) {
			currentMapSize = perMap;
			if (remainder > 0) {
				currentMapSize++;
				remainder--;
			}
			perMapTask = new HashMap<Key, Value>();
			for (int j = 0; j < currentMapSize; j++) {
				pair = pairs.get(offset + j);
				perMapTask.put(pair.getKey(), pair.getValue());
			}
			keyValueGroups.add(perMapTask);
			offset += currentMapSize;
		}
		return keyValueGroups;
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see cgl.imr.client.TwisterModel#runMapReduceBCast(cgl.imr.base.Value)
	 */
	public TwisterMonitor runMapReduceBCast(Value val) throws TwisterException {
		// entrance permission
		if (this.jobState.ordinal() < JobState.MAP_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"Map tasks havn't been configured yet, cannot run MapReduce job.");
		}
		if (this.jobState.ordinal() >= JobState.REDUCE_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.REDUCE_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"runMapReduce(List<KeyValuePair>) can not be done during reduce configuration.");
		}
		if (this.jobState.ordinal() >= JobState.COMBINE_CONFIGURING.ordinal()
				&& jobState.ordinal() < JobState.COMBINE_CONFIGURED.ordinal()) {
			throw new TwisterException(
					"runMapReduce(List<KeyValuePair>) can not be done during combine configuration.");
		}
		if (this.jobState.ordinal() >= JobState.MAP_STARTING.ordinal()
				&& this.jobState.ordinal() <= JobState.COMBINE_COMPLETE
						.ordinal()) {
			throw new TwisterException(
					"Job has been submitted, cannot run it twice.");
		}
		// see if reducer and combiner configured
		if (this.jobConf.isHasReduceClass()
				&& !this.execPlan.isReduceConfigured()) {
			configureReduce(null);
		}
		if (this.jobConf.isHasCombinerClass()
				&& !this.execPlan.isCombinerConfigured()) {
			configureCombiner();
		}
		logger.info("Starting Map Tasks.");
		this.jobState = JobState.MAP_STARTING;
		this.monitor.start();
		SendRecvResponse response = runMapReduceInternal(val);
		this.execPlan.seBcastValue(val);
		jobState = JobState.MAP_STARTED;
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in runMapReduce(Value).");
		}
		// Exception from remote code
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"Sending Map tasks produced errors at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return this.monitor;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException("Time out  in runMapReduce(Value).");
		}
		logger.info("Starting Map tasks  is completed. ");
		return this.monitor;
	}

	private SendRecvResponse runMapReduceInternal(Value bcastValue) {
		Map<Integer, TaskAssignment> mapTasksMap = new HashMap<Integer, TaskAssignment>();
		for (int i = 0; i < this.jobConf.getNumMapTasks(); i++) {
			MapTaskRequest mapRequest = new MapTaskRequest(i,
					iterationCount.get());
			mapRequest.addKeyValue(new StringKey(jobConf.getJobId() + i),
					bcastValue);
			mapRequest.setJobId(this.jobConf.getJobId());
			mapRequest.setResponseTopic(this.responseTopic);
			TaskAssignment mapAssignment = new TaskAssignment(mapRequest,
					this.execPlan.getMapTasksMap().get(i));
			mapTasksMap.put(i, mapAssignment);
		}
		SendRecvResponse response = this.broadcaster.sendAllRequestsByTCP(
				mapTasksMap, TwisterConstants.MAP_TASK_REQUEST);
		return response;
	}

	boolean isMapRunning() {
		if (this.jobState == JobState.MAP_STARTED) {
			return true;
		}
		return false;
	}

	/**
	 * mark Map tasks are completed
	 */
	void setMapCompleted() {
		if (this.jobState == JobState.MAP_STARTED) {
			this.jobState = JobState.MAP_COMPLETED;
		}
	}

	/**
	 * start shuffle
	 * 
	 * @throws TwisterException
	 */
	void startShuffle() throws TwisterException {
		if (!this.execPlan.isReduceConfigured()) {
			throw new TwisterException(
					"No reduce class, cannot start shuffling.");
		}
		if (this.jobState.ordinal() < JobState.MAP_COMPLETED.ordinal()) {
			throw new TwisterException(
					"Map hasn't been completed yet, cannot go to MemCache cleaning");
		}
		if (this.jobState.ordinal() > JobState.MAP_COMPLETED.ordinal()) {
			throw new TwisterException("Shuffling has alreday been started");
		}
		logger.info("Start Shuffling.");
		this.jobState = JobState.SHUFFLE_STARTING;
		SendRecvResponse response = startShuffleInternal();
		this.jobState = JobState.SHUFFLE_STARTED;
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in starting shuffle.");
		}
		// Exception from remote code, clean and terminate remote job,
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"startShuffle produced exceptions at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException("Time out in startShuffle.");
		}
		logger.info("start shuffling is completed. ");
	}

	private SendRecvResponse startShuffleInternal() throws TwisterException {
		StartShuffleMessage msg = new StartShuffleMessage(jobConf.getJobId(),
				this.responseTopic);
		SendRecvResponse response = this.broadcaster
				.bcastRequestsAndReceiveResponses(msg);
		return response;
	}

	boolean isShuffleRunning() {
		if (this.jobState == JobState.SHUFFLE_STARTED) {
			return true;
		}
		return false;
	}

	void setShuffleCompleted() {
		this.jobState = JobState.SHUFFLE_COMPLETED;
	}

	/**
	 * start reducer
	 * 
	 * @param reduceInputMap
	 * @throws TwisterException
	 */
	void startReduce(Map<Integer, Integer> reduceInputMap)
			throws TwisterException {
		if (!this.execPlan.isReduceConfigured()) {
			throw new TwisterException(
					"Reduce are not configured, cannot start reducing.");
		}
		if (this.jobState.ordinal() < JobState.SHUFFLE_COMPLETED.ordinal()) {
			throw new TwisterException(
					"Shuffle hasn't been completed yet, cannot go to Reduce");
		}
		if (this.jobState.ordinal() > JobState.SHUFFLE_COMPLETED.ordinal()) {
			throw new TwisterException("Shuffling has alreday been started");
		}
		logger.info("Start Reduce.");
		this.jobState = JobState.REDUCE_STARTING;
		SendRecvResponse response = startReduceInternal(reduceInputMap);
		this.jobState = JobState.REDUCE_STARTED;
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in starting shuffle.");
		}
		// Exception from remote code, clean and terminate remote job,
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"startReduce produced exceptions at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException("Time out in startReduce.");
		}
		logger.info("start shuffling is completed.");
	}

	private SendRecvResponse startReduceInternal(
			Map<Integer, Integer> reduceInputMap) throws TwisterException {
		StartReduceMessage msg = new StartReduceMessage(reduceInputMap,
				jobConf.getJobId(), this.responseTopic);
		SendRecvResponse response = this.broadcaster
				.bcastRequestsAndReceiveResponses(msg);
		return response;
	}

	boolean isReducRunning() {
		if (this.jobState == JobState.REDUCE_STARTED) {
			return true;
		}
		return false;
	}

	void setReduceCompleted() {
		this.jobState = JobState.REDUCE_COMPLETED;
	}

	/**
	 * Gather based on daemon rank map (daemon ID, rank ID), and gather method
	 * (direct/MST)
	 * 
	 * @param daemonRankMap
	 * @param gatherMethod
	 * @throws TwisterException
	 */
	void startGather(Map<Integer, Integer> daemonRankMap, byte gatherMethod)
			throws TwisterException {
		if (!this.execPlan.isCombinerConfigured()) {
			throw new TwisterException(
					"Combiner is not configured, cannot start combining.");
		}
		if (this.jobState.ordinal() < JobState.REDUCE_COMPLETED.ordinal()) {
			throw new TwisterException(
					"Reduce hasn't been completed yet, cannot go to gathering");
		}
		if (this.jobState.ordinal() > JobState.REDUCE_COMPLETED.ordinal()) {
			throw new TwisterException("Shuffling has alreday been started");
		}
		logger.info("Start Combine.");
		this.jobState = JobState.COMBINE_STARTING;
		SendRecvResponse response = startGartherInternal(daemonRankMap,
				gatherMethod);
		this.jobState = JobState.COMBINE_STARTED;
		// Exception,from local code, irrecoverable
		if (response.getStatus().equals(SendRecvStatus.LOCAL_EXCEPTION)) {
			throw new TwisterException(
					"Local driver exceptions in starting combining.");
		}
		// Exception from remote code, clean and terminate remote job,
		if (response.getStatus().equals(SendRecvStatus.REMOTE_EXCEPTION)) {
			throw new TwisterException(
					"startGather produced exceptions at the daemons. Please see the logs for further information.");
		}
		// fault
		if (response.getStatus().equals(SendRecvStatus.REMOTE_FALIURE)) {
			this.reSubmitByPlan();
			return;
		}
		// timeout
		if (response.getStatus().equals(SendRecvStatus.TIMEOUT)) {
			throw new TwisterException("Time out in startGather.");
		}
		logger.info("start combining is completed. ");
	}

	/**
	 * we can add option here in future, to be able to user MSTGather, or Direct
	 * downloading
	 * 
	 * gather method uses TwisterConstants.GATHER_IN_DIRECT
	 * 
	 * @param reducerDaemonMap
	 * @throws TwisterException
	 */
	private SendRecvResponse startGartherInternal(
			Map<Integer, Integer> daemonRankMap, byte gatherMethod)
			throws TwisterException {
		StartGatherMessage msg = new StartGatherMessage(jobConf.getJobId(),
				daemonRankMap, this.responseTopic, gatherMethod);
		SendRecvResponse response = this.broadcaster
				.bcastRequestsAndReceiveResponses(msg);
		// we have to check if the operation get successful
		// since we need driver operation, but this is too difficult to control
		if (response.getStatus().equals(SendRecvStatus.SUCCESS)) {
			if (gatherMethod == TwisterConstants.GATHER_IN_MST
					&& this.gatherer != null) {
				this.gatherer.gatherCombineInputInMST(msg);
			}
		}
		return response;
	}

	boolean isGatherRunning() {
		if (this.jobState.ordinal() == JobState.COMBINE_STARTED.ordinal()) {
			return true;
		}
		return false;
	}

	void setGatherCompleted() {
		this.jobState = JobState.COMBINE_COMPLETE;
	}

	boolean isRunMapReduceCompleted() {
		if (this.jobConf.isHasMapClass() && !this.jobConf.isHasReduceClass()
				&& !this.jobConf.isHasCombinerClass()
				&& this.jobState.equals(JobState.MAP_COMPLETED)) {
			return true;
		}
		if (this.jobConf.isHasMapClass() && this.jobConf.isHasReduceClass()
				&& !this.jobConf.isHasCombinerClass()
				&& this.jobState.equals(JobState.REDUCE_COMPLETED)) {
			return true;
		}
		if (this.jobConf.isHasMapClass() && this.jobConf.isHasReduceClass()
				&& this.jobConf.isHasCombinerClass()
				&& this.jobState.equals(JobState.COMBINE_COMPLETE)) {
			return true;
		}
		return false;
	}

	/**
	 * complete the current iteration and add 1 to the current iteration count
	 */
	void setIterationCompleted() {
		this.jobState = JobState.ITERATION_OVER;
		this.monitor.close();
		this.iterationCount.addAndGet(1);
	}

	boolean isIterationCompleted() {
		if (this.jobState.ordinal() == JobState.ITERATION_OVER.ordinal()) {
			return true;
		}
		return false;
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see cgl.imr.client.TwisterModel#close()
	 */
	public void close() {
		// if driver before sending a new job, JobState is not initialized
		if (this.jobState.ordinal() == JobState.TERMINATE_COMPLETES.ordinal()) {
			System.out.println("Driver has already been closed.. ");
			return;
		}
		cleanup();
		try {
			this.pubSubService.close();
		} catch (PubSubException e) {
			e.printStackTrace();
		}
		this.jobState = JobState.TERMINATE_COMPLETES;
		shutDownHook.setIsClosed();
		logger.info("MapReduce computation termintated gracefully.");
	}

	/**
	 * Actually we don't care the response from this operation EndJobRequest
	 * include all the cleaning work (cleanMemCache is included)
	 * 
	 * @return
	 */
	private SendRecvResponse cleanup() {
		this.faultDetector.close();
		this.monitor.close(); // won't harm if the monitor is not started
		EndJobRequest endMapReduceRequest = new EndJobRequest();
		endMapReduceRequest.setJobId(jobConf.getJobId());
		endMapReduceRequest.setResponseTopic(responseTopic);
		SendRecvResponse response = this.broadcaster
				.bcastEndJobRequestsAndReceiveResponses(endMapReduceRequest);
		return response;
	}

	/**
	 * find the history redo as the history, if initialized send new job request
	 * 
	 * @throws TwisterException
	 * 
	 */
	private void reSubmitByPlan() throws TwisterException {
		int numRetries = 0;
		SendRecvResponse response = null;
		do {
			numRetries++;
			logger.info("Job " + this.jobConf.getJobId()
					+ ": Start handling the failure. Retry " + numRetries + ".");
			logger.info("Cleanup starts, please wait for a few minutes.");
			cleanup();
			// clean up and wait, we hope the threads started can finish
			try {
				Thread.sleep(TwisterConstants.CLEANUP_AND_WAIT_TIME);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Cleanup ends.");
			//auto go to next iteration
			this.iterationCount.addAndGet(1);
			this.faultDetector.start();
			if (this.jobState.ordinal() == JobState.INITIATED.ordinal()) {
				System.out.println("Current Job State: Initialized.");
				response = sendNewJobRequestInternal();
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				break;
			} else if (this.jobState.ordinal() == JobState.MAP_CONFIGURED
					.ordinal()) {
				System.out.println("Current Job State: Map Configured.");
				response = sendNewJobRequestInternal();
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				if (this.execPlan.getPartitionFile() != null) {
					response = this.configureMapsInternal(this.execPlan
							.getPartitionFile());
				} else if (this.execPlan.getMapConfigurations() != null) {
					response = this.configureMapsInternal(this.execPlan
							.getMapConfigurations());
				} else {
					response = this.configureMapsInternal();
				}
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				// add MemCache
				if (this.execPlan.isMemCacheAdded()) {
					response = this.addToMemCacheInternal(
							this.execPlan.getMemCacheAddress(),
							this.execPlan.getMemCachedData());
				}
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				break;
			} else if (this.jobState.ordinal() == JobState.REDUCE_CONFIGURED
					.ordinal()) {
				response = sendNewJobRequestInternal();
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				if (this.execPlan.getPartitionFile() != null) {
					response = this.configureMapsInternal(this.execPlan
							.getPartitionFile());
				} else if (this.execPlan.getMapConfigurations() != null) {
					response = this.configureMapsInternal(this.execPlan
							.getMapConfigurations());
				} else {
					response = this.configureMapsInternal();
				}
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				// add MemCache
				if (this.execPlan.isMemCacheAdded()) {
					response = this.addToMemCacheInternal(
							this.execPlan.getMemCacheAddress(),
							this.execPlan.getMemCachedData());
				}
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				response = this.configureReduceInternal(this.execPlan
						.getReduceConfigurations());
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				break;
			} else if (this.jobState.ordinal() >= JobState.MAP_STARTED
					.ordinal()
					&& this.jobState.ordinal() < JobState.COMBINE_COMPLETE
							.ordinal()) {
				System.out.println("Current Job State: MapReduce running.");
				System.out.println("Resubmit tasks.");
				response = sendNewJobRequestInternal();
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				if (this.execPlan.getPartitionFile() != null) {
					response = this.configureMapsInternal(this.execPlan
							.getPartitionFile());
				} else if (this.execPlan.getMapConfigurations() != null) {
					response = this.configureMapsInternal(this.execPlan
							.getMapConfigurations());
				} else {
					response = this.configureMapsInternal();
				}
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					System.out.println("Failure in re-configuring Map tasks");
					continue;
				}
				// add MemCache
				if (this.execPlan.isMemCacheAdded()) {
					response = this.addToMemCacheInternal(
							this.execPlan.getMemCacheAddress(),
							this.execPlan.getMemCachedData());
				}
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				response = this.configureReduceInternal(this.execPlan
						.getReduceConfigurations());
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				// a new combiner, if fault happened during combining
				// this new combiner can desert all data collected.
				if (this.execPlan.isCombinerConfigured()) {
					this.configureCombinerInternal();
				}
				// start monitor
				this.monitor.start();
				// resubmit and set the JobState
				if (this.execPlan.getKeyValuePair() != null) {
					response = this.runMapReduceInternal(this.execPlan
							.getKeyValuePair());
				} else if (this.execPlan.getBcastValue() != null) {
					response = this.runMapReduceInternal(this.execPlan
							.getBcastValue());
				} else {
					response = this.runMapReduceInternal();
				}
				this.jobState = JobState.MAP_STARTED;
				break;
			} else if (this.jobState.ordinal() == JobState.ITERATION_OVER
					.ordinal()) {
				System.out.println("Current Job State: Between Iterations.");
				// for the operations between iteration, such as cleanMemCache
				// we reset the job but no need to re-execute the iteration
				response = sendNewJobRequestInternal();
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				if (this.execPlan.getPartitionFile() != null) {
					response = this.configureMapsInternal(this.execPlan
							.getPartitionFile());
				} else if (this.execPlan.getMapConfigurations() != null) {
					response = this.configureMapsInternal(this.execPlan
							.getMapConfigurations());
				} else {
					response = this.configureMapsInternal();
				}
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				// add MemCache
				if (this.execPlan.isMemCacheAdded()) {
					response = this.addToMemCacheInternal(
							this.execPlan.getMemCacheAddress(),
							this.execPlan.getMemCachedData());
				}
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				response = this.configureReduceInternal(this.execPlan
						.getReduceConfigurations());
				if (!response.getStatus().equals(SendRecvStatus.SUCCESS)) {
					continue;
				}
				if (this.execPlan.isCombinerConfigured()) {
					// no need to re-configure the whole combiner,
					// data in combiner may need accessing
					// and configure combiner is just to create a combiner
					// object
					// this.configureCombinerInternal();
					configureGathererInternal();
				}
				break;
			} else {
				throw new TwisterException(
						"Invalid job state, cannot recover..");
			}
		} while (numRetries <= TwisterConstants.NUM_RETRIES);
		if (numRetries > TwisterConstants.NUM_RETRIES) {
			throw new TwisterException("Recovery from failure failed.");
		}
	}

	/**
	 * All the incoming messages are received here first, and next this method
	 * handles the requests appropriately.
	 * 
	 * Notice that this is a queue, onEvent cannot run concurrently
	 * 
	 * CombineInput / TaskStatus / WorkResponse
	 */
	public void onEvent(TwisterMessage message) {
		if (message != null) {
			// no one uses the negative code
			byte msgType = -1;
			try {
				msgType = message.readByte();
			} catch (SerializationException e) {
				msgType = -1;
				e.printStackTrace();
			}
			if (msgType == TwisterConstants.COMBINE_INPUT
					&& this.gatherer != null) {
				this.gatherer.handleCombineInput(message);
			} else if (msgType == TwisterConstants.TASK_STATUS) {
				this.monitor.handleTaskStatus(message);
			} else if (msgType == TwisterConstants.DAEMON_STATUS) {
				// DaemonStatus may be sent just before the fault detector get
				// initialized
				this.faultDetector.handleDaemonStatus(message);
			} else if (msgType == TwisterConstants.WORKER_RESPONSE) {
				// These are responses. So should go to response queue.
				try {
					WorkerResponse response = new WorkerResponse(message);
					responseMap.put(response.getRefMessageId(), response);
				} catch (Exception e) {
					System.out.println("Error in receiving worker responses.");
					logger.error("Error in receiving worker responses.", e);
				}
			} else {
				System.out.println("Unknown response got in the driver");
			}
		}
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see cgl.imr.client.TwisterModel#getCurrentCombiner()
	 */
	public Combiner getCurrentCombiner() throws TwisterException {
		if (this.currentCombiner == null) {
			throw new TwisterException(
					"Combiner is not engaged. Please check if the combiner is specified in the JobConf.");
		}
		return this.currentCombiner;
	}

	void checkAndHandleFailureInMonitoring() throws TwisterException {
		if (this.faultDetector.isHasFault()) {
			this.reSubmitByPlan();
		}
	}

	boolean isCurrentIteration(int iteration) {
		if (this.iterationCount.get() == iteration) {
			return true;
		}
		return false;
	}
}