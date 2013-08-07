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

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.SerializationException;
import cgl.imr.base.Subscribable;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterConstants.EntityType;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.PubSubFactory;
import cgl.imr.client.DaemonInfo;
import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.message.DirListRequest;
import cgl.imr.message.DirListResponse;
import cgl.imr.message.EndJobRequest;
import cgl.imr.message.MapTaskGroupRequest;
import cgl.imr.message.MapTaskRequest;
import cgl.imr.message.MapperRequest;
import cgl.imr.message.MemCacheClean;
import cgl.imr.message.MemCacheInput;
import cgl.imr.message.NewJobRequest;
import cgl.imr.message.ReduceInput;
import cgl.imr.message.ReducerRequest;
import cgl.imr.message.StartGatherMessage;
import cgl.imr.message.StartReduceMessage;
import cgl.imr.message.StartShuffleMessage;
import cgl.imr.message.WorkerResponse;
import cgl.imr.types.IntKey;
import cgl.imr.util.CustomClassLoader;
import cgl.imr.util.DataHolder;
import cgl.imr.util.JarClassLoaderException;

/**
 * Main entity that handles most of the server side functionality. DaemonWorker
 * accept messages coming from the pub-sub broker network and perform them
 * appropriately. To run map/reduce computations it uses the
 * <code>java.util.concurrent.Executor</code> functionality.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * @author zhangbj
 * 
 */
public class DaemonWorker implements Subscribable {
	// get class loader, note this is static
	private static Map<String, CustomClassLoader> classLoaders = new ConcurrentHashMap<String, CustomClassLoader>();

	public static CustomClassLoader getClassLoader(String jobId) {
		return classLoaders.get(jobId);
	}

	private static Logger logger = Logger.getLogger(DaemonWorker.class);
	// configuration informations
	private int daemonNo;
	private String hostIP;
	private int daemonPort;
	private Map<Integer, DaemonInfo> daemonInfo;
	private TwisterConfigurations config;
	// pub-sub service
	private String daemonCommTopic;
	private PubSubService pubSubService;
	// cache system, object cache and byte cache
	private MemCache memCache;
	private ConcurrentHashMap<String, DataHolder> dataCache;
	// job ids and mapper requests
	private Map<String, Map<Integer, Mapper>> mappers;
	// collector for mappers
	private Map<String, MapOutputCollectorImpl> mapCollectors;
	// job ids and reducer requests
	private ConcurrentMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<Reducer>>> reducers;
	// bcast reducers
	private ConcurrentMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<Reducer>>> bcastReducers;
	// collector for reducers
	private ConcurrentMap<String, ReduceOutputCollectorImpl> reduceCollectors;
	// executors
	private ExecutorService longTaskExecutor;
	private Executor shortTaskExecutor;
	private Executor clientTaskExecutor;
	private Timer timerTaskExecutor;
	// status notifier
	private StatusNotifier notifer;

	public DaemonWorker(int daemonNo, int numMapWorkers,
			ConcurrentHashMap<String, DataHolder> dataCache, int daemonPort,
			String hostIP, Executor clientTaskExecutor,
			Map<Integer, DaemonInfo> daemonInfo) throws TwisterException {
		this.daemonNo = daemonNo;
		this.hostIP = hostIP;
		this.daemonPort = daemonPort;
		this.daemonInfo = daemonInfo;
		// load configuration
		try {
			this.config = TwisterConfigurations.getInstance();
		} catch (ConfigurationException e) {
			throw new TwisterException(e);
		}
		// Initialize caches
		this.memCache = MemCache.getInstance();
		this.dataCache = dataCache;
		// pub-sbu service start
		this.daemonCommTopic = TwisterConstants.MAP_REDUCE_TOPIC_BASE + "/"
				+ daemonNo;
		// System.out.println("Starting pubsub service");
		try {
			this.pubSubService = PubSubFactory.getPubSubService(config,
					EntityType.DAEMON, daemonNo);
			this.pubSubService.setSubscriber(this);
			this.pubSubService.subscribe(daemonCommTopic);
			this.pubSubService
					.subscribe(TwisterConstants.CLEINT_TO_WORKER_BCAST);
			this.pubSubService.start();
		} catch (PubSubException e) {
			logger.error("Failure in starting Broker Connection. Terminating the daemon.");
			throw new TwisterException(e);
		}
		// System.out.println("pubsub service started");
		// Initialize maps
		this.mappers = new HashMap<String, Map<Integer, Mapper>>();
		// add map collectors here
		this.mapCollectors = new HashMap<String, MapOutputCollectorImpl>();
		this.reducers = new ConcurrentHashMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<Reducer>>>();
		this.bcastReducers = new ConcurrentHashMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<Reducer>>>();
		// add reduce collectors here
		this.reduceCollectors = new ConcurrentHashMap<String, ReduceOutputCollectorImpl>();
		// for mapper and reducer request and map and reduce tasks
		//this.longTaskExecutor = Executors.newFixedThreadPool((int) Math
				//.ceil(numMapWorkers / (double) 2));
		this.longTaskExecutor = Executors.newFixedThreadPool(numMapWorkers);
		//For data sending and downloading, 
		this.clientTaskExecutor = clientTaskExecutor;
		// this is task spawned for doing internal short tasks
		this.shortTaskExecutor = Executors.newCachedThreadPool();
		// status notification, monitoring thread, use timer task
		notifer = new StatusNotifier(pubSubService, daemonNo, hostIP);
		timerTaskExecutor = new Timer();
		timerTaskExecutor.scheduleAtFixedRate(notifer,
				TwisterConstants.DAEMON_STATUS_INTERVAL,
				TwisterConstants.DAEMON_STATUS_INTERVAL);
		logger.info("Daemon no: " + daemonNo + " started with " + numMapWorkers
				+ " workers.");
	}

	/**
	 * Helper method to create the partition file using a distribution of input
	 * files available across the cluster. List all the file names that matches
	 * the given filter pattern and sends a response.
	 * 
	 * @param msg
	 *            - A DirListRequest.
	 * @throws TwisterException
	 * @throws PubSubException
	 * @throws SerializationException
	 */
	private void handleDirList(TwisterMessage msg) {
		DirListRequest listRequest = new DirListRequest();
		try {
			listRequest.fromTwisterMessage(msg);
		} catch (SerializationException e) {
			listRequest = null;
			e.printStackTrace();
		}
		if (listRequest == null) {
			return;
		}
		File[] files = null;
		List<String> selectedFiles = new ArrayList<String>();
		File dir = new File(listRequest.getDirectry());
		// create a copy of the filter string and make it final
		final String filterString = listRequest.getFileFilter() + "";
		if (!dir.exists()) {
			logger.warn("Requested directory: " + dir.getName()
					+ " does not exist.");
			WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
			response.setRefMessageId(listRequest.getRefMessageId());
			response.setExceptionString(listRequest.getDirectry()
					+ " directory does not exist");
			try {
				this.pubSubService.send(listRequest.getResponseTopic(),
						response);
			} catch (PubSubException e) {
				e.printStackTrace();
			}
		} else {
			// filter the files and get the absolute path
			FileFilter fileFilter = new FileFilter() {
				public boolean accept(File file) {
					/*
					 * return !(file.isDirectory());
					 */
					if (file.isDirectory()) {
						return false;
					}
					if (file.getName().startsWith(filterString)) {
						return true;
					}
					// we assume there is a pattern
					boolean match = false;
					try {
						match = file.getName().matches(filterString);
					} catch (PatternSyntaxException e) {
						match = false;
					}
					if (match) {
						return true;
					}
					return false;
				}
			};
			files = dir.listFiles(fileFilter);
			if (files != null) {
				for (File file : files) {
					selectedFiles.add(file.getAbsolutePath());
				}
			}
			DirListResponse response = new DirListResponse(selectedFiles,
					daemonNo, hostIP);
			try {
				this.pubSubService.send(listRequest.getResponseTopic(),
						response);
			} catch (PubSubException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Initializing the daemon for a new MapReduce computation. This will create
	 * a new class loader for this job and store it in a hash table for later
	 * use.
	 * 
	 * @param message
	 *            - Set of bytes for NewJobRequest message.
	 * @throws SerializationException
	 * @throws PubSubException
	 */
	private synchronized void handleNewJobRequest(TwisterMessage message) {
		NewJobRequest newJobRequest = null;
		try {
			newJobRequest = new NewJobRequest(message);
		} catch (SerializationException e) {
			e.printStackTrace();
		}
		if (newJobRequest == null) {
			return;
		}
		WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
		// Client expects the daemonNo to be added to the refId.
		response.setRefMessageId(newJobRequest.getRefMessageId() + daemonNo);
		CustomClassLoader classLoader = null;
		// search existing classloader
		for (String friendJobID : newJobRequest.getFriendJobList()) {
			classLoader = classLoaders.get(friendJobID);
			if (classLoader != null) {
				if (daemonNo == 0) {
					logger.info("Class Loader is found.");
				}
				break;
			}
		}
		// create a new one
		if (classLoader == null) {
			try {
				classLoader = new CustomClassLoader();
			} catch (JarClassLoaderException e) {
				classLoader = null;
				response.setExceptionString("Could not initiate the class loader.");
				logger.error(e);
			}
		}
		// store the new classloader
		if (classLoader != null) {
			classLoader.increaseRef();
			classLoaders.put(newJobRequest.getJobId(), classLoader);
		}
		// reply to the driver
		try {
			this.pubSubService.send(newJobRequest.getResponseTopic(), response);
		} catch (PubSubException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates a Mapper to this particular map task. The mappers are stored
	 * (cached) till the termination of that particular MapReduce computation.
	 * Sends a response to the client.
	 * 
	 * ZBJ: create a thread to handle mapper request since it could take long
	 * time to load the file in mapper configuration we have to wait.
	 * 
	 * @param request
	 */
	void handleMapperRequest(TwisterMessage request) {
		HandleMapperRequestThread handler = new HandleMapperRequestThread(
				request);
		longTaskExecutor.execute(handler);
	}

	/**
	 * Thread to handle map request
	 * 
	 * @author zhangbj
	 */
	private class HandleMapperRequestThread implements Runnable {
		private TwisterMessage request;
	
		HandleMapperRequestThread(TwisterMessage req) {
			request = req;
		}
	
		@Override
		public void run() {
			MapperRequest mapperRequest = null;
			try {
				mapperRequest = new MapperRequest(this.request);
			} catch (SerializationException e) {
				mapperRequest = null;
				e.printStackTrace();
			}
			if (mapperRequest == null) {
				System.out.println("Errors in reading MapperRequest: "
						+ daemonNo + " " + hostIP);
				return;
			}
			// Create the response object.
			WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
			response.setRefMessageId(mapperRequest.getRefMessageId());
			// start processing
			JobConf jobConf = mapperRequest.getJobConf();
			CustomClassLoader classLoader = classLoaders
					.get(jobConf.getJobId());
			if (classLoader != null) {
				// create a collector for mappers, one job per node only has
				// one mapper collector
				MapOutputCollectorImpl mapCollector = null;
				synchronized (mapCollectors) {
					mapCollector = mapCollectors.get(jobConf.getJobId());
					if (mapCollector == null) {
						try {
							mapCollector = new MapOutputCollectorImpl(jobConf,
									pubSubService, mapperRequest, dataCache,
									daemonNo, hostIP, daemonPort);
						} catch (TwisterException e) {
							mapCollector = null;
							response.setExceptionString("Failure in creating mapper collector.");
							e.printStackTrace();
						}
						if (mapCollector != null) {
							mapCollectors.put(jobConf.getJobId(), mapCollector);
						}
					}
				}
				// create mapper
				if (mapCollectors != null) {
					Mapper mapperExec = null;
					try {
						mapperExec = new Mapper(pubSubService, mapperRequest,
								mapCollector, classLoader, daemonNo);
					} catch (TwisterException e) {
						mapperExec = null;
						response.setExceptionString("Failure in creating mapper.");
						e.printStackTrace();
					}
					if (mapperExec != null) {
						synchronized (mappers) {
							Map<Integer, Mapper> mapperMap = mappers
									.get(jobConf.getJobId());
							if (mapperMap == null) {
								mapperMap = new HashMap<Integer, Mapper>();
								mappers.put(jobConf.getJobId(), mapperMap);
							}
							mapperMap.put(mapperRequest.getMapTaskNo(),
									mapperExec);
						}
					}
				}
			} else {
				response.setExceptionString("Invalid job Id. No class loader configured.");
			}
			if (response.getExceptionString() != null) {
				System.out.println("MapperRequest Exception " + daemonNo + " "
						+ hostIP);
			}
			try {
				pubSubService.send(mapperRequest.getResponseTopic(), response);
			} catch (PubSubException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Starts a Reducer to handle the reduce computation. Reducer is also cached
	 * and terminated at the end of the MapReduce computation. Sends a response
	 * to the client.
	 * 
	 * @param request
	 *            - A ReducerRequest.
	 * @throws TwisterException
	 * @throws PubSubException
	 * @throws SerializationException
	 */
	private void handleReducerRequest(TwisterMessage request) {
		ReducerRequest reduceRequest = null;
		try {
			reduceRequest = new ReducerRequest(request);
		} catch (SerializationException e) {
			reduceRequest = null;
			e.printStackTrace();
		}
		if (reduceRequest == null) {
			return;
		}
		/*
		 * System.out.println("handleReducerRequest: ReduceNo " +
		 * reduceRequest.getReduceConf().getReduceTaskNo() + " on Daemon: " +
		 * daemonNo + " on IP: " + this.hostIP);
		 */
		// create a response
		WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
		response.setRefMessageId(reduceRequest.getRefMessageId());
		// start processing
		JobConf jobConf = reduceRequest.getJobConf();
		CustomClassLoader classLoader = classLoaders.get(reduceRequest
				.getJobConf().getJobId());
		// create reducer collector
		if (classLoader != null) {
			ReduceOutputCollectorImpl reduceCollector = null;
			synchronized (reduceCollectors) {
				reduceCollector = reduceCollectors.get(jobConf.getJobId());
				if (reduceCollector == null) {
					reduceCollector = new ReduceOutputCollectorImpl(
							pubSubService, reduceRequest, dataCache, daemonNo,
							hostIP, daemonPort, daemonInfo);
					reduceCollectors.put(jobConf.getJobId(), reduceCollector);
				}
			}
			//create reducer
			Reducer reduceExecutor = null;
			try {
				reduceExecutor = new Reducer(this.pubSubService, reduceRequest,
						reduceCollector, classLoader, dataCache, hostIP,
						daemonNo);
			} catch (TwisterException e) {
				reduceExecutor = null;
				response.setExceptionString("Failure in creating reducer.");
				e.printStackTrace();
			}
			/*
			 * every job has an reduceExecMap. Each map entry is a topic and a
			 * reducer, the topic is sent by the client, called reduce topic.
			 * topicForReduceTask = reduceTopicBase + reduceTaskNo;
			 */
			if (reduceExecutor != null) {
				ConcurrentMap<String, ConcurrentLinkedQueue<Reducer>> reduceExecMap = this.reducers
						.get(jobConf.getJobId());
				ConcurrentLinkedQueue<Reducer> reduceExecs = null;
				if (reduceExecMap == null) {
					reduceExecMap = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Reducer>>();
					reduceExecs = new ConcurrentLinkedQueue<Reducer>();
					reduceExecs.add(reduceExecutor);
					reduceExecMap.put(reduceRequest.getReduceTopic(),
							reduceExecs);
					this.reducers.put(jobConf.getJobId(), reduceExecMap);
				} else {
					if (reduceExecMap.containsKey(reduceRequest
							.getReduceTopic())) {
						reduceExecs = reduceExecMap.get(reduceRequest
								.getReduceTopic());
						reduceExecs.add(reduceExecutor);
					} else {
						reduceExecs = new ConcurrentLinkedQueue<Reducer>();
						reduceExecs.add(reduceExecutor);
						reduceExecMap.put(reduceRequest.getReduceTopic(),
								reduceExecs);
					}
				}
				try {
					this.pubSubService
							.subscribe(reduceRequest.getReduceTopic());
				} catch (PubSubException e) {
					response.setExceptionString("Failure in subscribing reduce topic.");
					e.printStackTrace();
				}
				// For RowBcast supported, I guess this is a routine special to
				// Fox
				// Algorithm
				if (reduceRequest.getJobConf().isRowBCastSupported()) {
					String bcastTopic = jobConf.getRowBCastTopic()
							+ reduceRequest.getReduceConf().getReduceTaskNo()
							/ jobConf.getSqrtReducers();
					// Add topic and reducer in reduceExecMap of bcastReducers
					reduceExecMap = this.bcastReducers.get(jobConf.getJobId());
					if (reduceExecMap == null) {
						reduceExecMap = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Reducer>>();
						reduceExecs = new ConcurrentLinkedQueue<Reducer>();
						reduceExecs.add(reduceExecutor);
						reduceExecMap.put(bcastTopic, reduceExecs);
						this.bcastReducers.put(jobConf.getJobId(),
								reduceExecMap);
					} else {
						if (reduceExecMap.containsKey(bcastTopic)) {
							reduceExecs = reduceExecMap.get(bcastTopic);
							reduceExecs.add(reduceExecutor);
						} else {
							reduceExecs = new ConcurrentLinkedQueue<Reducer>();
							reduceExecs.add(reduceExecutor);
							reduceExecMap.put(bcastTopic, reduceExecs);
						}
					}
					try {
						this.pubSubService.subscribe(bcastTopic);
					} catch (PubSubException e) {
						response.setExceptionString("Failure in subscribing reduceBcast topic.");
						e.printStackTrace();
					}
				}
			}
		} else {
			response.setExceptionString("Invalid job id. No class loader is set.");
		}
		// move this statement from bottom to here, this is successful
		// response
		try {
			this.pubSubService.send(reduceRequest.getResponseTopic(), response);
		} catch (PubSubException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Schedules the execution of a map task by finding appropriate mapper
	 * object.
	 * 
	 * @param request
	 *            - A MapTaskRequest.
	 * @throws TwisterException
	 * @throws SerializationException
	 * @throws PubSubException
	 */
	void handleMapTask(TwisterMessage request) {
		MapTaskRequest mapRequest = null;
		try {
			mapRequest = new MapTaskRequest(request);
		} catch (SerializationException e) {
			mapRequest = null;
			e.printStackTrace();
		}
		if (mapRequest == null) {
			return;
		}
		WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
		response.setRefMessageId(mapRequest.getRefMessageId());
		Mapper exec = null;
		synchronized (mappers) {
			Map<Integer, Mapper> mapperMap = mappers.get(mapRequest.getJobId());
			exec = mapperMap.get(mapRequest.getMapTaskNo());
		}
		if (exec != null) {
			exec.setCurrentRequest(mapRequest);
		} else {
			response.setExceptionString("No executors are found.");
			logger.error("No mapper is registered for this map task "
					+ mapRequest.getMapTaskNo() + ". @ the daemon no: "
					+ daemonNo);
		}
		// send response, then start the task
		boolean isSendingResponseSuccessful = true;
		try {
			this.pubSubService.send(mapRequest.getResponseTopic(), response);
		} catch (PubSubException e) {
			isSendingResponseSuccessful = false;
			e.printStackTrace();
		}
		if (exec != null && isSendingResponseSuccessful) {
			longTaskExecutor.execute(exec);
		}
	}

	/**
	 * Schedules the execution of a map task by finding appropriate mapper
	 * object.
	 * 
	 * @param request
	 *            - A MapTaskRequest.
	 * @throws TwisterException
	 * @throws SerializationException
	 * @throws PubSubException 
	 */
	/*
	void handleMapTaskFromTCP(TwisterMessage request) {
		MapTaskRequest mapRequest = null;
		try {
			mapRequest = new MapTaskRequest(request);
		} catch (SerializationException e) {
			mapRequest = null;
			e.printStackTrace();
		}
		if (mapRequest == null) {
			return;
		}
		synchronized (mappers) {
			Map<Integer, Mapper> mapperMap = mappers.get(mapRequest.getJobId());
			Mapper exec = mapperMap.get(mapRequest.getMapTaskNo());
			if (exec != null) {
				exec.setCurrentRequest(mapRequest);
				longTaskExecutor.execute(exec);
			} else {
				logger.error("No mapper is registered for this map task "
						+ mapRequest.getMapTaskNo() + ". @ the daemon no: "
						+ daemonNo);
			}
		}
	}
	*/
	
	void handleMapTaskFromTCP(TwisterMessage request) {
		MapTaskGroupRequest mapRequests = null;
		try {
			mapRequests = new MapTaskGroupRequest(request);
		} catch (SerializationException e) {
			mapRequests = null;
			e.printStackTrace();
		}
		if (mapRequests == null) {
			return;
		}
		List<Mapper> execs = new ArrayList<Mapper>();
		synchronized (mappers) {
			for (MapTaskRequest mapRequest : mapRequests.getMapRequests()) {
				Map<Integer, Mapper> mapperMap = mappers.get(mapRequest
						.getJobId());
				Mapper exec = mapperMap.get(mapRequest.getMapTaskNo());
				if (exec != null) {
					exec.setCurrentRequest(mapRequest);
					execs.add(exec);
				} else {
					logger.error("No mapper is registered for this map task "
							+ mapRequest.getMapTaskNo() + ". @ the daemon no: "
							+ daemonNo);
				}
			}
		}
		for (Mapper exec : execs) {
			longTaskExecutor.execute(exec);
		}
	}

	/**
	 * send the reduce data to the proper reducer. Since there is only one such
	 * task for each job and it only sends TwisterMessage, we put it in
	 * shortTaskExecutor
	 * 
	 * 
	 * @param message
	 */
	private void handleStartShuffle(TwisterMessage message) {
		HandleStartShuffleThread handler = new HandleStartShuffleThread(message);
		shortTaskExecutor.execute(handler);
	}

	private class HandleStartShuffleThread implements Runnable {
		private TwisterMessage message;
	
		HandleStartShuffleThread(TwisterMessage msg) {
			message = msg;
		}
	
		@Override
		public void run() {
			StartShuffleMessage msg = null;
			MapOutputCollectorImpl mapCollector = null;
			WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
			try {
				msg = new StartShuffleMessage(message);
			} catch (SerializationException e) {
				msg = null;
				e.printStackTrace();
				return;
			}
			if (msg != null) {
				// This is the response for a bcast operation.
				// Client expects the daemonNo to be added to the refId.
				response.setRefMessageId(msg.getRefMessageId() + daemonNo);
				synchronized (mapCollectors) {
					mapCollector = mapCollectors.get(msg.getJobId());
				}
			}
			// add response
			try {
				pubSubService.send(msg.getResponseTopic(), response);
			} catch (PubSubException e) {
				System.out.println("StartShuffle Exception " + daemonNo + " "
						+ hostIP);
				e.printStackTrace();
			}
			// start shuffle
			if (mapCollector != null) {
				mapCollector.shuffle();
			} 
		}
	}

	private void handleReduceInput(TwisterMessage msg) {
		HandleReduceInputThread handler = new HandleReduceInputThread(msg);
		clientTaskExecutor.execute(handler);
	}

	/**
	 * Collect map outputs to the reduce task and schedules the execution if all
	 * the expected map outputs are received.
	 * 
	 * @param msg
	 * @throws TwisterException
	 * @throws SerializationException
	 */
	private class HandleReduceInputThread implements Runnable {
		private TwisterMessage message;

		HandleReduceInputThread(TwisterMessage msg) {
			message = msg;
		}

		@Override
		public void run() {
			ReduceInput reduceInput = null;
			try {
				reduceInput = new ReduceInput(message);
			} catch (SerializationException e) {
				reduceInput = null;
				e.printStackTrace();
			}
			if (reduceInput == null) {
				return;
			}
			boolean reduceRequestHandled = false;
			ConcurrentLinkedQueue<Reducer> reduceExecutors = null;
			/*
			 * ZBJ: test the size of reducer executor, It seems that there is
			 * only one reducer. In handling reducer request, for each reducer,
			 * there is an reduceNo and a reducer topic related to it. Sink
			 * should be the destination topic here. I guess for each topic,
			 * there is only one reducer. Still needs careful check.
			 */
			Map<String, ConcurrentLinkedQueue<Reducer>> reduceMap = reducers
					.get(reduceInput.getJobId());
			if (reduceMap != null) {
				reduceExecutors = reduceMap.get(reduceInput.getReduceTopic());
				if (reduceExecutors != null) {
					for (Reducer reducer : reduceExecutors) {
						// System.out.println("Reducer " +
						// reducer.getReducerNo() +
						// " is handling reduce input.");
						try {
							reducer.handleReduceInputMessage(reduceInput);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					reduceRequestHandled = true;
				}
			}
			// Bcast reducers.
			reduceMap = bcastReducers.get(reduceInput.getJobId());
			if (reduceMap != null) {
				reduceExecutors = reduceMap.get(reduceInput.getReduceTopic());
				if (reduceExecutors != null) {
					for (Reducer reducer : reduceExecutors) {
						try {
							reducer.handleReduceInputMessageForBcast(
									reduceInput,
									new IntKey(reducer.getReducerNo()));
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					reduceRequestHandled = true;
				}
			}
			if (!reduceRequestHandled) {
				logger.error("Reduce input is not handled. There is no reduce task expecting this input. @ the daemon no: "
						+ daemonNo);
			}
		}
	}

	/**
	 * put StartReduce waiting to another thread, in this way reduceInput
	 * message after this won't be blocked.
	 * 
	 * @param message
	 * @throws SerializationException
	 */
	private void handleStartReduce(TwisterMessage message) {
		HandleStartReduceThread handler = new HandleStartReduceThread(message);
		shortTaskExecutor.execute(handler);
	}

	/**
	 * check if all reduce have been received, then start reducers
	 * 
	 * @author zhangbj
	 * 
	 */
	private class HandleStartReduceThread implements Runnable {
		private TwisterMessage message;

		HandleStartReduceThread(TwisterMessage msg) {
			message = msg;
		}

		@Override
		public void run() {
			StartReduceMessage msg = null;
			try {
				msg = new StartReduceMessage(message);
			} catch (SerializationException e) {
				msg = null;
				e.printStackTrace();
			}
			if (msg == null) {
				return;
			} 
			// This is the response for a bcast operation.
			// Client expects the daemonNo to be added to the refId.
			WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
			response.setRefMessageId(msg.getRefMessageId() + daemonNo);
			// try to get the reducer
			ConcurrentMap<String, ConcurrentLinkedQueue<Reducer>> reduceExecMap = reducers
					.get(msg.getJobId());
			// this daemon may just has no reducer
			/*
			 * if (reduceExecMap == null) {
			 * response.setExceptionString("Can not find the reducer."); }
			 */
			// now send...
			try {
				pubSubService.send(msg.getResponseTopic(), response);
			} catch (PubSubException e) {
				e.printStackTrace();
			}
			if (reduceExecMap == null) {
				return;
			}
			// create a list of reducers for starting
			List<Reducer> reducersForStart = new ArrayList<Reducer>();
			for (ConcurrentLinkedQueue<Reducer> reducers : reduceExecMap
					.values()) {
				// probably here is only one reducer, since each topic
				// has one reducer
				for (Reducer reducer : reducers) {
					reducersForStart.add(reducer);
				}
			}
			List<Reducer> reducersStarted = new ArrayList<Reducer>();
			boolean allReceived = false;
			boolean timeOut = false;
			int totalSleeps = 0;
			int sleep = TwisterConstants.SMALL_WAIT_INTEVAL;
			while (!(allReceived || timeOut)) {
				for (Reducer reducer : reducersForStart) {
					// get the expected ReduceInput number in a reducer
					// with a specific ID
					int reducerNo = reducer.getReducerNo();
					int numExpectedInputs = msg
							.getNumReduceInputsExpected(reducerNo);
					if (reducer.getNumReduceInputsReceived() == numExpectedInputs) {
						// execute which already got all the data
						longTaskExecutor.execute(reducer);
						reducersStarted.add(reducer);
						if (reducer.getReducerNo() == 0
								|| numExpectedInputs == 0) {
							System.out.println(numExpectedInputs + "/"
									+ reducer.getNumReduceInputsReceived()
									+ " Reducer " + reducerNo + " starts in "
									+ totalSleeps + " milliseconds on Daemon "
									+ daemonNo + ".");
						}
					}
				}
				reducersForStart.removeAll(reducersStarted);
				if (reducersForStart.isEmpty()) {
					allReceived = true;
				} else {
					// try to sleep
					try {
						Thread.sleep(sleep);
						totalSleeps += sleep;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (totalSleeps > TwisterConstants.WAIT_INTEVAL_SWITCH_TIME) {
						sleep = TwisterConstants.LARGE_WAIT_INTEVAL;
					}
				}
				if (totalSleeps > TwisterConstants.MAX_WAIT_TIME) {
					timeOut = true;
				}
			}
			if (timeOut) {
				for (Reducer reducer : reducersForStart) {
					logger.error("Reduce Task "
							+ reducer.getReducerNo()
							+ " did not receive all the inputs. So timeout occurs. Only "
							+ reducer.getNumReduceInputsReceived()
							+ " received, but "
							+ msg.getNumReduceInputsExpected(reducer
									.getReducerNo()) + " expected");
				}
			}
		}
	}

	/**
	 * send the combine data to client
	 * 
	 * currently we put it in shorTaskExecutor, because there is only one task
	 * for each job on this daemon, through it acts like a client.
	 * 
	 * @param message
	 */
	private void handleStartGather(TwisterMessage message) {
		HandleStartGatherThread handler = new HandleStartGatherThread(message);
		shortTaskExecutor.execute(handler);
	}

	/**
	 * we may have option here, one is to use dataCache and direct downloading
	 * Another is to use MSTGather
	 * 
	 * @author zhangbj
	 * 
	 */
	private class HandleStartGatherThread implements Runnable {
		private TwisterMessage message;

		HandleStartGatherThread(TwisterMessage msg) {
			message = msg;
		}

		@Override
		public void run() {
			StartGatherMessage msg = null;
		
			try {
				msg = new StartGatherMessage(message);
			} catch (SerializationException e) {
				e.printStackTrace();
			}
			if(msg == null) {
				return;
			}
			WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
			// This is the response for a bcast operation.
			// Client expects the daemonNo to be added to the refId.
			response.setRefMessageId(msg.getRefMessageId() + daemonNo);
			ReduceOutputCollectorImpl reduceCollector = reduceCollectors
					.get(msg.getJobId());
			// this daemon may not has reducer collector
			/*
			 * if (reduceCollector == null) {
			 * response.setExceptionString("Can not find the reduce collector."
			 * ); }
			 */
			try {
				pubSubService.send(msg.getResponseTopic(), response);
			} catch (PubSubException e) {
				System.out.println("Start Gather Exception " + daemonNo + " "
						+ hostIP);
				e.printStackTrace();
			}
			if (reduceCollector != null) {
				if (msg.getGatherMethod() == TwisterConstants.GATHER_IN_MST) {
					reduceCollector.gatherToCombiner(msg);
				} else {
					reduceCollector.reduceToCombiner();
				}
			}
		}
	}

	/**
	 * Adds a Value type data object to memcache. This method is called by
	 * onEvent and many bcast related method in CMDProssesor, bcast methods are
	 * processed in serverTaskExecutor
	 * 
	 * It is also called by CMDProssesor, so it is not private
	 * 
	 * @param message
	 * @throws SerializationException
	 * @throws PubSubException
	 */
	void handleMemCacheInput(TwisterMessage message) {
		HandleMemCacheInputThread handler = new HandleMemCacheInputThread(
				message);
		shortTaskExecutor.execute(handler);
	}

	private class HandleMemCacheInputThread implements Runnable {
		private TwisterMessage message;

		HandleMemCacheInputThread(TwisterMessage msg) {
			message = msg;
		}

		@Override
		public void run() {
			MemCacheInput input = null;
			try {
				input = new MemCacheInput(message);
			} catch (SerializationException e) {
				e.printStackTrace();
			}
			if (input == null) {
				return;
			}
			memCache.add(input.getJobId(), input.getKey(), input.getValue());
			boolean all = true;
			for (int i = 0; i < input.getRange(); i++) {
				Value value = memCache.get(input.getJobId(), input.getKeyBase()
						+ i);
				if (value == null) {
					all = false;
					break;
				}
			}
			if (all) {
				WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
				// Client expects the daemonNo to be added to the refId.
				response.setRefMessageId(input.getRefMessageId() + daemonNo);
				try {
					pubSubService.send(input.getResponseTopic(), response);
				} catch (PubSubException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Remove memcache, no exception can be set here.
	 * If there is exception in extracting the messaging, I cannot get the response topic.
	 * If there is exception in sending, the driver won't know. So the driver could only detect
	 * TimeOut or Daemon Failure
	 * 
	 * @param message
	 * @throws SerializationException
	 * @throws PubSubException
	 */
	private void handleMemCacheClean(TwisterMessage message) {
		MemCacheClean cleanRequest = null;
		try {
			cleanRequest = new MemCacheClean(message);
		} catch (SerializationException e) {
			cleanRequest = null;
			e.printStackTrace();
		}
		if (cleanRequest == null) {
			return;
		}
		memCache.remove(cleanRequest.getJobId());
		// This is the response for a bcast operation.
		// Client expects the daemonNo to be added to the refId.
		WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
		response.setRefMessageId(cleanRequest.getRefMessageId() + daemonNo);
		try {
			this.pubSubService.send(cleanRequest.getResponseTopic(), response);
		} catch (PubSubException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Removes the cached mappers and the reducers and send a response to the
	 * client.
	 * 
	 * @param request
	 *            - An EndMapReduceRequest.
	 * @throws TwisterException
	 * @throws PubSubException
	 * @throws SerializationException
	 * @throws InterruptedException
	 */
	private synchronized void handleMapReduceTermination(TwisterMessage request) {
		EndJobRequest endIterations = new EndJobRequest();
		try {
			endIterations.fromTwisterMessage(request);
		} catch (SerializationException e) {
			endIterations = null;
			e.printStackTrace();
		}
		if (endIterations == null) {
			return;
		}
		String jobId = endIterations.getJobId();
		Map<Integer, Mapper> mapperMap = mappers.get(jobId);
		// close mappers
		if (mapperMap != null) {
			for (Mapper mapper : mapperMap.values()) {
				try {
					mapper.close();
				} catch (TwisterException e) {
					e.printStackTrace();
				}
			}
			mapperMap.clear();
			mappers.remove(jobId);
		}
		// remove mapCollectors
		this.mapCollectors.remove(jobId);
		// remove reducers
		Map<String, ConcurrentLinkedQueue<Reducer>> reduceExecutorMap = this.reducers
				.get(jobId);
		if (reduceExecutorMap != null) {
			for (String reduceTopic : reduceExecutorMap.keySet()) {
				ConcurrentLinkedQueue<Reducer> reducers = reduceExecutorMap
						.get(reduceTopic);
				for (Reducer reducer : reducers) {
					try {
						reducer.terminate();
					} catch (TwisterException e) {
						e.printStackTrace();
					}
				}
				try {
					this.pubSubService.unsubscribe(reduceTopic);
				} catch (PubSubException e) {
					e.printStackTrace();
				}
				reducers.clear();
			}
			reduceExecutorMap.clear();
			reducers.remove(jobId);
			this.reduceCollectors.remove(jobId);
		}
		// By this time all the reducers have been terminated. We only need to
		// check if there are any reducers registered as bcast reducers.
		reduceExecutorMap = this.bcastReducers.get(jobId);
		if (reduceExecutorMap != null) {
			for (String reduceTopic : reduceExecutorMap.keySet()) {
				try {
					this.pubSubService.unsubscribe(reduceTopic);
				} catch (PubSubException e) {
					e.printStackTrace();
				}
			}
			reduceExecutorMap.clear();
			bcastReducers.remove(jobId);
		}
		// Remove the class loader. we use refCount to check if we need to
		// delete a classloader
		CustomClassLoader classLoader = classLoaders.get(jobId);
		if (classLoader != null) {
			int refCount = classLoader.decreaseRef();
			if (refCount == 0) {
				if (daemonNo == 0) {
					logger.info("Class Loader is removed");
				}
				classLoader.close();
				classLoaders.remove(jobId);
				classLoader = null;
			} else {
				classLoaders.remove(jobId);
			}
		} else {
			logger.warn("Termination request received for invalid jobId.");
		}
		// Remove memCahce objects if any.
		memCache.remove(jobId);
		// Send a response message ...
		WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
		// Client expects the daemonNo to be added to the refId.
		response.setRefMessageId(endIterations.getRefMessageId() + daemonNo);
		try {
			this.pubSubService.send(endIterations.getResponseTopic(), response);
		} catch (PubSubException e) {
			e.printStackTrace();
		}
		// do gc
		// Runtime.getRuntime().gc();
		System.gc();
	}

	/**
	 * Terminate the DaemonWorker. it is called in CMDProcessor, so it is not
	 * private. 
	 * 
	 * @throws TwisterException
	 */
	void termintate() {
		this.notifer.cancel();
		this.timerTaskExecutor.purge();
		try {
			this.pubSubService.close();
		} catch (PubSubException e) {
			logger.error("Failure happened in closing Broker Connection.");
		}
	}

	/**
	 * Listening method for all the incoming messages from the pub-sub broker
	 * network.
	 * 
	 * Notice that this is a queue, onEvent cannot run concurrently
	 */
	public void onEvent(TwisterMessage message) {
		if (message != null) {
			byte msgType = -1;
			try {
				msgType = message.readByte();
			} catch (SerializationException e) {
				msgType = -1;
				e.printStackTrace();
			}
			switch (msgType) {
			case TwisterConstants.DIR_LIST_REQ:
				handleDirList(message);
				break;
			case TwisterConstants.NEW_JOB_REQUEST:
				handleNewJobRequest(message);
				break;
			case TwisterConstants.MAPPER_REQUEST:
				handleMapperRequest(message);
				break;
			case TwisterConstants.REDUCE_WORKER_REQUEST:
				handleReducerRequest(message);
				break;
			case TwisterConstants.MAP_TASK_REQUEST:
				handleMapTask(message);
				break;
			case TwisterConstants.REDUCE_INPUT:
				handleReduceInput(message);
				break;
			case TwisterConstants.MAP_ITERATIONS_OVER:
				handleMapReduceTermination(message);
				break;
			case TwisterConstants.MEMCACHE_INPUT:
				handleMemCacheInput(message);
				break;
			case TwisterConstants.MEMCACHE_CLEAN:
				handleMemCacheClean(message);
				break;
			case TwisterConstants.START_REDUCE:
				handleStartReduce(message);
				break;
			case TwisterConstants.START_SHUFFLE:
				handleStartShuffle(message);
				break;
			case TwisterConstants.START_GATHER:
				handleStartGather(message);
				break;
			default:
				logger.error("Invalid message received by the DaemonWorker.");
			}
		}
	}
}
