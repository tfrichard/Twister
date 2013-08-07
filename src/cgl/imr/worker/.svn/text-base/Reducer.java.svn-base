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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import cgl.imr.base.Key;
import cgl.imr.base.PubSubService;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;
import cgl.imr.base.impl.GenericBytesMessage;
import cgl.imr.message.ReduceInput;
import cgl.imr.message.ReducerRequest;
import cgl.imr.message.TaskStatus;
import cgl.imr.types.StringValue;
import cgl.imr.util.CustomClassLoader;
import cgl.imr.util.DataHolder;
import cgl.imr.util.TwisterCommonUtil;

/**
 * Executor for reduce tasks.Reducer holds the map outputs assigned to it until
 * all the outputs are received. Then it executes the reduce task.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class Reducer implements Runnable {

	private static Logger logger = Logger.getLogger(Reducer.class);

	// private String combineSink;
	// private int numMapTasks;

	private PubSubService pubsubService;
	private ReduceOutputCollectorImpl collector;

	private Map<Key, List<Value>> reduceInputs;
	private int numReduceInputsReceived = 0;
	private ReduceTask reduceTask;

	private String jobId;
	private int reducerNo;
	private int daemonNo;
	private int iteration;

	private String hostIP;
	private ConcurrentHashMap<String, DataHolder> dataCache;

	/**
	 * create a collector inside
	 * 
	 * @param pubsubService
	 * @param request
	 * @param classLoader
	 * @param dataCache
	 * @param daemonPort
	 * @param hostIP
	 * @throws TwisterException
	 */
	public Reducer(PubSubService pubsubService, ReducerRequest request,
			ReduceOutputCollectorImpl reduceCollector,
			CustomClassLoader classLoader,
			ConcurrentHashMap<String, DataHolder> dataCache, String hostIP,
			int daemonNo) throws TwisterException {

		/*
		 * there is only one request at the beginning, set at configure reducer
		 */
		this.pubsubService = pubsubService;
		this.collector = reduceCollector;
		this.hostIP = hostIP;
		this.dataCache = dataCache;
		reduceInputs = new ConcurrentHashMap<Key, List<Value>>();
		this.jobId = request.getJobConf().getJobId();
		this.iteration = request.getIteration();
		this.reducerNo = request.getReduceConf().getReduceTaskNo();
		this.daemonNo = daemonNo;
		System.out.println(this.hostIP + ": Reduce " + reducerNo
				+ " Setup. Iteration:  " + this.iteration);
		Class<?> c;
		try {
			c = classLoader.loadClass(request.getJobConf().getReduceClass());
			reduceTask = (ReduceTask) c.newInstance();
			reduceTask.configure(request.getJobConf(), request.getReduceConf());
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}

	/**
	 * Reduce task runs here.
	 */
	public void run() {
		long beginTime = 0;
		boolean hasException = false;
		try {
			this.collector.testAndSetIteration(iteration);

			beginTime = System.currentTimeMillis();
			for (Key key : reduceInputs.keySet()) {
				reduceTask.reduce(collector, key, reduceInputs.get(key));
			}
			long endTime = System.currentTimeMillis();

			// this.collector.reduceToCombiner();

			TaskStatus status = new TaskStatus(TwisterConstants.REDUCE_TASK,
					TwisterConstants.SUCCESS, this.reducerNo, this.daemonNo,
					(endTime - beginTime), iteration);
			pubsubService.send(TwisterConstants.RESPONSE_TOPIC_BASE + "/"
					+ this.jobId, status);
			// Clean reduceInputs of the reducer,then go to next iteration
			cleanReduceInputs();
			// be careful, here reducer needs to maintain the iteration count by
			// itself
			// in future we won't rely on the iteration control here, but on the
			// command sent from the driver
			iteration++;
			hasException = false;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
			hasException = true;
		}
		if (hasException) {
			TaskStatus status = new TaskStatus(TwisterConstants.REDUCE_TASK,
					TwisterConstants.FAILED, this.reducerNo, this.daemonNo,
					(System.currentTimeMillis() - beginTime), iteration);
			try {
				pubsubService.send(TwisterConstants.RESPONSE_TOPIC_BASE + "/"
						+ this.jobId, status);
			} catch (Exception e) {
				logger.error(
						"Error in reducer, could not send the exception to the client.",
						e);
			}
		}
	}

	void terminate() throws TwisterException {
		this.reduceTask.close();
		cleanReduceInputs();
	}

	/**
	 * Get reduce number won't touch the critical data region
	 * 
	 * @return
	 */
	int getReducerNo() {
		return reducerNo;
	}

	/**
	 * Used for checking if all reduce inputs received.
	 * 
	 * @return
	 */
	synchronized int getNumReduceInputsReceived() {
		return numReduceInputsReceived;
	}

	private synchronized void cleanReduceInputs() {
		this.reduceInputs.clear();
		numReduceInputsReceived = 0;
	}

	/**
	 * Adds the reduce inputs to the reduceInputs.
	 * 
	 * @param reduceInput
	 * @throws TwisterException
	 * @throws SerializationException
	 */
	synchronized void handleReduceInputMessage(ReduceInput reduceInput)
			throws TwisterException, SerializationException {
		if (!reduceInput.isHasData()) {
			reduceInput = getReduceInputFromRemoteHost(reduceInput);
		}
		/*
		 * This could be from a duplicate map task that could have stuck in the
		 * past. We can ignore it.
		 */
		if (iteration != reduceInput.getIteration()) {
			System.out.println("Duplicate at the reducer.. iteration= "
					+ iteration + " inputs =" + reduceInput.getIteration());
			return;
		}
		Map<Key, List<Value>> tmpMap = reduceInput.getKeyValues();
		List<Value> listOfValues = null;
		for (Key key : tmpMap.keySet()) {
			listOfValues = tmpMap.get(key);
			addKeyValueFromReduceInputs(key, listOfValues);
		}
		numReduceInputsReceived++;
	}

	/**
	 * add reduce input got from Bcast
	 * 
	 * @param reduceInputTmp
	 * @param newKey
	 * @throws NumberFormatException
	 * @throws TwisterException
	 * @throws SerializationException
	 */
	synchronized void handleReduceInputMessageForBcast(
			ReduceInput reduceInputTmp, Key newKey)
			throws NumberFormatException, TwisterException,
			SerializationException {

		/*
		 * ZBJ: For BCast function, we use similar processing originally here
		 * reduceInput is got from getReduceInputFromRemoteHost directly (see
		 * code above) ActiveMQ sending is not considered
		 */

		ReduceInput reduceInput = reduceInputTmp;
		if (!reduceInput.isHasData()) {
			reduceInput = getReduceInputFromRemoteHost(reduceInputTmp);
		}

		/*
		 * if fault happens, jobs are cleaned and restart with a new iteration
		 * number, output from old map tasks are ignored. This could be from a
		 * duplicate map task that could have stuck in the past. We can ignore
		 * it.
		 */
		if (iteration != reduceInput.getIteration()) {
			System.out.println("Duplicate at the reducer.. iteration= "
					+ iteration + " inputs =" + reduceInput.getIteration());
			return;
		}

		Map<Key, List<Value>> tmpMap = reduceInput.getKeyValues();

		List<Value> listOfValues = null;
		for (Key key : tmpMap.keySet()) {
			listOfValues = tmpMap.get(key);
			addKeyValueFromReduceInputs(newKey, listOfValues);
		}

		numReduceInputsReceived++;
	}

	/**
	 * synchronization for the case reduceInputs are concurrently handled. The
	 * function is invoked by hadnleREduceInputMessage and
	 * handleReduceInputMessageForBcast
	 * 
	 * @param key
	 * @param val
	 */
	private void addKeyValueFromReduceInputs(Key key, List<Value> newValues) {
		List<Value> values = this.reduceInputs.get(key);
		if (values == null) {
			values = new ArrayList<Value>();
			values.addAll(newValues);
			this.reduceInputs.put(key, values);
		} else {
			values.addAll(newValues);
		}
	}

	private ReduceInput getReduceInputFromRemoteHost(ReduceInput reduceInputTmp)
			throws NumberFormatException, TwisterException,
			SerializationException {

		/*
		 * If we come to fetch the data from remote host, There is only one
		 * keyValue pair here in outputs. The value is key to fetch the data
		 */
		StringValue memKey = null;
		Map<Key, List<Value>> tmpMap = reduceInputTmp.getKeyValues();
		for (List<Value> val : tmpMap.values()) {
			memKey = (StringValue) val.get(0);
		}

		String[] parts = memKey.toString().split(":");
		String host = parts[0].trim();
		int port = Integer.parseInt(parts[1].trim());
		String key = parts[2].trim();
		int remoteDaemonNo = Integer.parseInt(parts[3].trim());

		// System.out.println(memKey);
		byte[] data = null;

		/*
		 * for local data, we use daemonNo not hostIP to check if it is local
		 * data
		 */
		if (remoteDaemonNo == daemonNo) {
			DataHolder holder = dataCache.get(key);
			if (holder != null) {
				data = holder.getData();
				/*
				 * ZBJ: Remove the global reference of the data. In order to let
				 * gc collect the memory, otherwise there will be memory leak.
				 */
				holder.decrementDownloadCount();
				if (holder.getDowloadCount() <= 0) {
					dataCache.remove(key);
				}
			} else {
				System.out.println("No such data with Key " + key
						+ " on Daemon " + daemonNo);
				throw new TwisterException("No such data.");
			}
		} else {
			// for remote data
			List<String> keys = new ArrayList<String>();
			keys.add(key);
			Map<String, byte[]> remoteData = TwisterCommonUtil
					.getDataFromServer(host, port, keys, this.hostIP);
			data = remoteData.get(key);
		}

		/*
		 * keys fromTwisterMessage won't read the msgType byte, in order to keep
		 * data stream consistent, we need to read it here.
		 */
		TwisterMessage message = new GenericBytesMessage(data);
		byte msgType = message.readByte();
		if (msgType != TwisterConstants.REDUCE_INPUT) {
			throw new TwisterException("This is not a ReduceInput.");
		}
		return new ReduceInput(message);
	}
}
