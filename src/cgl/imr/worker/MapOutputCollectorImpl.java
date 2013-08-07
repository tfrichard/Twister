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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.doomdark.uuid.UUIDGenerator;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.PubSubService;
import cgl.imr.base.ReducerSelector;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.message.MapperRequest;
import cgl.imr.message.ReduceInput;
import cgl.imr.message.TaskStatus;
import cgl.imr.types.StringValue;
import cgl.imr.util.CustomClassLoader;
import cgl.imr.util.DataHolder;

/**
 * Collector for Map outputs. All the public access methods are made
 * synchronized.
 * 
 * @author Jaliya Ekanayake (jaliyae@gamil.com, jekanaya@cs.indiana.edu)
 * 
 * @author zhangbj
 */
public class MapOutputCollectorImpl implements MapOutputCollector {

	private String jobId;
	private PubSubService pubsubService;

	private int daemonNo;
	private String hostIP;
	private int daemonPort;
	private ConcurrentHashMap<String, DataHolder> dataCache;

	private int iteration;
	private boolean hasReduceClass;
	private ReducerSelector reduceSelector;
	// A map to record the distribution of reducInputs on reducers
	private Map<Integer, Integer> reduceInputMap;
	// This is the map for KeyValue pairs collected
	private Map<Integer, ReduceInput> reduceInputs;

	/*
	 * This part is for supporting rowBcast
	 */
	private boolean bcastSupported = false;
	private String bcastTopicBase;
	private int sqrtReducers; // the number of reducers for receiving a bcast
								// value
	private Map<Integer, ReduceInput> rowBCastValues;

	/**
	 * This is the shared collector for each job, to do data collectoon of map
	 * outputs, and send to reducers
	 * 
	 * @param jobConf
	 * @param pubsubService
	 * @param mapperRequest
	 * @param dataCache
	 * @param daemonNo
	 * @param hostIP
	 * @param daemonPort
	 * @throws TwisterException
	 */
	public MapOutputCollectorImpl(JobConf jobConf, PubSubService pubsubService,
			MapperRequest mapperRequest,
			ConcurrentHashMap<String, DataHolder> dataCache, int daemonNo,
			String hostIP, int daemonPort) throws TwisterException {
		this.jobId = jobConf.getJobId();
		this.pubsubService = pubsubService;
		this.dataCache = dataCache;
		this.daemonNo = daemonNo;
		this.hostIP = hostIP;
		this.daemonPort = daemonPort;

		this.iteration = mapperRequest.getIteration();
		this.hasReduceClass = jobConf.isHasReduceClass();

		CustomClassLoader classLoader = DaemonWorker.getClassLoader(jobConf
				.getJobId());
		this.reduceSelector = getReducerSelector(jobConf, classLoader,
				mapperRequest.getReduceTopicBase());

		this.reduceInputs = new HashMap<Integer, ReduceInput>();
		this.reduceInputMap = new HashMap<Integer, Integer>();

		if (jobConf.isRowBCastSupported()) {
			this.bcastSupported = true;
			this.bcastTopicBase = jobConf.getRowBCastTopic();
			this.sqrtReducers = jobConf.getSqrtReducers();
			this.rowBCastValues = new HashMap<Integer, ReduceInput>();
		}
	}

	/**
	 * Method to load reducer selector for a specific job
	 * 
	 * @param reducerSink
	 * @return
	 * @throws TwisterException
	 */
	private ReducerSelector getReducerSelector(JobConf jobConf,
			CustomClassLoader classLoader, String reduceTopicBase)
			throws TwisterException {
		Class<?> c;
		try {
			c = Class.forName(jobConf.getReducerSelectorClass(), true,
					classLoader);
			ReducerSelector reducerSelector = (ReducerSelector) c.newInstance();
			reducerSelector.configure(jobConf, reduceTopicBase);
			return reducerSelector;
		} catch (Exception e) {
			throw new TwisterException("Could not load reducer selector.", e);
		}
	}

	/**
	 * reduceInputMap is to record the reduceInput count on each reducer for one
	 * collector, there should be only one count for each reducer according to
	 * the behavior of collect method. But for collectBCastToRow. there could be
	 * many count
	 * 
	 * @param reduceNo
	 */
	private void markReduceInputMap(int reduceNo) {
		if (!reduceInputMap.containsKey(reduceNo)) {
			reduceInputMap.put(reduceNo, 1);
		} else {
			int count = reduceInputMap.get(reduceNo);
			reduceInputMap.put(reduceNo, ++count);
		}
	}

	/**
	 * Key Values are put into ReduceInput Message, which is sent to the a
	 * reducer. This is for traditional reducer, we move from synchronized method
	 * to synchronized object to create more parallel.
	 */
	public synchronized void collect(Key key, Value val) {
		ReduceInput reduceInput = null;
		int reduceNo = reduceSelector.getReducerNumber(key);
		reduceInput = reduceInputs.get(reduceNo);
		if (reduceInput == null) {
			reduceInput = new ReduceInput(iteration);
			reduceInput.setReduceTopic(reduceSelector.getReduceTopicBase()
					+ reduceNo);
			reduceInput.setJobId(reduceSelector.getJobId());
			reduceInputs.put(reduceNo, reduceInput);
			markReduceInputMap(reduceNo);
		}
		reduceInput.collectKeyValue(key, val);
	}

	@Override
	public synchronized void collect(Map<Key, Value> keyvals) {
		for (Key key : keyvals.keySet()) {
			ReduceInput reduceInput = null;
			int reduceNo = reduceSelector.getReducerNumber(key);
			reduceInput = reduceInputs.get(reduceNo);
			if (reduceInput == null) {
				reduceInput = new ReduceInput(iteration);
				reduceInput.setReduceTopic(reduceSelector.getReduceTopicBase()
						+ reduceNo);
				reduceInput.setJobId(reduceSelector.getJobId());
				reduceInputs.put(reduceNo, reduceInput);
				markReduceInputMap(reduceNo);
			}
			reduceInput.collectKeyValue(key, keyvals.get(key));
		}
	}

	/**
	 * collect keyvalues for reducebcast
	 */
	public synchronized void collectBCastToRow(int rowNum, Key key, Value val) {
		if (!bcastSupported) {
			throw new RuntimeException(
					"Please enable Broadcast to row option using JobConf at the TwisterDriver.");
		}

		/*
		 * Because the KeyValue pairs collected will be bcasted to several
		 * reducers, mark all of them.
		 */
		int begin = rowNum * sqrtReducers;
		int end = begin + sqrtReducers;
		for (int i = begin; i < end; i++) {
			markReduceInputMap(i);
		}

		ReduceInput bcastReduceInput = rowBCastValues.get(rowNum);
		if (bcastReduceInput == null) {
			bcastReduceInput = new ReduceInput(this.iteration);
			bcastReduceInput.setReduceTopic(bcastTopicBase + rowNum);
			bcastReduceInput.setJobId(reduceSelector.getJobId());
			rowBCastValues.put(rowNum, bcastReduceInput);
		}

		/*
		 * we need to make sure if the keyvalue pairs collected here are
		 * mergeable. Do not let them be merged wrongly.
		 */
		bcastReduceInput.collectKeyValue(key, val);
	}

	/**
	 * set iteration
	 */
	synchronized boolean testAndSetIteration(int iteration) {
		if (this.iteration < iteration) {
			this.iteration = iteration;
			this.reduceInputs.clear();
			this.reduceInputMap.clear();
			if (this.bcastSupported) {
				this.rowBCastValues.clear();
			}
			return true;
		} else if (this.iteration == iteration) {
			return true;
		} else {
			return false;
		}
	}

	private TwisterMessage copyDataToCacheIfLargeAndGetReduceInput(
			ReduceInput input, int numReceivers) {

		TwisterMessage inputMsg = null;
		try {
			inputMsg = this.pubsubService.createTwisterMessage();
			input.toTwisterMessage(inputMsg);

			// ZBJ: Test if it is direct down-load
			if (inputMsg.getLength() > TwisterConstants.map_indirect_transfer_threashold) {
				byte[] inputData = inputMsg.getBytesAndDestroy();

				if (this.daemonNo == 0) {
					System.out
							.println("MapOutputCollector TCP Direct Downloading on "
									+ this.daemonNo
									+ ": "
									+ inputData.length
									+ " iteration: " + this.iteration);
				}

				String cacheKey = UUIDGenerator.getInstance()
						.generateTimeBasedUUID().toString();

				/*
				 * Here we add the fourth attribute daemonNo for data location
				 * checking
				 */
				Value tmpVal = new StringValue(hostIP + ":" + daemonPort + ":"
						+ cacheKey + ":" + daemonNo);
				this.dataCache.put(cacheKey, new DataHolder(inputData,
						numReceivers));
				input.getKeyValues().clear();
				input.collectKeyValue(TwisterConstants.fixed_key_M2R, tmpVal);
				input.setNoHasData();

				TwisterMessage newInputMsg = this.pubsubService
						.createTwisterMessage();
				input.toTwisterMessage(newInputMsg);
				return newInputMsg;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return inputMsg;
	}

	/**
	 * do shuffle
	 */
	synchronized void shuffle() {
		boolean hasException = false;
		Exception exception = null;
		long beginTime = System.currentTimeMillis();
		try {
			// check if we really have reducers
			if (hasReduceClass) {
				// try to randomize the sending order
				List<Integer> keyList = new ArrayList<Integer>(
						this.reduceInputs.keySet());
				Random randomGenerator = new Random(this.daemonNo);
				while (!keyList.isEmpty()) {
					int reduceNo = keyList.remove(randomGenerator
							.nextInt(keyList.size()));
					ReduceInput input = this.reduceInputs.get(reduceNo);
					// One map output goes to one reducer
					TwisterMessage newInputMsg = copyDataToCacheIfLargeAndGetReduceInput(
							input, 1);
					this.pubsubService
							.send(input.getReduceTopic(), newInputMsg);
				}
				/*
				for (ReduceInput input : this.reduceInputs.values()) {
					// One map output goes to one reducer
					TwisterMessage newInputMsg = copyDataToCacheIfLargeAndGetReduceInput(
							input, 1);
					this.pubsubService
							.send(input.getReduceTopic(), newInputMsg);
				}
				*/
				if (this.bcastSupported) {
					for (ReduceInput input : this.rowBCastValues.values()) {
						// sqrtReducers is the number of receiver of the data
						TwisterMessage newInputMsg = copyDataToCacheIfLargeAndGetReduceInput(
								input, this.sqrtReducers);
						this.pubsubService.send(input.getReduceTopic(),
								newInputMsg);
					}
				}
				long endTime = System.currentTimeMillis();
				// send a response to driver here 
				TaskStatus status = new TaskStatus(
						TwisterConstants.SHUFFLE_TASK,
						TwisterConstants.SUCCESS, this.daemonNo, this.daemonNo,
						(endTime - beginTime), iteration);
				status.setReduceInputMap(this.reduceInputMap);
				this.pubsubService.send(TwisterConstants.RESPONSE_TOPIC_BASE
						+ "/" + this.jobId, status);
				// then do clean
				this.reduceInputs.clear();
				this.reduceInputMap.clear();
				if (this.bcastSupported) {
					this.rowBCastValues.clear();
				}
			}
			hasException = false;
		} catch (Exception e) {
			e.printStackTrace();
			hasException = true;
			exception = e;
		}
		// if there is exception
		if (hasException) {
			TaskStatus status = new TaskStatus(TwisterConstants.SHUFFLE_TASK,
					TwisterConstants.FAILED, this.daemonNo, this.daemonNo,
					(System.currentTimeMillis() - beginTime), iteration);
			status.setExceptionString(exception.getMessage());
			try {
				this.pubsubService.send(TwisterConstants.RESPONSE_TOPIC_BASE
						+ "/" + this.jobId, status);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
