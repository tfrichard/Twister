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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.doomdark.uuid.UUIDGenerator;

import cgl.imr.base.Key;
import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;
import cgl.imr.client.DaemonInfo;
import cgl.imr.message.CombineInput;
import cgl.imr.message.ReducerRequest;
import cgl.imr.message.StartGatherMessage;
import cgl.imr.types.StringValue;
import cgl.imr.util.DataHolder;
import cgl.imr.util.MSTGatherer;

/**
 * Collect the output of a Reduce task that need to be passed to the
 * CombineTask.
 * 
 * @author Jaliya Ekanayake (jaliyae@gamil.com, jekanaya@cs.indiana.edu)
 * 
 */
public class ReduceOutputCollectorImpl implements ReduceOutputCollector {

	private PubSubService pubsubService;

	private CombineInput combineInput;
	private String combineTopic;
	private int iteration;
	// private int reduceNo;

	private ConcurrentHashMap<String, DataHolder> dataCache;
	private int daemonNo;
	private int daemonPort;
	private String hostIP;
	
	private Map<Integer, DaemonInfo> daemonInfo;

	public ReduceOutputCollectorImpl(PubSubService pubsubService,
			ReducerRequest request,
			ConcurrentHashMap<String, DataHolder> dataCache, int daemonNo,
			String hostIP, int daemonPort, Map<Integer, DaemonInfo> daemonInfo) {

		this.pubsubService = pubsubService;

		this.combineTopic = request.getCombineTopic();
		this.iteration = request.getIteration();
		// this.reduceNo = request.getReduceConf().getReduceTaskNo();

		this.combineInput = new CombineInput(this.combineTopic, this.iteration,
				this.daemonNo);

		this.dataCache = dataCache;
		this.daemonNo = daemonNo;
		this.hostIP = hostIP;
		this.daemonPort = daemonPort;
		this.daemonInfo = daemonInfo;
	}

	public synchronized void collect(Key key, Value val) {
		combineInput.addKeyValue(key, val);
	}

	private void rebornCombineInput() {
		this.combineInput = new CombineInput(this.combineTopic, this.iteration,
				this.daemonNo);
	}

	synchronized boolean testAndSetIteration(int iteration) {
		if (this.iteration < iteration) {
			this.iteration = iteration;
			rebornCombineInput();
			return true;
		} else if (this.iteration == iteration) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Send data to combiner, it will be called within reducer.
	 * 
	 * @throws PubSubException
	 */
	synchronized void reduceToCombiner() {
		try {
			// There should be only one combiner.
			TwisterMessage newInputMsg = copyDataToCacheIfLargeAndGetCombineInput(
					combineInput, 1);
			// System.out.println("Send to combiner " + this.daemonNo);
			pubsubService.send(combineInput.getCombineTopic(), newInputMsg);
			rebornCombineInput();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private TwisterMessage copyDataToCacheIfLargeAndGetCombineInput(
			CombineInput input, int numReceivers) {
		TwisterMessage inputMsg = null;
		try {
			inputMsg = this.pubsubService.createTwisterMessage();
			input.toTwisterMessage(inputMsg);

			// if it is big message, we will serialize it to bytes.
			if (inputMsg.getLength() > TwisterConstants.reduce_indirect_transfer_threashold) {

				byte[] inputData = inputMsg.getBytesAndDestroy();

				if (this.daemonNo == 0) {
					System.out.println("Reducer TCP Direct Downloading "
							+ this.daemonNo + ": " + inputData.length
							+ " iteration: " + iteration);
				}

				String cacheKey = UUIDGenerator.getInstance()
						.generateTimeBasedUUID().toString();
				Value tmpVal = new StringValue(hostIP + ":" + daemonPort + ":"
						+ cacheKey);
				this.dataCache.put(cacheKey, new DataHolder(inputData,
						numReceivers));
				input.getKeyValues().clear();
				input.addKeyValue(TwisterConstants.fixed_key_R2C, tmpVal);
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
	 * Get gatherer and do gather
	 */
	synchronized void gatherToCombiner(StartGatherMessage msg) {
		try {
			TwisterMessage inputMsg = this.pubsubService.createTwisterMessage();
			this.combineInput.toTwisterMessage(inputMsg);
			int me = this.daemonNo;
			Map<Integer, Integer> daemonRankMap = msg.getReduceDaemonRankMap();
			for (int rank : daemonRankMap.keySet()) {
				if (this.daemonNo == daemonRankMap.get(rank)) {
					me = rank;
					break;
				}
			}
			MSTGatherer gatherer = new MSTGatherer(msg.getJobId(),
					msg.getKeyBase(), me, daemonInfo,
					msg.getReduceDaemonRankMap(), dataCache, inputMsg);
			gatherer.gatherInMST(msg.getRoot(), msg.getLeft(), msg.getRight());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
