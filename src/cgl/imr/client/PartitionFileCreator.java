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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.SerializationException;
import cgl.imr.base.Subscribable;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterConstants.EntityType;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.impl.PubSubFactory;
import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.message.DirListRequest;
import cgl.imr.message.DirListResponse;
import cgl.imr.message.PubSubMessage;
import cgl.imr.message.WorkerResponse;

/**
 * A utility class to create a partition file - meta data file- accumulating the
 * meta data about the the data partitions. This class assumes that the data is
 * already distributed to the compute nodes.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class PartitionFileCreator implements Subscribable {

	private static Logger logger = Logger.getLogger(PartitionFileCreator.class);

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out
					.println("Usage: [sub directory under data directory][file filter][partition file]");
			System.out
					.println("E.g.[sub directory under data directory]= /mytest/data ");
			System.out
					.println("this means that the actual path will be /path/to/data/dir/in/twiser.properties/mytest/data");
			return;
		}
		PartitionFileCreator pc;
		try {
			pc = new PartitionFileCreator();
			pc.pollNodesAndCreateParitionFile(args[0], args[1], args[2]);
			pc.close();
			System.out.println("Partition file created.");
			System.exit(0);
		} catch (Exception e) {
			logger.error("PartitionFileCreate failed.", e);
			System.exit(-1);
		}
	}

	private TwisterConfigurations configs;
	private PubSubService pubSubService;

	private org.safehaus.uuid.UUIDGenerator uuidGen = null;
	private String responseTopic = null;
	private List<String> nodes = null;
	protected ConcurrentLinkedQueue<TwisterMessage> responseMap = new ConcurrentLinkedQueue<TwisterMessage>();

	public PartitionFileCreator() throws TwisterException, PubSubException {
		try {
			this.configs = TwisterConfigurations.getInstance();
		} catch (ConfigurationException e) {
			throw new TwisterException(e);
		}

		populateListOfNodes();
		this.uuidGen = UUIDGenerator.getInstance();
		String refId = uuidGen.generateTimeBasedUUID().toString();
		responseTopic = TwisterConstants.PARTITION_FILE_RESPONSE_TOPIC_BASE
				+ refId;

		this.pubSubService = PubSubFactory.getPubSubService(configs,
				EntityType.DRIVER, (int) System.currentTimeMillis());
		this.pubSubService.setSubscriber(this);
		this.pubSubService.subscribe(responseTopic);
		this.pubSubService.start();
	}

	public void close() throws PubSubException {
		if (pubSubService != null) {
			this.pubSubService.close();
		}
	}

	protected List<DirListResponse> bcastRequestsAndReceiveResponses(
			PubSubMessage message, int numResponse) throws PubSubException,
			SerializationException {
		List<WorkerResponse> errors = new ArrayList<WorkerResponse>();
		List<DirListResponse> dirListResponses = new ArrayList<DirListResponse>();
		setRefMessage(message);
		pubSubService.send(TwisterConstants.CLEINT_TO_WORKER_BCAST, message);
		// Keep checking for responses.
		boolean resReceived = false;
		boolean timeOut = false;
		long removeCount = 0;
		long waitedTime = 0;
		long waitingStartTime = System.currentTimeMillis();
		TwisterMessage response = null;
		while (!(resReceived || timeOut)) {
			response = responseMap.poll();
			if (response != null) {
				removeCount++;
				if (response.readByte() == TwisterConstants.WORKER_RESPONSE) {
					WorkerResponse res = new WorkerResponse(response);
					errors.add(res);
				} else {
					DirListResponse res = new DirListResponse();
					res.fromTwisterMessage(response);
					dirListResponses.add(res);
				}
			}
			// All responses received.
			if (removeCount == numResponse) {
				resReceived = true;
			} else {
				// Wait and see.
				waitedTime = System.currentTimeMillis() - waitingStartTime;
				if (waitedTime > TwisterConstants.SEND_RECV_POLLNODE_MAX_SLEEP_TIME) {
					// Timeout has reached. Check one more time and
					// consider the pending requests as failed.
					timeOut = true;
				}
			}
		}
		if (timeOut) {
			while ((response = responseMap.poll()) != null) {
				if (response.readByte() == TwisterConstants.WORKER_RESPONSE) {
					WorkerResponse res = new WorkerResponse(response);
					errors.add(res);
				} else {
					DirListResponse res = new DirListResponse();
					res.fromTwisterMessage(response);
					dirListResponses.add(res);
				}
			}
		}
		if (errors.size() > 0) {
			System.out.println(errors.size()
					+ " throws excpetions. Printing the first one.");
			WorkerResponse res = errors.get(0);
			if (res.isHasException()) {
				System.out.println(errors.get(0).getExceptionString());
			} else {
				System.out
						.println("No exception, but did not receive a directory listing.");
			}
		}
		return dirListResponses;
	}

	protected String setRefMessage(PubSubMessage msg) {
		String refMsgId = uuidGen.generateTimeBasedUUID().toString();
		msg.setRefMessageId(refMsgId);
		return refMsgId;
	}

	public void pollNodesAndCreateParitionFile(String dir, String filter,
			String partitionFile) throws TwisterException {
		dir = configs.getLocalDataDir() + "/" + dir;
		dir = dir.replace("//", "/");
		int fileNo = 0;
		List<String> tmpFiles = null;
		int numDaemonsPerNode = configs.getDamonsPerNode();
		int numDaemons = nodes.size() * numDaemonsPerNode;

		DirListRequest req = new DirListRequest(dir, filter, responseTopic);

		List<DirListResponse> responses = null;
		try {
			responses = bcastRequestsAndReceiveResponses(req, numDaemons);
		} catch (Exception e) {
			throw new TwisterException("Could not send the dir list requests.",
					e);
		}

		try {
			BufferedWriter bw = new BufferedWriter(
					new FileWriter(partitionFile));
			for (DirListResponse res : responses) {
				tmpFiles = res.getFileNames();
				if (tmpFiles != null) {
					for (String file : tmpFiles) {
						bw.write(fileNo + "," + res.getDaemonIP() + ","
								+ res.getDaemonNo() + "," + file + "\n");
						fileNo++;
					}
				}
			}
			bw.flush();
			bw.close();
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}

	private void populateListOfNodes() throws TwisterException {
		BufferedReader reader;
		nodes = new ArrayList<String>();
		try {
			reader = new BufferedReader(new FileReader(configs.getNodeFile()));
			String line = null;
			while ((line = reader.readLine()) != null) {
				nodes.add(line);
			}
			reader.close();
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}

	@Override
	public void onEvent(TwisterMessage message) {
		responseMap.add(message);
	}
}
