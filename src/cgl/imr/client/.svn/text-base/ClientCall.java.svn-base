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
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.Subscribable;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterConstants.EntityType;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.TwisterSerializable;
import cgl.imr.base.impl.PubSubFactory;
import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;

/**
 * A utility class to send/receive messages using pub-sub infrastructure.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class ClientCall implements Subscribable, Runnable {

	private static Logger logger = Logger.getLogger(ClientCall.class);

	private static long MAX_WAIT_TIME = 60000; // one minute.
	private static long SLEEP_TIME = 5; // milliseconds
	private TwisterConfigurations mrConfig;
	private int noOfResponse = 1;
	private int noOfResponseReceived = 0;

	private PubSubService pubSubService;

	private boolean responseReceived = false;
	private List<TwisterMessage> responses;

	public ClientCall() throws TwisterException {
		this.responses = new ArrayList<TwisterMessage>();

		try {
			mrConfig = TwisterConfigurations.getInstance();
		} catch (ConfigurationException e1) {
			throw new TwisterException(e1);
		}

		try {
			int entityId = new Random(System.currentTimeMillis()).nextInt() * 1000000; // entity
																						// ids
																						// for
																						// drivers
																						// starts
																						// from
																						// 100000
																						// and
																						// above.
			this.pubSubService = PubSubFactory.getPubSubService(mrConfig,
					EntityType.DRIVER, entityId);
			this.pubSubService.setSubscriber(this);
			this.pubSubService.start();
		} catch (PubSubException e) {
			if (this.pubSubService != null) {
				try {
					this.pubSubService.close();
				} catch (PubSubException e1) {
					// ignore.
				}
			}
			throw new TwisterException(e);
		}
		responses = new ArrayList<TwisterMessage>();// Initialize the response list
	}

	public synchronized void close() throws TwisterException {
		try {
			this.pubSubService.close();
		} catch (PubSubException e) {
			throw new TwisterException(e);
		}

	}

	public void onEvent(TwisterMessage message) {
		noOfResponseReceived++;
		responses.add(message);
		if (noOfResponseReceived == noOfResponse) {
			responseReceived = true;
		}
	}

	/**
	 * Reset the caller.
	 */
	private void reset() {
		this.responseReceived = false;
		this.noOfResponseReceived = 0;
		this.noOfResponse = 1;
		this.responses.clear();
	}

	public void run() {
		long curTime = System.currentTimeMillis();
		while (!responseReceived) {
			try {
				Thread.sleep(SLEEP_TIME);
			} catch (InterruptedException e) {
			}
			if ((System.currentTimeMillis() - curTime) > MAX_WAIT_TIME) {
				logger
						.error("Maximum wait time reached waiting for responses at ClientCall.");
				break;
			}
		}
	}

	/**
	 * This method acts as the request-response invocation using pub/sub First,
	 * it subscribe to the response topic, and send the request. Caller can
	 * specify the number of responses expected.
	 * 
	 * @param pubTopic
	 *            - Topic to send data.
	 * @param subTopic
	 *            - Topic to receive data.
	 * @param data
	 *            - Data to be sent.
	 * @return List of received byte[]s corresponding to incomming messags.
	 * @throws TwisterException
	 */
	public synchronized List<TwisterMessage> sendReceive(String pubTopic,
			String subTopic, TwisterSerializable data) throws TwisterException {

		try {
			// Reset Call
			reset();

			this.pubSubService.subscribe(subTopic);

			Thread th = new Thread(this);
			th.start();

			pubSubService.send(pubTopic, data);
			try {
				th.join();
			} catch (InterruptedException e) {
				throw new TwisterException(e);
			}
			this.pubSubService.unsubscribe(subTopic);
		} catch (PubSubException e) {
			throw new TwisterException(e);
		}
		return responses;
	}
}
