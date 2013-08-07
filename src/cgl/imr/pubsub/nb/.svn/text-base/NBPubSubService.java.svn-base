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

package cgl.imr.pubsub.nb;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.SerializationException;
import cgl.imr.base.Subscribable;
import cgl.imr.base.TwisterConstants.EntityType;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.TwisterSerializable;
import cgl.imr.base.impl.GenericBytesMessage;
import cgl.imr.config.ConfigurationException;
import cgl.narada.event.NBEvent;
import cgl.narada.event.TemplateProfileAndSynopsisTypes;
import cgl.narada.matching.Profile;
import cgl.narada.service.ServiceException;
import cgl.narada.service.client.ClientService;
import cgl.narada.service.client.EventConsumer;
import cgl.narada.service.client.EventProducer;
import cgl.narada.service.client.NBEventListener;
import cgl.narada.service.client.SessionService;

/**
 * Abstract the NaradaBrokering's pub/sub communication. This class creates a
 * single connection to the broker and use it to send and receive messages.
 * 
 * A similar implementation may allow other pub/sub communication frameworks to
 * be used with Twister.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 *         11/24/2009
 * 
 */
public class NBPubSubService implements PubSubService, NBEventListener {

	private ClientService clientService;
	private NBConfigurations config;
	private EventConsumer consumer;
	private int entityId;
	private EventProducer producer;
	private Map<String, Profile> profiles;
	private Subscribable subscriber;

	public NBPubSubService(EntityType type, int daemonNo)
			throws PubSubException {
		try {
			config = new NBConfigurations();
		} catch (ConfigurationException e) {
			throw new PubSubException("NaradaBrokering configuration error.", e);
		}
		this.entityId = daemonNo + 100000;// daemon numbers starts from
											// 100000.new
											// Random(System.currentTimeMillis()).nextInt();
		System.out.println("ENTITY ID:- " + this.entityId);
		profiles = new ConcurrentHashMap<String, Profile>();
		try {
			clientService = SessionService.getClientService(this.entityId);
			Properties props = new Properties();
			if (type.equals(EntityType.DRIVER)) {
				props.put("hostname", config.getMainBrokerHost());
			} else {
				props.put("hostname", config.getABrokerHost(daemonNo
						% config.getNumBrokers()));
			}
			props.put("portnum", config.brokerPort);
			clientService
					.initializeBrokerCommunications(props, config.commType);
			System.out.println("Connected Broker = "
					+ props.getProperty("hostname"));
			// Initialize the EventProducer
			producer = clientService.createEventProducer();
			producer.generateEventIdentifier(true);
			producer.setTemplateId(new Random().nextInt());
			producer.setDisableTimestamp(false);
		} catch (ServiceException e) {
			throw new PubSubException("NaradaBrokering connection error.", e);
		}
	}

	/**
	 * Currently we put it as an empty call
	 */
	@Override
	public void start() throws PubSubException {
	}

	/**
	 * Cleanup the NaradaBrokering connections and profiles.
	 */
	public void close() throws PubSubException {
		try {
			for (String key : profiles.keySet()) {
				consumer.unSubscribe(profiles.get(key));
			}
			clientService.closeBrokerConnection();
			clientService.terminateServices();
		} catch (ServiceException se) {
			throw new PubSubException(
					"Could not unsubscribe from the profile.", se);
		}
	}

	/**
	 * Callback method for the NaradaBrokering. This method simply calls the
	 * subscribable's <code>onEvent()</code> method.
	 */
	public void onEvent(NBEvent nbMessage) {
		if (this.subscriber != null) {
			this.subscriber.onEvent(new GenericBytesMessage(nbMessage
					.getContentPayload()));
		}
	}

	/**
	 * Sends a <code>byte[]</code> message to a given <code>String</code> topic.
	 */
	public void send(String topic, TwisterSerializable series)
			throws PubSubException {

		try {
			TwisterMessage message = this.createTwisterMessage();
			series.toTwisterMessage(message);

			NBEvent nbEvent = producer.generateEvent(
					TemplateProfileAndSynopsisTypes.STRING, topic,
					message.getBytesAndDestroy());
			producer.publishEvent(nbEvent);
		} catch (ServiceException se) {
			throw new PubSubException(se);
		} catch (SerializationException e) {
			throw new PubSubException(e);
		}
	}

	@Override
	public void send(String topic, TwisterMessage message)
			throws PubSubException {
		try {
			NBEvent nbEvent = producer.generateEvent(
					TemplateProfileAndSynopsisTypes.STRING, topic,
					message.getBytesAndDestroy());
			producer.publishEvent(nbEvent);
		} catch (ServiceException se) {
			throw new PubSubException(se);
		} catch (SerializationException e) {
			throw new PubSubException(e);
		}
	}

	/**
	 * Sets the subscribe object for this <code>PubSubService</code>.
	 */
	public void setSubscriber(Subscribable callback) throws PubSubException {
		if (callback != null) {
			try {
				this.consumer = clientService.createEventConsumer(this);
			} catch (ServiceException e) {
				throw new PubSubException(e);
			}
			this.subscriber = callback;
		} else {
			throw new PubSubException("Susbcriber cannot be null");
		}
	}

	/**
	 * Subscribe to a topic.
	 */
	public void subscribe(String topic) throws PubSubException {
		if (this.subscriber != null) {
			if (!profiles.containsKey(topic)) {
				try {
					Profile profile = clientService.createProfile(
							TemplateProfileAndSynopsisTypes.STRING, topic);
					profiles.put(topic, profile);
					consumer.subscribeTo(profile);
				} catch (ServiceException e) {
					throw new PubSubException(
							"NaradaBrokering connection error in creating the consumer.",
							e);
				}
			}
		} else {
			throw new PubSubException(
					"No subscriber to call. Please set a subscriber.");
		}
	}

	/**
	 * Unsubscribe from a topic.
	 */
	public void unsubscribe(String topic) throws PubSubException {
		try {
			consumer.unSubscribe(profiles.get(topic));
			profiles.remove(topic);
		} catch (ServiceException se) {
			throw new PubSubException(se);
		}
	}

	@Override
	public TwisterMessage createTwisterMessage() throws PubSubException {
		return new GenericBytesMessage(null);
	}
}
