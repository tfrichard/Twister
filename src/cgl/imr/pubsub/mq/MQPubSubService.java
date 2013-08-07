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

package cgl.imr.pubsub.mq;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.SerializationException;
import cgl.imr.base.Subscribable;
import cgl.imr.base.TwisterConstants.EntityType;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.TwisterSerializable;
import cgl.imr.config.ConfigurationException;

/**
 * Register this client with ActiveMQ, Similar functionality as the classes for
 * NaradaBrokering connection.
 * 
 * Every topic has a session to hold a related message consumer. So for one
 * connection, there is a mapping between topics and Sessions (or Consumers)
 * Once message for a specific topic is received, onMessage will invoke
 * Subscriber's onEvent function to handle it.
 * 
 * Notice: Use one session for several Subscribers may cause consumer hang at
 * setMessageListener. As a result, every session only hold one consumer.
 * 
 * @author Bingjing Zhang (zhangbj@indiana.edu) 6/13/2010
 * 
 *         To support multiple connections.
 * 
 * @author Bingjing Zhang (zhangbj@indiana.edu) 07/27/2011
 * 
 *         revise all
 * @author Bingjing Zhang (zhangbj@indiana.edu) 05/28/2011
 * 
 */
public class MQPubSubService implements PubSubService, MessageListener {
	private int daemonNo;
	private String driverConnectionKey;
	private String primeConnectionKey;
	// the class which will handle the message event
	private Subscribable subscriber = null;
	// the mapping between topics and Subscription
	private Map<String, Subscription> topics;
	// the mapping between connection keys and connections
	private Map<String, Connection> connections;
	private AtomicBoolean isStarted;

	private class Subscription {
		private Map<Session, MessageConsumer> consumers = null;

		private Subscription() {
			consumers = new ConcurrentHashMap<Session, MessageConsumer>();
		}

		private void addConsumer(Session session, MessageConsumer consumer) {
			consumers.put(session, consumer);
		}

		private void close() throws JMSException {
			for (Session session : consumers.keySet()) {
				session.close();
			}
			consumers.clear();
		}
	}

	/**
	 * Constructor, create connection according to amq.properties
	 * 
	 * @param type
	 * @param daemonNo
	 * @throws PubSubException
	 */
	public MQPubSubService(EntityType type, int daemonNo)
			throws PubSubException {
		// load ActiveMQ connection configurations
		MQConfigurations config = null;
		try {
			config = new MQConfigurations(type);
		} catch (ConfigurationException e) {
			throw new PubSubException(
					"Errors happen in ActiveMQ configuration.", e);
		}
		this.daemonNo = daemonNo;
		this.driverConnectionKey = MQConfigurations.key_uri_base
				+ MQConfigurations.key_uri_index_start;
		this.primeConnectionKey = null;
		// establish hashmap for mapping
		this.topics = new ConcurrentHashMap<String, Subscription>();
		// ConcurrentHashMap should work to all kinds of concurrent access
		this.connections = new ConcurrentHashMap<String, Connection>();
		// key starts from index 0
		int index = MQConfigurations.key_uri_index_start;
		// create connection factory
		for (int i = 0; i < config.getPropertiesSize(); i++) {
			String connectionURI = null;
			connectionURI = config
					.getURI(MQConfigurations.key_uri_base + index);
			// could be null, driver node doesn't have prime broker
			if (index == 1) {
				if (connectionURI == null) {
					primeConnectionKey = null;
					index++;
					connectionURI = config.getURI(MQConfigurations.key_uri_base
							+ index);
				} else {
					primeConnectionKey = MQConfigurations.key_uri_base + index;
				}
			}
			// should not be null
			if (connectionURI == null) {
				throw new PubSubException(
						"Errors happen in ActiveMQ configuration.");
			}
			// create connection
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
					connectionURI);
			// connectionFactory.setCopyMessageOnSend(false);
			// connectionFactory.setUseDedicatedTaskRunner(false);
			// connectionFactory.setOptimizeAcknowledge(true);
			try {
				Connection connection = connectionFactory.createConnection();
				this.connections.put(MQConfigurations.key_uri_base + index,
						connection);
			} catch (JMSException e) {
				throw new PubSubException(
						"Errors happen in creating ActiveMQ connection"
								+ MQConfigurations.key_uri_base + index
								+ " on " + this.daemonNo + ".", e);
			}
			index++;
		}
		this.isStarted = new AtomicBoolean(false);
	}

	/**
	 * Start the service
	 */
	@Override
	public void start() throws PubSubException {
		for (Connection connection : this.connections.values()) {
			try {
				connection.start();
			} catch (JMSException e) {
				throw new PubSubException(
						"Errors happen in starting ActiveMQ connection on "
								+ this.daemonNo + ".", e);
			}
		}
		this.isStarted.set(true);
	}

	/**
	 * close connection. All sessions will be destroyed automatically. Should
	 * notice that there could be any other sending activity happening when
	 * closing
	 */
	@Override
	public void close() throws PubSubException {
		this.isStarted.set(false);
		for (Connection connection : this.connections.values()) {
			try {
				connection.close();
			} catch (JMSException e) {
				throw new PubSubException(
						"Errors happen in closing connection on "
								+ this.daemonNo + ".", e);
			}
		}
		this.topics.clear();
		this.connections.clear();
	}

	/**
	 * get main connection
	 * 
	 * @return
	 */
	private String getMainConnectionKey() {
		String mainKey = null;
		if (this.primeConnectionKey != null) {
			return this.primeConnectionKey;
		}
		if (this.driverConnectionKey != null) {
			return this.driverConnectionKey;
		}
		if (this.connections.size() > 0) {
			mainKey = new ArrayList<String>(this.connections.keySet()).get(0);
		}
		return mainKey;
	}

	private void send(String connectionKey, String topic,
			TwisterSerializable series) throws PubSubException, JMSException,
			SerializationException {
		Connection connection = this.connections.get(connectionKey);
		if (connection == null) {
			throw new PubSubException("No such a connection key "
					+ connectionKey + " on daemon " + this.daemonNo + ".");
		}
		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		TwisterMessage msg = new MQMessage(session.createBytesMessage(),
				session);
		series.toTwisterMessage(msg);
		MessageProducer producer = session.createProducer(session
				.createTopic(topic));
		producer.send(((MQMessage) msg).getBytesMessage());
		session.close();
	}

	/**
	 * handle failure, remove the failed connection
	 * 
	 * @param connectionKey
	 * @throws PubSubException
	 */
	private void handleSendingFailure(String connectionKey)
			throws PubSubException {
		Connection connection = this.connections.remove(connectionKey);
		if (connection != null) {
			try {
				connection.close();
			} catch (JMSException e) {
				e.printStackTrace();
			}
			if (connectionKey.equals(this.driverConnectionKey)) {
				driverConnectionKey = null;
			}
			if (connectionKey.equals(this.primeConnectionKey)) {
				primeConnectionKey = null;
			}
		}
		if (this.connections.size() == 0) {
			throw new PubSubException("No ActiveMQ connection available on "
					+ this.daemonNo + ".");
		}
	}

	/**
	 * Create a session and a producer inside to send byte message, catch if
	 * fail and resend. Do sending till the time you have no connection
	 * available.
	 */
	@Override
	public void send(String topic, TwisterSerializable series)
			throws PubSubException {
		if(!this.isStarted.get()) {
			throw new PubSubException("ActiveMQ connection is not started on "
					+ this.daemonNo + ".");
		}
		boolean fault = false;
		do {
			String connectionKey = getMainConnectionKey();
			try {
				send(connectionKey, topic, series);
				break;
			} catch (Exception e) {
				fault = true;
				e.printStackTrace();
			}
			if (fault) {
				System.err.println("Failure in sending a message for topic "
						+ topic + " on " + this.daemonNo + " with connection "
						+ connectionKey + ".");
				handleSendingFailure(connectionKey);
			}
		} while (fault);
	}

	@Override
	/**
	 * catch if fail and recreate a new message
	 */
	public TwisterMessage createTwisterMessage() throws PubSubException {
		String connectionKey = getMainConnectionKey();
		try {
			Session session = this.connections.get(connectionKey)
					.createSession(false, Session.AUTO_ACKNOWLEDGE);
			TwisterMessage msg = new MQMessage(session.createBytesMessage(),
					session);
			return msg;
		} catch (Exception e) {
			e.printStackTrace();
			handleSendingFailure(connectionKey);
		}
		return null;
	}

	@Override
	/**
	 * Since message is connected to the session, I cannot do resending here
	 * if the connection has some problem.
	 */
	public void send(String topic, TwisterMessage message)
			throws PubSubException {
		if (!this.isStarted.get()) {
			throw new PubSubException("ActiveMQ connection is not started on "
					+ this.daemonNo + ".");
		}
		try {
			MQMessage msg = (MQMessage) message;
			Session session = msg.getSession();
			MessageProducer producer = session.createProducer(session
					.createTopic(topic));
			producer.send(msg.getBytesMessage());
			session.close();
		} catch (Exception e) {
			throw new PubSubException(e);
		}
	}

	@Override
	/**
	 * set the class who will use this MQPubSubService.
	 * It is called subscriber
	 * once the message comes, this class will handle it.
	 */
	public void setSubscriber(Subscribable callback) throws PubSubException {
		if (this.subscriber != null) {
			throw new PubSubException(
					"ActiveMQ:  Susbcriber has been set. on Daemon "
							+ this.daemonNo + ".");
		}
		if (callback != null) {
			this.subscriber = callback;
		} else {
			throw new PubSubException(
					"ActiveMQ:  Susbcriber cannot be NULL on Daemon "
							+ this.daemonNo + ".");
		}
	}

	/**
	 * create session, and hold a consumer (durable subscriber) inside
	 */
	@Override
	public void subscribe(String topic) throws PubSubException {
		if (this.subscriber != null) {
			Subscription sub = null;
			/*
			 * see if we have the subscription for this topic. If we already
			 * have the subscription object, That means we have all the
			 * subscription on all the connections, so just try to return
			 */
			synchronized (topics) {
				if (!topics.containsKey(topic)) {
					sub = new Subscription();
					topics.put(topic, sub);
				} else {
					return;
				}
			}
			// System.out.println("Start creating consumers " + topic);
			for (String connectionKey : this.connections.keySet()) {
				try {
					// create session, since topic may subscribe after
					// connection start
					Session session = this.connections.get(connectionKey)
							.createSession(false, Session.AUTO_ACKNOWLEDGE);
					Topic tpc = session.createTopic(topic);
					MessageConsumer consumer = session.createConsumer(tpc);
					// problem found when multiple consumers share one session,
					// setMessageListener may be stuck there
					consumer.setMessageListener(this);
					sub.addConsumer(session, consumer);
				} catch (JMSException e) {
					e.printStackTrace();
					handleSendingFailure(connectionKey);
				}
				// System.out.println("Finish creating consumers " + topic);
			}
		} else {
			throw new PubSubException("ActiveMQ: no subscriber on "
					+ this.daemonNo + ".");
		}
	}

	@Override
	/**
	 * Unsubscribe a topic,
	 * close the session, 
	 * remove the mapping record.
	 * 
	 */
	public void unsubscribe(String topic) throws PubSubException {
		try {
			Subscription sub = (Subscription) topics.remove(topic);
			sub.close();
		} catch (JMSException e) {
			throw new PubSubException(
					"ActiveMQ: error in unsubscribing topic on "
							+ this.daemonNo + ".", e);
		}
	}

	/**
	 * Once message arrives, build a byte message, use subscriber class to
	 * handle this event
	 */
	@Override
	public void onMessage(Message mqMessage) {
		if (this.subscriber != null) {
			this.subscriber.onEvent(new MQMessage((BytesMessage) mqMessage,
					null));
		}
	}
}
