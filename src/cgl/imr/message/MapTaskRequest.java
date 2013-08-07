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

package cgl.imr.message;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cgl.imr.base.Key;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;
import cgl.imr.util.CustomClassLoader;
import cgl.imr.worker.DaemonWorker;

/**
 * Request for map tasks.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 *         Modified for adding cross-mapper collector
 * @author zhangbj
 * 
 */
public class MapTaskRequest extends PubSubMessage {
	private String jobId;
	private int mapTaskNo;
	private String keyClass;
	private String valClass;
	private Map<Key, Value> keyValues;
	private String responseTopic;

	/*
	 * It is important to know the current iteration
	 */
	private int iteration;
	// private String sinkBase;

	protected MapTaskRequest() {
		mapTaskNo = 0;
		this.keyValues = new HashMap<Key, Value>();
	}

	public MapTaskRequest(TwisterMessage request) throws SerializationException {
		this();
		this.fromTwisterMessage(request);
	}

	public MapTaskRequest(int mapTaskNo, int iteration) {
		this();
		this.mapTaskNo = mapTaskNo;
		this.iteration = iteration;
	}

	public void addKeyValue(Key key, Value val) {
		if (this.keyValues.size() == 0) {
			this.keyClass = key.getClass().getName();
			this.valClass = val.getClass().getName();
		}
		this.keyValues.put(key, val);
	}

	/* Create an object using the serialized bytes. */
	@Override
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		// First byte is the message type, has already been in message
		// Read the refId if any and set the boolean flag.
		readRefIdIfAny(message);
		this.jobId = message.readString();
		this.mapTaskNo = message.readInt();
		this.iteration = message.readInt();
		int numKeys = message.readInt();
		if (numKeys > 0) {
			keyClass = message.readString();
			valClass = message.readString();
			CustomClassLoader classLoader = DaemonWorker.getClassLoader(jobId);
			if (classLoader == null) {
				throw new SerializationException(
						"Could not find a class loader for this job id.");
			}
			Class<?> kClass = null;
			try {
				kClass = Class.forName(keyClass, true, classLoader);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new SerializationException(e);
			}
			Class<?> vClass = null;
			try {
				vClass = Class.forName(valClass, true, classLoader);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new SerializationException(e);
			}
			Key key = null;
			Value val = null;
			for (int i = 0; i < numKeys; i++) {
				try {
					key = (Key) kClass.newInstance();
				} catch (Exception e) {
					e.printStackTrace();
					throw new SerializationException(e);
				}
				key.fromTwisterMessage(message);
				try {
					val = (Value) vClass.newInstance();
				} catch (Exception e) {
					e.printStackTrace();
					throw new SerializationException(e);
				}
				val.fromTwisterMessage(message);
				// Add the key value pair.
				addKeyValue(key, val);
			}
		}
		responseTopic = message.readString();
	}

	/**
	 * Serialize the object.
	 */
	@Override
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		message.writeByte(MAP_TASK_REQUEST);
		// Write the refID if any with the boolean flag.
		serializeRefId(message);
		message.writeString(jobId);
		message.writeInt(mapTaskNo);
		message.writeInt(iteration);
		message.writeInt(keyValues.keySet().size());
		if (keyValues.keySet().size() > 0) {
			message.writeString(keyClass);
			message.writeString(valClass);
			Value value = null;
			for (Key key : keyValues.keySet()) {
				key.toTwisterMessage(message);
				value = keyValues.get(key);
				value.toTwisterMessage(message);
			}
		}
		message.writeString(responseTopic);
	}

	public String getJobId() {
		return jobId;
	}

	public Map<Key, Value> getKeyValues() {
		return keyValues;
	}

	public int getMapTaskNo() {
		return mapTaskNo;
	}

	public int getIteration() {
		return iteration;
	}

	public String getResponseTopic() {
		return responseTopic;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public void setKeyValues(Map<Key, Value> keyValues) {
		Iterator<Key> ite = keyValues.keySet().iterator();
		Key key = null;
		while (ite.hasNext()) {
			key = ite.next();
			this.addKeyValue(key, keyValues.get(key));
		}
	}

	public void setResponseTopic(String responseTopic) {
		this.responseTopic = responseTopic;
	}
}
