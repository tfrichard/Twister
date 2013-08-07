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
import java.util.Map;

import cgl.imr.base.Key;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;

public class CombineInput extends PubSubMessage {

	private String combineTopic;
	private String keyClass;
	private String valueClass;

	private Map<Key, Value> keyValues;

	private int iteration;
	private int daemonNo;
	private boolean hasData = true;

	public CombineInput() {
		this.keyValues = new HashMap<Key, Value>();
	}

	public CombineInput(String topic, int iteration, int daemonNo) {
		this();
		this.combineTopic = topic;
		this.iteration = iteration;
		this.daemonNo = daemonNo;
	}

	public void setNoHasData() {
		this.hasData = false;
	}

	public boolean isHasData() {
		return hasData;
	}

	/**
	 * assume there is no key value conflicts
	 * 
	 * @param key
	 * @param val
	 */
	public void addKeyValue(Key key, Value val) {
		if (this.keyValues.size() == 0) {
			this.keyClass = key.getClass().getName();
			this.valueClass = val.getClass().getName();
		}
		this.keyValues.put(key, val);
	}

	@Override
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {

		try {
			/*
			 * First byte is the message type It is read in message type
			 * checking in onEvent listeners, we don't need to reload this
			 * information again
			 */

			// Read the refId if any and set the boolean flag.
			readRefIdIfAny(message);
			this.iteration = message.readInt();
			this.daemonNo = message.readInt();

			// read the sink bytes
			this.combineTopic = message.readString();
			this.hasData = message.readBoolean();

			// number of <key, value> pairs
			int numKeys = message.readInt();

			if (numKeys > 0) {
				this.keyClass = message.readString();
				this.valueClass = message.readString();

				Class<?> c = null;
				Key key = null;
				Value val = null;
				for (int i = 0; i < numKeys; i++) {
					c = Class.forName(keyClass);
					key = (Key) c.newInstance();
					key.fromTwisterMessage(message);

					c = Class.forName(valueClass);
					val = (Value) c.newInstance();
					val.fromTwisterMessage(message);

					// Add the key value pair.
					this.keyValues.put(key, val);
				}
			}
		} catch (Exception e) {
			throw new SerializationException("Could not load classes", e);
		}
	}

	@Override
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {

		// First byte is the message type.
		message.writeByte(COMBINE_INPUT);

		// Write the refID if any with the boolean flag.
		serializeRefId(message);
		message.writeInt(iteration);
		message.writeInt(daemonNo);

		// Now write the sink
		message.writeString(combineTopic);
		message.writeBoolean(hasData);

		message.writeInt(this.keyValues.keySet().size());
		if (this.keyValues.keySet().size() > 0) {
			message.writeString(keyClass);
			message.writeString(valueClass);
		}

		Value value = null;
		for (Key key : this.keyValues.keySet()) {
			key.toTwisterMessage(message);
			value = this.keyValues.get(key);
			value.toTwisterMessage(message);
		}
	}

	public String getCombineTopic() {
		return combineTopic;
	}

	public Map<Key, Value> getKeyValues() {
		return this.keyValues;
	}

	public int getIteration() {
		return iteration;
	}

	public void setIteration(int iteration) {
		this.iteration = iteration;
	}

	public int getDaemonNo() {
		return daemonNo;
	}
}