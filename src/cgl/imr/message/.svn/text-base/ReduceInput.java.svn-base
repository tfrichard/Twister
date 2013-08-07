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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cgl.imr.base.Key;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;
import cgl.imr.util.CustomClassLoader;
import cgl.imr.worker.DaemonWorker;

/**
 * Message carrying the map outputs to the reducers. Holds a
 * <code> Map<Key,List<Value>> </code>.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class ReduceInput extends PubSubMessage {

	private String jobId;
	private String keyClass;
	private String valueClass;

	private boolean hasData;
	private Map<Key, List<Value>> keyValues;

	/*
	 * the topic of destination of ReduceInput sending
	 */
	private String reduceTopic;
	private int iteration;

	public ReduceInput() {
		this.keyClass = null;
		this.valueClass = null;
		this.hasData = true;
		this.keyValues = new HashMap<Key, List<Value>>();
	}

	public ReduceInput(int iteration) {
		this();
		this.iteration = iteration;
	}

	public ReduceInput(TwisterMessage input) throws SerializationException {
		this();
		this.fromTwisterMessage(input);
	}

	/**
	 * If key exits, add value, else add new key and values. There is one key
	 * type and value type allowed. If you change key type during the
	 * collection, keyClass and valueClass will be replaced. Then there will be
	 * problem.
	 */
	public void collectKeyValue(Key key, Value val) {
		/*
		 * Set keyValues class name assume all key values stored here use the
		 * same key and value class, otherwise print warning info, one is to
		 * check key class, another is to check value class
		 */
		if (this.keyValues.isEmpty()) {
			this.keyClass = key.getClass().getName();
			this.valueClass = val.getClass().getName();
		} else {
			if (!key.getClass().getName().equals(this.keyClass)) {
				System.out.println("Redefined keyClass: "
						+ key.getClass().getName() + ". Original keyClass: "
						+ this.keyClass);
			}
			if (!val.getClass().getName().equals(this.valueClass)) {
				System.out.println("Redefined valueClass: "
						+ val.getClass().getName() + ". Original valueClass: "
						+ this.valueClass);
			}
		}
		/*
		 * If there is no such a key, create a new value list and add the value.
		 * If the key is already exist, check if it is mergeable. Be careful
		 * about this operation! Once the key is mergeable, we will do merge on
		 * value no matter if its merge operation has proper implementation. And
		 * we also assume there is only one value in the list if the key is
		 * mergeable.
		 */
		List<Value> values = this.keyValues.get(key);
		if (values == null) {
			values = new ArrayList<Value>();
			values.add(val);
			this.keyValues.put(key, values);
		} else {
			if (key.isMergeableInShuffle()) {
				Value mergedValue = values.get(0);
				mergedValue.mergeInShuffle(val);
			} else {
				values.add(val);
			}
		}
	}

	@Override
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		/*
		 * First byte is the message type, already been read in onEvent
		 */

		// Read the refId if any and set the boolean flag.
		readRefIdIfAny(message);
		this.iteration = message.readInt();
		this.jobId = message.readString();

		// read the sink bytes
		this.reduceTopic = message.readString();

		// has real data or the indirect keys.
		this.hasData = message.readBoolean();
		// Number of key, list<value> pairs
		int numKeys = message.readInt();

		if (numKeys > 0) {
			this.keyClass = message.readString();
			this.valueClass = message.readString();

			try {
				CustomClassLoader classLoader = DaemonWorker
						.getClassLoader(jobId);
				if (classLoader == null) {
					throw new SerializationException(
							"Could not find a class loader for this job id.");
				}

				Class<?> kClass = Class.forName(keyClass, true, classLoader);
				Class<?> vClass = Class.forName(valueClass, true, classLoader);

				Key key = null;
				for (int i = 0; i < numKeys; i++) {
					key = (Key) kClass.newInstance();
					key.fromTwisterMessage(message);
					// Now see how many values are there under this key.
					int numValues = message.readInt();
					List<Value> values = new ArrayList<Value>();
					for (int j = 0; j < numValues; j++) {
						Value val = (Value) vClass.newInstance();
						val.fromTwisterMessage(message);
						values.add(val);
					}
					this.keyValues.put(key, values);
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new SerializationException("Could not load classes", e);
			}
		}
	}

	@Override
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {

		// First byte is the message type.
		message.writeByte(REDUCE_INPUT);

		// Write the refID if any with the boolean flag.
		serializeRefId(message);
		message.writeInt(iteration);

		// Now write the job
		message.writeString(jobId);

		// Now write the sink, the receiver of reduceInput
		message.writeString(reduceTopic);

		message.writeBoolean(hasData);

		/*
		 * Next, write the number of <key <List of values>> pairs in this
		 * message for the intended reducer.
		 */
		message.writeInt(keyValues.keySet().size());

		if (keyValues.keySet().size() > 0) {
			message.writeString(keyClass);
			message.writeString(valueClass);
		}

		List<Value> values = null;
		for (Key key : keyValues.keySet()) {
			key.toTwisterMessage(message);
			values = keyValues.get(key);
			message.writeInt(values.size());
			for (Value val : values) {
				val.toTwisterMessage(message);
			}
		}
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getJobId() {
		return jobId;
	}
	
	public void setNoHasData() {
		this.hasData = false;
	}

	public boolean isHasData() {
		return hasData;
	}

	public int getIteration() {
		return iteration;
	}

	public Map<Key, List<Value>> getKeyValues() {
		return keyValues;
	}

	public void setReduceTopic(String reduceTopic) {
		this.reduceTopic = reduceTopic;
	}
	
	public String getReduceTopic() {
		return reduceTopic;
	}
}
