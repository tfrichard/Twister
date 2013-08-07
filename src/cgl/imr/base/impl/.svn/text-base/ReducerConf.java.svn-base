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

package cgl.imr.base.impl;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.TwisterSerializable;
import cgl.imr.base.Value;

/**
 * Configuration for Reducer. Has a task no and a property bag.
 * 
 * @author Jaliya Ekanayake (jaliyae@gamil.com, jekanaya@cs.indiana.edu)
 * 
 */
public class ReducerConf implements TwisterConstants, TwisterSerializable {
	private ClassLoader classLoader;
	private boolean hasValue = false;
	private int reduceTaskNo;
	private Value value;

	private String valueClass;

	private ReducerConf() {
	}

	public ReducerConf(TwisterMessage msg, ClassLoader classLoader)
			throws SerializationException {
		this();
		this.classLoader = classLoader;
		this.fromTwisterMessage(msg);
	}

	public ReducerConf(int taskNo) {
		this();
		this.reduceTaskNo = taskNo;
	}

	public ReducerConf(int taskNo, Value value) {
		this(taskNo);
		this.value = value;
		this.hasValue = true;
		this.valueClass = value.getClass().getCanonicalName();
	}

	/**
	 * Construct the object from the Serialized bytes.
	 */
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		this.reduceTaskNo = message.readInt();
		this.hasValue = message.readBoolean();

		if (this.hasValue) {
			this.valueClass = message.readString();

			Class<?> c;
			try {
				c = Class.forName(valueClass, true, classLoader);
				this.value = (Value) c.newInstance();
			} catch (Exception e) {
				throw new SerializationException("Could not load the class.", e);
			}

			this.value.fromTwisterMessage(message);
		}
	}

	/**
	 * Serialize the object.
	 */
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		message.writeInt(this.reduceTaskNo);

		message.writeBoolean(hasValue);

		if (this.hasValue) {
			message.writeString(valueClass);
			value.toTwisterMessage(message);
		}
	}

	public int getReduceTaskNo() {
		return reduceTaskNo;
	}

	public Value getValue() {
		return value;
	}

}
