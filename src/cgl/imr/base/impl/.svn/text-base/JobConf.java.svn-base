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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.TwisterSerializable;

/**
 * Configuration for MapReduce computations. Groups various properties needed by
 * the framework to execute a MapReduce computation.
 * 
 * @author Jaliya Ekanayake (jaliyae@gamil.com, jekanaya@cs.indiana.edu)
 * 
 * @author zhangbj
 * 
 */
public class JobConf implements TwisterSerializable {
	private String jobId;
	private List<String> friendJobList;
	private boolean faultTolerance;
	private int numMapTasks;
	private int numReduceTasks;
	private boolean hasMapClass;
	private String mapClass;
	private boolean hasReduceClass;
	private String reduceClass;
	private boolean hasCombinerClass;
	private String combinerClass;
	private String reducerSelectorClass;
	private boolean rowBCastSupported;
	private String rowBCastTopic;
	private int sqrtReducers;
	private Map<String, String> properties;

	private JobConf() {
		this.friendJobList = new ArrayList<String>();
		this.faultTolerance = false;
		this.hasMapClass = false;
		this.hasReduceClass = false;
		this.hasCombinerClass = false;
		// Setting the default reducer selector.
		this.reducerSelectorClass = HashBasedReducerSelector.class.getName();
		this.properties = new HashMap<String, String>();
	}

	public JobConf(String jobId) {
		this();
		this.jobId = jobId;
	}

	public JobConf(TwisterMessage message) throws SerializationException {
		this();
		this.fromTwisterMessage(message);
	}

	/**
	 * Serializes the <code>JobConf</code> object.
	 */
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		this.jobId = message.readString();
		// add friend job ID list
		int numFriendJobIDs = message.readInt();
		if (numFriendJobIDs > 0) {
			for (int i = 0; i < numFriendJobIDs; i++) {
				this.friendJobList.add(message.readString());
			}
		}
		this.faultTolerance = message.readBoolean();
		this.numMapTasks = message.readInt();
		this.numReduceTasks = message.readInt();
		this.mapClass = message.readString();
		this.hasReduceClass = message.readBoolean();
		if (this.hasReduceClass) {
			this.reduceClass = message.readString();
		}
		this.hasCombinerClass = message.readBoolean();
		if (this.hasCombinerClass) {
			this.combinerClass = message.readString();
		}
		this.reducerSelectorClass = message.readString();
		this.rowBCastSupported = message.readBoolean();
		if (this.rowBCastSupported) {
			this.rowBCastTopic = message.readString();
			this.sqrtReducers = message.readInt();
		}
		int numProperties = message.readInt();
		for (int i = 0; i < numProperties; i++) {
			this.properties.put(message.readString(), message.readString());
		}
	}

	/**
	 * De-serializes the <code>JobConf</code> object.
	 */
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		message.writeString(jobId);
		// write friend job id list
		message.writeInt(friendJobList.size());
		if (friendJobList.size() > 0) {
			for (String friendJobID : friendJobList) {
				message.writeString(friendJobID);
			}
		}
		message.writeBoolean(faultTolerance);
		message.writeInt(numMapTasks);
		message.writeInt(numReduceTasks);
		message.writeString(mapClass);
		message.writeBoolean(this.hasReduceClass);
		if (this.hasReduceClass) {
			message.writeString(reduceClass);
		}
		message.writeBoolean(this.hasCombinerClass);
		if (this.hasCombinerClass) {
			message.writeString(combinerClass);
		}
		message.writeString(reducerSelectorClass);
		message.writeBoolean(this.rowBCastSupported);
		if (this.rowBCastSupported) {
			message.writeString(rowBCastTopic);
			message.writeInt(sqrtReducers);
		}
		int numProperties = this.properties.size();
		message.writeInt(numProperties);
		// Have to write key value pairs for properties
		for (String key : this.properties.keySet()) {
			message.writeString(key);
			message.writeString(properties.get(key));
		}
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getJobId() {
		return jobId;
	}

	public void setFriendJobList(List<String> friendJobList) {
		this.friendJobList = friendJobList;
	}

	public List<String> getFriendJobList() {
		return this.friendJobList;
	}

	public void setFaultTolerance() {
		this.faultTolerance = true;
	}

	public boolean isFaultTolerance() {
		return faultTolerance;
	}

	public void setNumMapTasks(int numMapTasks) {
		this.numMapTasks = numMapTasks;
	}

	public int getNumMapTasks() {
		return numMapTasks;
	}

	public void setNumReduceTasks(int numReduceTasks) {
		this.numReduceTasks = numReduceTasks;
	}

	public int getNumReduceTasks() {
		return numReduceTasks;
	}

	public void setMapperClass(Class<?> mapClass) {
		this.mapClass = mapClass.getName();
		this.hasMapClass = true;
	}

	public String getMapClass() {
		return mapClass;
	}
	
	public boolean isHasMapClass() {
		return this.hasMapClass;
	}

	public void setReducerClass(Class<?> reduceClass) {
		this.reduceClass = reduceClass.getName();
		this.hasReduceClass = true;
	}

	public String getReduceClass() {
		return this.reduceClass;
	}

	public boolean isHasReduceClass() {
		return this.hasReduceClass;
	}

	public void setCombinerClass(Class<?> combinerClass) {
		this.combinerClass = combinerClass.getName();
		this.hasCombinerClass = true;
	}

	public String getCombinerClass() {
		return combinerClass;
	}

	public boolean isHasCombinerClass() {
		return hasCombinerClass;
	}

	public void setReducerSelectorClass(Class<?> reducerSelectorClass) {
		this.reducerSelectorClass = reducerSelectorClass.getName();
	}

	public String getReducerSelectorClass() {
		return reducerSelectorClass;
	}

	public void setRowBCastSupported(boolean rowBCastSupported) {
		this.rowBCastSupported = rowBCastSupported;
		this.rowBCastTopic = TwisterConstants.MAP_TO_REDUCE_ROW_WISE_BCAST
				+ jobId;
		if (numReduceTasks == 0) {
			throw new RuntimeException(
					"Please set the number of reduce tasks first.");
		}
		double sqrt = Math.sqrt(numReduceTasks);
		if ((Math.ceil(sqrt) - sqrt) != 0) {
			throw new RuntimeException(
					"To use the row broadcast option the number of reduce tasks must have a perfect square.");
		}
		this.sqrtReducers = (int) sqrt;
	}

	public boolean isRowBCastSupported() {
		return rowBCastSupported;
	}

	public String getRowBCastTopic() {
		return rowBCastTopic;
	}

	public int getSqrtReducers() {
		return sqrtReducers;
	}

	public boolean isHasProperties() {
		return properties.size() > 0 ? true : false;
	}

	public void setProperties(Hashtable<String, String> properties) {
		this.properties = properties;
	}

	public Map<String, String> getProperties() {
		return this.properties;
	}

	public void addProperty(String key, String val) {
		this.properties.put(key, val);
	}

	public String getProperty(String key) {
		return this.properties.get(key);
	}
}
