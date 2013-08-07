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

package cgl.imr.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cgl.imr.base.KeyValuePair;
import cgl.imr.base.Value;
import cgl.imr.types.MemCacheAddress;

public class ExecutionPlan {
	// map configuration records
	private boolean mapConfigured;
	private List<Value> mapConfigurations;
	private String partitionFile;
	// this is recorded in "internal" method
	private Map<Integer, Integer> mapTasksMap;
	// reduce configuration records
	private boolean reduceConfigured;
	private List<Value> reduceConfigurations;
	// this is recorded in "internal" method
	private Map<Integer, Integer> reduceTasksMap;
	// combiner configuration records
	private boolean combinerConfigured;
	// memcache configuration records
	private boolean memcacheAdded;
	private MemCacheAddress memcacheAddress;
	private Map<String, Value> memcachedData;
	// run mapreduce records
	private List<KeyValuePair> keyValuePair;
	private Value bcastValue;

	ExecutionPlan() {
		this.mapConfigured = false;
		this.mapConfigurations = null;
		this.partitionFile = null;
		this.mapTasksMap = new ConcurrentHashMap<Integer, Integer>();
		this.reduceConfigured = false;
		this.reduceConfigurations = null;
		this.reduceTasksMap = new ConcurrentHashMap<Integer, Integer>();;
		this.combinerConfigured = false;
		this.memcacheAdded = false;
		this.memcacheAddress = null;
		this.memcachedData = null;
		this.keyValuePair = null;
		this.bcastValue = null;
	}

	boolean isMapConfigured() {
		return mapConfigured;
	}

	void setMapConfigured() {
		this.mapConfigured = true;
	}

	void setPartitionFile(String partitionFile) {
		this.partitionFile = partitionFile;
	}

	String getPartitionFile() {
		return partitionFile;
	}

	void setMapConfigurations(List<Value> mapConfigurations) {
		this.mapConfigurations = mapConfigurations;
	}

	List<Value> getMapConfigurations() {
		return mapConfigurations;
	}

	void setMapTasksMap(Map<Integer, TaskAssignment> mapTasksMap) {
		for (int taskID : mapTasksMap.keySet()) {
			this.mapTasksMap.put(taskID, mapTasksMap.get(taskID)
					.getAssignedDaemon());
		}
	}

	Map<Integer, Integer> getMapTasksMap() {
		return this.mapTasksMap;
	}

	boolean isReduceConfigured() {
		return reduceConfigured;
	}

	void setReduceConfigured() {
		this.reduceConfigured = true;
	}

	void setReduceConfigurations(List<Value> reduceConfigurations) {
		this.reduceConfigurations = reduceConfigurations;
	}
	
	void setReduceTasksMap(Map<Integer, TaskAssignment> reduceTasks) {
		for (int taskID : reduceTasks.keySet()) {
			this.reduceTasksMap.put(taskID, reduceTasks.get(taskID)
					.getAssignedDaemon());
		}
	}

	Map<Integer, Integer> getReduceTasksMap() {
		return this.reduceTasksMap;
	}

	List<Value> getReduceConfigurations() {
		return reduceConfigurations;
	}

	boolean isCombinerConfigured() {
		return this.combinerConfigured;
	}

	void setCombinerConfigured() {
		this.combinerConfigured = true;
	}

	boolean isMemCacheAdded() {
		return this.memcacheAdded;
	}
	
	void setMemCacheAdded(boolean added) {
		this.memcacheAdded = added;
	}

	Map<String, Value> getMemCachedData() {
		return this.memcachedData;
	}

	void setMemCachedData(Map<String, Value> memcachedData) {
		this.memcachedData = memcachedData;
	}
	
	MemCacheAddress getMemCacheAddress() {
		return this.memcacheAddress;
	}

	void setMemCacheAddress(MemCacheAddress memcacheAddress) {
		this.memcacheAddress = memcacheAddress;
	}

	void setKeyValuePair(List<KeyValuePair> lastKeyValuePair) {
		this.keyValuePair = lastKeyValuePair;
		this.bcastValue = null;
	}

	List<KeyValuePair> getKeyValuePair() {
		return keyValuePair;
	}

	void seBcastValue(Value lastBcastValue) {
		this.bcastValue = lastBcastValue;
		this.keyValuePair = null;
	}

	Value getBcastValue() {
		return this.bcastValue;
	}
}
