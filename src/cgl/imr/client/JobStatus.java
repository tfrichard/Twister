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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import cgl.imr.message.TaskStatus;

/**
 * Presents the state of the job to the user. Keeps track of successful/failed
 * tasks.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 * @author zhangbj
 * 
 */
public class JobStatus {
	private AtomicInteger numSuccessfulMapTasks;
	private AtomicInteger numFailedMapTasks;
	private Map<Integer, TaskStatus> mapTaskStatuses;
	private AtomicInteger numSuccessfulReduceTasks;
	private AtomicInteger numFailedReduceTasks;
	private Map<Integer, TaskStatus> reduceTaskStatuses;
	private AtomicInteger numCombineInputsReceived;
	private boolean success = true;

	public JobStatus() {
		this.numSuccessfulMapTasks = new AtomicInteger(0);
		this.numFailedMapTasks = new AtomicInteger(0);
		this.numSuccessfulReduceTasks = new AtomicInteger(0);
		this.numFailedReduceTasks = new AtomicInteger(0);
		this.numCombineInputsReceived = new AtomicInteger(0);
		this.mapTaskStatuses = new ConcurrentHashMap<Integer, TaskStatus>();
		this.reduceTaskStatuses = new ConcurrentHashMap<Integer, TaskStatus>();
	}

	void setFailed() {
		success = false;
	}

	public boolean isSuccess() {
		return success;
	}

	void addMapTaskStatus(TaskStatus mapStatus) {
		this.mapTaskStatuses.put(mapStatus.getTaskNo(), mapStatus);
	}

	public Map<Integer, TaskStatus> getMapTaskStatuses() {
		return mapTaskStatuses;
	}

	void incrementSuccessfulMapTasks() {
		numSuccessfulMapTasks.addAndGet(1);
	}

	public int getNumSuccessfulMapTasks() {
		return numSuccessfulMapTasks.get();
	}

	void incrementFailedMapTasks() {
		numFailedMapTasks.addAndGet(1);
	}

	public int getNumFailedMapTasks() {
		return numFailedMapTasks.get();
	}

	void addReduceTaskStatus(TaskStatus reduceStatus) {
		this.reduceTaskStatuses.put(reduceStatus.getTaskNo(), reduceStatus);
	}

	public Map<Integer, TaskStatus> getReduceTaskStatuses() {
		return reduceTaskStatuses;
	}

	void incrementSuccessfulReduceTasks() {
		numSuccessfulReduceTasks.addAndGet(1);
	}

	public int getNumSuccessfulReduceTasks() {
		return numSuccessfulReduceTasks.get();
	}

	void incrementFailedReduceTasks() {
		numFailedReduceTasks.addAndGet(1);
	}

	public int getNumFailedReduceTasks() {
		return numFailedReduceTasks.get();
	}

	void incrementCombineInputs() {
		numCombineInputsReceived.addAndGet(1);
	}

	public int getNumCombineInputsReceived() {
		return numCombineInputsReceived.get();
	}
}
