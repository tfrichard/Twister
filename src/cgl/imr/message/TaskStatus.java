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

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;

public class TaskStatus extends PubSubMessage {

	private String exceptionString;
	private long execuationTime;
	private boolean hasException;
	private byte status;
	private int taskNo;
	private int daemonNo;
	private byte taskType;
	private int iteration;
	private boolean hasReduceInputMap = false;
	private Map<Integer, Integer> reduceInputMap;

	private TaskStatus() {
	}

	public TaskStatus(byte taskType, byte status, int taskNo, int daemonNo,
			long execuationTime, int iteration) {
		super();
		this.taskType = taskType;
		this.status = status;
		this.taskNo = taskNo;
		this.daemonNo = daemonNo;
		this.execuationTime = execuationTime;
		this.iteration = iteration;

	}

	public Map<Integer, Integer> getReduceInputMap() {
		return reduceInputMap;
	}

	public void setReduceInputMap(Map<Integer, Integer> reduceInputMap) {
		this.hasReduceInputMap = true;
		this.reduceInputMap = reduceInputMap;
	}

	public boolean isHasReduceInputMap() {
		return hasReduceInputMap;
	}

	public TaskStatus(TwisterMessage message) throws SerializationException {
		this();
		this.fromTwisterMessage(message);
	}

	@Override
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {

		/*
		 *  First byte is the message type, already been read in onEvent
		 */

		// read the task type
		taskType = message.readByte();
		status = message.readByte();
		taskNo = message.readInt();
		daemonNo = message.readInt();
		iteration = message.readInt();
		execuationTime = message.readLong();
		hasException = message.readBoolean();
		if (hasException) {
			exceptionString = message.readString();
		}

		hasReduceInputMap = message.readBoolean();
		if (hasReduceInputMap) {
			int key;
			int value;
			int count = message.readInt();
			reduceInputMap = new HashMap<Integer, Integer>();
			for (int i = 0; i < count; i++) {
				key = message.readInt();
				value = message.readInt();
				reduceInputMap.put(key, value);
			}
		}
	}

	@Override
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {

		// First byte is the message type.
		message.writeByte(TASK_STATUS);
		message.writeByte(taskType);
		message.writeByte(status);
		message.writeInt(taskNo);
		message.writeInt(daemonNo);
		message.writeInt(iteration);
		message.writeLong(execuationTime);
		message.writeBoolean(hasException);
		if (hasException) {
			message.writeString(exceptionString);
		}

		message.writeBoolean(hasReduceInputMap);
		if (hasReduceInputMap) {
			message.writeInt(reduceInputMap.size());
			for (int key : reduceInputMap.keySet()) {
				message.writeInt(key);
				message.writeInt(reduceInputMap.get(key));
			}
		}
	}

	public String getExceptionString() {
		return exceptionString;
	}

	public long getExecuationTime() {
		return execuationTime;
	}

	public int getStatus() {
		return status;
	}

	public int getTaskNo() {
		return taskNo;
	}
	
	public int getDaemonNo() {
		return daemonNo;
	}


	public int getIteration() {
		return iteration;
	}

	public int getTaskType() {
		return taskType;
	}

	public boolean isHasException() {
		return hasException;
	}

	public void setExceptionString(String exceptionString) {
		this.hasException = true;
		this.exceptionString = exceptionString;
	}

	public void setExecuationTime(long execuationTime) {
		this.execuationTime = execuationTime;
	}

	public void setStatus(byte status) {
		this.status = status;
	}

	public void setTaskNo(int taskNo) {
		this.taskNo = taskNo;
	}
	
	public void setDaemonNo(int daemonNo) {
		this.daemonNo = daemonNo;
	}

	public void setTaskType(byte taskType) {
		this.taskType = taskType;
	}
}
