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

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.worker.DaemonWorker;

/**
 * Message sent requesting mapper task.
 * Mapper will be reused by MapTaskRequest
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class MapperRequest extends PubSubMessage {
	private JobConf jobConf;
	private MapperConf mapperConf;
	private int mapTaskNo;
	// it is important to keep iterationCount, to know the start of iteration
	private int iteration;
	private String reduceTopicBase;
	private String responseTopic;

	MapperRequest() {
	}

	public MapperRequest(TwisterMessage request) throws SerializationException {
		this();
		this.fromTwisterMessage(request);
	}

	public MapperRequest(JobConf jobConf, MapperConf mapConf, int iteration) {
		this.jobConf = jobConf;
		this.mapperConf = mapConf;
		this.mapTaskNo = mapConf.getMapTaskNo();
		this.iteration = iteration;
	}

	/**
	 * Notice that First byte is the message type, it is already read in onEvent
	 * message type checking
	 */
	@Override
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		// Read the refId if any and set the boolean flag.
		readRefIdIfAny(message);
		mapTaskNo = message.readInt();
		iteration = message.readInt();
		reduceTopicBase = message.readString();
		responseTopic = message.readString();
		jobConf = new JobConf(message);
		ClassLoader loader = DaemonWorker.getClassLoader(jobConf.getJobId());
		if (loader == null) {
			throw new SerializationException(
					"Could not find a class loader for this job Id.");
		}
		mapperConf = new MapperConf(message, loader);
	}

	@Override
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		message.writeByte(MAPPER_REQUEST);
		// Write the refID if any with the boolean flag.
		serializeRefId(message);
		message.writeInt(mapTaskNo);
		message.writeInt(iteration);
		message.writeString(reduceTopicBase);
		message.writeString(responseTopic);
		jobConf.toTwisterMessage(message);
		mapperConf.toTwisterMessage(message);
	}

	public JobConf getJobConf() {
		return jobConf;
	}

	public int getIteration() {
		return iteration;
	}

	public MapperConf getMapConf() {
		return mapperConf;
	}

	public int getMapTaskNo() {
		return mapTaskNo;
	}

	public String getResponseTopic() {
		return responseTopic;
	}

	public void setMapConf(MapperConf mapConf) {
		this.mapperConf = mapConf;
	}

	public void setResponseTopic(String responseTopic) {
		this.responseTopic = responseTopic;
	}
	
	public void setReduceTopicBase(String reduceTopicBase) {
		this.reduceTopicBase = reduceTopicBase;
	}
	
	public String getReduceTopicBase() {
		return this.reduceTopicBase;
	}
}
