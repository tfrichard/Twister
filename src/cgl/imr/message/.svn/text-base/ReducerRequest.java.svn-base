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
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.worker.DaemonWorker;

/**
 * Message requesting a reducer.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class ReducerRequest extends PubSubMessage {

	private String combineTopic;
	private JobConf jobConf;
	private ReducerConf reducerConf;
	private String reduceTopic;
	private String responseTopic;
	private int iteration;

	protected ReducerRequest() {
	}

	public ReducerRequest(TwisterMessage request) throws SerializationException {
		this();
		this.fromTwisterMessage(request);
	}

	public ReducerRequest(JobConf jobConf, ReducerConf reduceConf,
			String reduceTopic, String responseTopic, String combineTopic,
			int iteration) {
		this.jobConf = jobConf;
		this.reducerConf = reduceConf;
		this.reduceTopic = reduceTopic;
		this.responseTopic = responseTopic;
		this.combineTopic = combineTopic;
		this.iteration = iteration;
	}

	@Override
	public void fromTwisterMessage(TwisterMessage message) throws SerializationException {

		try {
			// First byte is the message type
			/*
			 * byte msgType = message.readByte(); if (msgType !=
			 * REDUCE_WORKER_REQUEST) { throw new SerializationException(
			 * "Invalid set of bytes to deserialize " +
			 * this.getClass().getName() + "."); }
			 */

			// Read the refId if any and set the boolean flag.
			readRefIdIfAny(message);
			this.iteration = message.readInt();

			this.reduceTopic = message.readString();

			this.responseTopic = message.readString();

			this.combineTopic = message.readString();

			this.jobConf = new JobConf(message);
			// this.jobConf.fromBytes(data);

			ClassLoader loader = DaemonWorker
					.getClassLoader(jobConf.getJobId());
			if (loader == null) {
				throw new SerializationException(
						"Could not find a class loader for this job Id.");
			}

			this.reducerConf = new ReducerConf(message, loader);
			// this.reduceConf.fromBytes(data);

		} catch (Exception ioe) {
			throw new SerializationException(ioe);
		}
	}

	@Override
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {

		message.writeByte(REDUCE_WORKER_REQUEST);

		// Write the refID if any with the boolean flag.
		serializeRefId(message);
		
		message.writeInt(iteration);

		message.writeString(reduceTopic);

		message.writeString(responseTopic);

		message.writeString(combineTopic);

		jobConf.toTwisterMessage(message);

		reducerConf.toTwisterMessage(message);
	}

	public String getCombineTopic() {
		return combineTopic;
	}

	public JobConf getJobConf() {
		return jobConf;
	}

	public int getIteration() {
		return iteration;
	}

	public ReducerConf getReduceConf() {
		return reducerConf;
	}

	public String getReduceTopic() {
		return reduceTopic;
	}

	public String getResponseTopic() {
		return responseTopic;
	}

	public void setCombineTopic(String combineTopic) {
		this.combineTopic = combineTopic;
	}

	public void setJobConf(JobConf jobConf) {
		this.jobConf = jobConf;
	}

	public void setReduceConf(ReducerConf reduceConf) {
		this.reducerConf = reduceConf;
	}

	public void setReduceTopic(String reduceTopic) {
		this.reduceTopic = reduceTopic;
	}

	public void setResponseTopic(String responseTopic) {
		this.responseTopic = responseTopic;
	}

}
