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

package cgl.imr.worker;

import java.util.Map;

import org.apache.log4j.Logger;

import cgl.imr.base.Key;
import cgl.imr.base.MapTask;
import cgl.imr.base.PubSubService;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.message.MapTaskRequest;
import cgl.imr.message.MapperRequest;
import cgl.imr.message.TaskStatus;
import cgl.imr.util.CustomClassLoader;

/**
 * Executor for the map tasks. Executor holds the map task configuration and the
 * <code>DaemonWorker</code> schedules them for execution depending on the
 * requests from the <code>TwisterDriver</code>.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class Mapper implements Runnable {

	private static Logger logger = Logger.getLogger(Mapper.class);

	private MapTaskRequest currentRequest = null;
	private MapOutputCollectorImpl collector;
	private JobConf jobConf;
	private MapTask mapTask;
	private int mapTaskNo;
	private int daemonNo;
	private PubSubService pubsubService;

	public Mapper(PubSubService pubsubService, MapperRequest mapperRequest,
			MapOutputCollectorImpl collector, CustomClassLoader classLoader,
			int daemonNo) throws TwisterException {
		this.collector = collector;
		this.pubsubService = pubsubService;
		this.mapTaskNo = mapperRequest.getMapTaskNo();
		this.daemonNo = daemonNo;
		this.jobConf = mapperRequest.getJobConf();
		MapperConf mapperConf = mapperRequest.getMapConf();
		try {
			String className = jobConf.getMapClass();
			Class<?> c = Class.forName(className, true, classLoader);
			this.mapTask = (MapTask) c.newInstance();
			this.mapTask.configure(jobConf, mapperConf);
		} catch (Exception e) {
			throw new TwisterException("Could not instantiate the Mapper.", e);
		}
	}

	void close() throws TwisterException {
		if (this.mapTask != null) {
			mapTask.close();
		}
	}

	/**
	 * Map Task runs here.
	 * 
	 */
	public void run() {
		boolean hasException = false;
		Exception exception = null;
		long beginTime = 0;
		int iteration = 0;
		try {
			if (currentRequest == null) {
				throw new TwisterException("No map request to execute.");
			}
			iteration = currentRequest.getIteration();
			// multiple request will cause iteration be set several times
			// collector.testAndSetIteration(iteration);
			beginTime = System.currentTimeMillis();
			Map<Key, Value> keyValueMap = currentRequest.getKeyValues();
			for (Key key : keyValueMap.keySet()) {
				mapTask.map(collector, key, keyValueMap.get(key));
			}
			long endTime = System.currentTimeMillis();
			TaskStatus status = new TaskStatus(TwisterConstants.MAP_TASK,
					TwisterConstants.SUCCESS, mapTaskNo, daemonNo,
					(endTime - beginTime), iteration);
			this.pubsubService.send(TwisterConstants.RESPONSE_TOPIC_BASE + "/"
					+ jobConf.getJobId(), status);
			// set no exception here
			hasException = false;
		} catch (Exception e) {
			e.printStackTrace();
			hasException = true;
			exception = e;
			logger.error(e);
		}
		// handle exceptions
		if (hasException) {
			TaskStatus status = new TaskStatus(TwisterConstants.MAP_TASK,
					TwisterConstants.FAILED, mapTaskNo, daemonNo,
					(System.currentTimeMillis() - beginTime), iteration);
			status.setExceptionString(exception.getMessage());
			try {
				this.pubsubService.send(TwisterConstants.RESPONSE_TOPIC_BASE
						+ "/" + jobConf.getJobId(), status);
			} catch (Exception e) {
				logger.error(e);
			}
		}
	}

	void setCurrentRequest(MapTaskRequest currentRequest) {
		// not only set request, but aslo set params for collector
		this.currentRequest = currentRequest;
		if (this.currentRequest != null) {
			int iteration = currentRequest.getIteration();
			// multiple request will cause iteration be set several times
			collector.testAndSetIteration(iteration);
		}
	}

	MapTaskRequest getCurrentRequest() {
		return this.currentRequest;
	}
}
