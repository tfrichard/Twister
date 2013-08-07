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

package cgl.imr.base;

import cgl.imr.deployment.QuickDeployment;
import cgl.imr.types.StringKey;

/**
 * Common place for the constants associated with the framework.
 * 
 * @author Jaliya Ekanayake (jaliyae@gamil.com, jekanaya@cs.indiana.edu)
 * 
 */
public interface TwisterConstants {
	// Job States
	public enum JobState {
		NOT_CONFIGURED, INITIALIZING, INITIATED, 
		MAP_CONFIGURING, MAP_CONFIGURED, 
		REDUCE_CONFIGURING, REDUCE_CONFIGURED, 
		COMBINE_CONFIGURING, COMBINE_CONFIGURED, 
		MAP_STARTING, MAP_STARTED, MAP_COMPLETED, 
		SHUFFLE_STARTING, SHUFFLE_STARTED, SHUFFLE_COMPLETED, 
		REDUCE_STARTING, REDUCE_STARTED, REDUCE_COMPLETED, 
		COMBINE_STARTING, COMBINE_STARTED, COMBINE_COMPLETE, 
		ITERATION_OVER, TERMINATE_COMPLETES;
	}
	
	public enum EntityType {
		DRIVER, DAEMON;
	}
	
	public enum SendRecvStatus {
		SUCCESS, TIMEOUT, LOCAL_EXCEPTION, REMOTE_EXCEPTION, REMOTE_FALIURE;
	}

	byte COMBINE_INPUT = 11;
	String COMBINE_TOPIC_BASE = "/twister/combine/topic";
	String MAP_REDUCE_TOPIC_BASE = "/twister/map-reduce/submit/topic";
	String CLEINT_TO_WORKER_BCAST = "/twister/client/to/worker/bcast/topic";
	String MAP_TO_REDUCE_ROW_WISE_BCAST = "/twister/map/to/reduce/bcast/topic";
	byte DIR_LIST_REQ = 13;
	byte DIR_LIST_RES = 14;


	// Configurations
	String FIXED_DATA_FILE = "fixed_data";
	String FIXED_DATA_MODEL = "fixed_data_model";
	String HEP_DATA_STRING = "hep_data_string";
	byte MAP_ITERATIONS_OVER = 9;

	// Tasks
	byte MAP_TASK = 0;
	byte MAP_TASK_REQUEST = 3;

	byte  TASK_REQUEST = 10;
	// Message types
	byte MAPPER_REQUEST = 1;
	byte MONITOR_REQUEST = 7;
	byte MONITOR_RESPONSE = 8;
	byte MEMCACHE_INPUT = 18;
	byte MEMCACHE_CLEAN = 19;
	byte DAEMON_STATUS = 20;
	byte START_REDUCE = 21;

	byte DAEMON_QUIT_REQUEST = 25;
	byte DATA_REQUEST = 26;
	byte DATA_REQUEST_ACK = 27;

	byte GATHER_DATA_REQUEST = 28;
	byte GATHER_DATA_REQUEST_ACK = 29;

	int BCAST_SENDRECV_UNIT = 8192; // bytes
	int SEND_TRIES= 10; // try 3 times for re-sending
	int BCAST_BYTES_NORMAL = 1000000; // 1MB
	int BCAST_BYTES_NIGHTMARE = 10000000; //10MB
	int BCAST_BYTES_HELL = 100000000; // 100MB
	int BCAST_BYTES_INFERNO = 1000000000; // 1GB
	int BCAST_LIST_NORMAL = 1; // 1 object to bcast
	int BCAST_LIST_NIGHTMARE = 10; // 10 objects
	int BCAST_LIST_HELL = 100; // 100 objects
	int BCAST_LIST_INFERNO = 1000; // 1000 objects
	int BCAST_NODES_NORMAL = 10; // 10 nodes
	int BCAST_NODES_NIGHTMARE = 100; //100 nodes
	int BCAST_NODES_HELL = 1000; // 1k nodes
	int BCAST_NODES_INFERNO = 10000; // 110k nodes
	byte CHAIN_BCAST_START = 30;
	byte CHAIN_BCAST_FORWARD_START = 31;
	byte CHAIN_BCAST_BACKWARD_START = 32;
	byte BCAST_ACK = 33;
	byte MST_SCATTER_START = 40;
	byte MST_BCAST_FORWARD = 42;
	byte BKT_SCATTER_START = 70;
	byte BKT_BCAST_START = 71;
	byte BKT_BCAST_FORWARD = 72;
	byte MST_BCAST = 51;
	byte MSG_DESERIAL_START = 34;

	// Timings
	byte NEW_JOB_REQUEST = 16;
	byte NEW_JOB_RESPONSE = 17;

	// Topics
	String PARTITION_FILE_RESPONSE_TOPIC_BASE = "/dir/list/response/topic/";
	String PARTITION_FILE_SPLIT_PATTERN = ",";

	String PROPERTIES_FILE = QuickDeployment.getTwisterHomePath()
			+ "bin/twister.properties";
	byte REDUCE_INPUT = 5;

	byte REDUCE_RESPONSE = 6;

	byte START_SHUFFLE = 61;
	byte SHUFFLE_TASK = 62;

	byte START_GATHER = 63;
	byte GATHER_TASK = 64;
	byte GATHER_IN_DIRECT = 65;
	byte GATHER_IN_MST = 66;

	byte REDUCE_TASK = 1;
	byte REDUCE_TASK_REQUEST = 4;
	String REDUCE_TOPIC_BASE = "/twister/reduce/topic";
	byte REDUCE_WORKER_REQUEST = 2;
	String RESPONSE_TOPIC_BASE = "/twister/response/for/client/topic";
	
	// fault detection code
	byte FAULT_DETECTION = 0;
	byte FAULT_DETECTION_ACK = 1;
	long CLEANUP_AND_WAIT_TIME = 30000;

	// settings for waiting for reducers to start
	public static int SMALL_WAIT_INTEVAL = 20;
	public static int LARGE_WAIT_INTEVAL = 2000;
	public static int WAIT_INTEVAL_SWITCH_TIME = 5000;
	public static int MAX_WAIT_TIME = 600000;

	// This is for creating partition file
	long SEND_RECV_POLLNODE_MAX_SLEEP_TIME = 60000; // 1 minutes
	long SEND_RECV_NEWJOB_MAX_SLEEP_TIME = 120000; // 2 minutes
	long SEND_RECV_ENDJOB_MAX_SLEEP_TIME = 30000; // 30 seconds
	// Normal send and receive waiting
	long SEND_RECV_MAX_SLEEP_TIME = 3600000; //1 hour

	// job status
	byte SUCCESS = 0;
	byte FAILED = 1;
	// monitoring 
	long MONITOR_SLEEP_TIME = 100; // milliseconds.

	byte TASK_STATUS = 12;
	byte WORKER_RESPONSE = 15;
	byte REDUCE_INPUT_URI = 16;

	int NUM_RETRIES = 3;
	int WAIT_COUNT_FOR_FAULTS = 5000;

	StringKey fixed_key_M2R = new StringKey("fixed_key_M2R_4a616c697961");
	int map_indirect_transfer_threashold = 1024; // 512KB, 64 KB 65536
	int reduce_indirect_transfer_threashold = 1048576; // 1M
	StringKey fixed_key_R2C = new StringKey("fixed_key_R2C_4a616c697961");

	// for status notifier and fault detection
	String DAEMON_STATUS_TOPIC = "/daemon/status/topic";
	int DAEMON_STATUS_INTERVAL = 10000;	// every 10 seconds
	int FAULT_DETECTION_INTERVAL = 30000;	// every 30 seconds
	long MAX_WAIT_TIME_FOR_FAULT = 60000; // every 60 seconds
	int CONNECT_MAX_WAIT_TIME =30000; // 30 second

}
