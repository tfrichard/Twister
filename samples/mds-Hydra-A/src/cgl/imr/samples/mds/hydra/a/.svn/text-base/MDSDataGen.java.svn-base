package cgl.imr.samples.mds.hydra.a;

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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.KeyValuePair;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.IntKey;
import cgl.imr.types.StringValue;

/**
 * Generate data for K-Means clustering using MapReduce. It uses a "map-only"
 * operation to generate data concurrently.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */
public class MDSDataGen {

	private static String DATA_FILE_SUFFIX = /* ".bin" */"";
	public static String NUM_DATA_PER_MAP = "data_points_per_map";
	public static String NUM_DATA = "data_points";

	public static void generateIDSFile(String idsFile, long numDataPoints)
			throws IOException {
		BufferedWriter idsWriter = new BufferedWriter(new FileWriter(idsFile));
		idsWriter.write(numDataPoints + "\n");// First line is the count.

		for (long i = 0; i < numDataPoints; i++) {
			idsWriter.write(i + "\n");
		}
		idsWriter.flush();
		idsWriter.close();
	}

	/**
	 * Produces a list of key,value pairs for map tasks.
	 * 
	 * @param numMaps
	 *            - Number of map tasks.
	 * @return - List of key,value pairs.
	 */
	private static List<KeyValuePair> getKeyValuesForMap(int numMaps,
			String dataFilePrefix, String dataDir) {
		List<KeyValuePair> keyValues = new ArrayList<KeyValuePair>();
		IntKey key = null;
		StringValue value = null;
		for (int i = 0; i < numMaps; i++) {
			key = new IntKey(i);
			value = new StringValue(dataDir + "/" + dataFilePrefix + i
					+ DATA_FILE_SUFFIX);
			keyValues.add(new KeyValuePair(key, value));
		}
		return keyValues;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("MDS args.len:" + args.length);
		if (args.length != 5) {
			String errorReport = "MDSDataGen: The Correct arguments are \n"
					+ "MDSDataGen [ids file] [sub dir][data file prefix] [num splits=num maps] [num data points] ";
			System.out.println(errorReport);
			System.exit(0);
		}
		/**
		 * Total data points should be = n x numMaps x 64, where n can be any
		 * integer.
		 */
		String idsFile = args[0];
		int numMapTasks = Integer.parseInt(args[3]);
		long numDataPoints = Long.parseLong(args[4]);
		// Generate initial cluster centers sequentially.
		generateIDSFile(idsFile, numDataPoints);
		if (numDataPoints % numMapTasks != 0) {
			System.out
					.print("Number of data points are not equally divisable to map tasks ");
			System.exit(0);
		}
		MDSDataGen client;
		try {
			client = new MDSDataGen();
			// client.driveMapReduce(numMapTasks,
			// numDataPointsPerMap,dataFilePrefix, dataDir);
			client.driveMapReduce(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	public void driveMapReduce(String[] args) throws Exception {
		String dataDir = args[1];
		String dataFilePrefix = args[2];
		int numMapTasks = Integer.parseInt(args[3]);
		long numDataPoints = Long.parseLong(args[4]);
		long numDataPointsPerMap = numDataPoints / numMapTasks;

		int numReducers = 0; // We don't need any reducers.

		// JobConfigurations
		JobConf jobConf = new JobConf("mds-data-gen"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(MDSDataGenMapTask.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		jobConf.addProperty(NUM_DATA_PER_MAP,
				String.valueOf(numDataPointsPerMap));
		jobConf.addProperty(NUM_DATA, String.valueOf(numDataPoints));

		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps();
		TwisterMonitor monitor = driver.runMapReduce(getKeyValuesForMap(
				numMapTasks, dataFilePrefix, dataDir));
		monitor.monitorTillCompletion();
		driver.close();
	}

}
