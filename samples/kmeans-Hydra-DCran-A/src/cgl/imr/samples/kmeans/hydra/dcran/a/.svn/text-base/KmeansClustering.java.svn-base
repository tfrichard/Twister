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

package cgl.imr.samples.kmeans.hydra.dcran.a;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.types.MemCacheAddress;

/**
 * Implements K-means clustering algorithm using MapReduce programming model.
 * <p>
 * <code>
 * K-means Clustering Algorithm for MapReduce
 * 	Do
 * 	Broadcast Cn 
 * 	[Perform in parallel] the map() operation
 * 	for each Vi
 * 		for each Cn,j
 * 	Dij <= Euclidian (Vi,Cn,j)
 * 	Assign point Vi to Cn,j with minimum Dij		
 * 	for each Cn,j
 * 		Cn,j <=Cn,j/K
 * 	
 * 	[Perform Sequentially] the reduce() operation
 * 	Collect all Cn
 * 	Calculate new cluster centers Cn+1
 * 	Diff<= Euclidian (Cn, Cn+1)
 * 	while (Diff <THRESHOLD)
 * </code>
 * <p>
 * The MapReduce algorithm we used is shown below. (Assume that the input is
 * already partitioned and available in the compute nodes). In this algorithm,
 * Vi refers to the ith vector, Cn,j refers to the jth cluster center in nth
 * iteration, Dij refers to the Euclidian distance between ith vector and jth
 * cluster center, and K is the number of cluster centers.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * @author Bingjing Zhang
 */
public class KmeansClustering {
	// the start of point values in each line
	// now the data structure is picID row col dim f1 f2 f3 f4 ... fdim
	// there is a strange space between col and dim, so they can not be split by
	// " "
	static int lineOffset = 3;
	static int valueOffset = 128;

	/**
	 * Main program to run K-means clustering.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 6) {
			String errorReport = "KMeansClustering: the Correct arguments are \n"
					+ "java cgl.imr.samples.kmeans....KmeansClustering "
					+ "[centroid file]  [partition file] [num bcast keyvalue pairs] [num map tasks] [num reduce tasks] [numLoop]";
			System.out.println(errorReport);
			System.exit(1);
		}
		String centroidFile = args[0];
		String partitionFile = args[1];
		int numBcastValues = Integer.parseInt(args[2]);
		int numMapTasks = Integer.parseInt(args[3]);
		int numReduceTasks = Integer.parseInt(args[4]);
		int numLoop = Integer.parseInt(args[5]);
		// count total execution time
		double beginTime = System.currentTimeMillis();
		try {
			KmeansClustering.driveClusteringMapReduce(centroidFile,
					partitionFile, numBcastValues, numMapTasks, numReduceTasks,
					numLoop);
		} catch (Exception e) {
			e.printStackTrace();
		}
		double endTime = System.currentTimeMillis();
		// Print the test statistics
		double timeInSeconds = ((double) (endTime - beginTime)) / 1000;
		System.out
				.println("------------------------------------------------------");
		System.out.println("Kmeans clustering took " + timeInSeconds
				+ " seconds.");
		System.out
				.println("------------------------------------------------------");
		System.exit(0);
	}

	public static void driveClusteringMapReduce(String centroidFile,
			String partitionFile, int numBcastValues, int numMapTasks,
			int numReduceTasks, int numLoop) throws Exception {
		// load centroids
		double CentroidsLoadInitTime = System.currentTimeMillis();
		List<Value> centroids = loadCentroidsTextToBroadcastValues(
				centroidFile, numBcastValues);
		double CentroidsLoadEndTime = System.currentTimeMillis();
		System.out.println("Centroids Loading time: "
				+ (CentroidsLoadEndTime - CentroidsLoadInitTime)
				+ "  Centroids List Size " + centroids.size());
		// initialize centroids printing executor, and do a print try
		ExecutorService taskExecutor = Executors.newSingleThreadExecutor();
		// create a thread to write the centroid file
		HandleCentroidsPrintingThread thread = new HandleCentroidsPrintingThread(
				centroids, 0);
		taskExecutor.execute(thread);
		// JobConfigurations
		JobConf jobConf = new JobConf("kmeans-map-reduce"
				+ UUIDGenerator.getInstance().generateTimeBasedUUID());
		jobConf.setMapperClass(KMeansClusteringMapTask.class);
		jobConf.setReducerClass(KMeansClusteringReduceTask.class);
		jobConf.setCombinerClass(KMeansClusteringCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReduceTasks);
		jobConf.setFaultTolerance();
		// driver initialization
		double jobInitStartTime = System.currentTimeMillis();
		TwisterDriver driver = new TwisterDriver(jobConf);
		double jobInitEndTime1 = System.currentTimeMillis();
		driver.configureMaps(partitionFile);
		double jobInitEndTime2 = System.currentTimeMillis();
		System.out.println("Total Job Initilization time: "
				+ (jobInitEndTime2 - jobInitStartTime));
		System.out.println("Driver  Initilization time: "
				+ (jobInitEndTime1 - jobInitStartTime));
		System.out.println("Map Configuration time: "
				+ (jobInitEndTime2 - jobInitEndTime1));
		// Main iteration for K-Means clustering
		for (int loopCount = 0; loopCount < numLoop; loopCount++) {
			double iterationStartTime = System.currentTimeMillis();
			// addresses are set based on the order in the centroids list
			MemCacheAddress memCacheKey = driver.addToMemCache(centroids);
			TwisterMonitor monitor = driver.runMapReduceBCast(memCacheKey);
			monitor.monitorTillCompletion();
			driver.cleanMemCache();
			if (((KMeansClusteringCombiner) driver.getCurrentCombiner())
					.getResults().size() != centroids.size()) {
				System.out.println("NOT ALL CENTROIDS COME BACK!");
			}
			// clean the centroids
			centroids.clear();
			double totalDistance = 0;
			for (Value value : ((KMeansClusteringCombiner) driver
					.getCurrentCombiner()).getResults()) {
				// get new centroids
				centroids.add(((CombineVectorData) value).getCentroid());
				// calculate the total distance
				totalDistance = totalDistance
						+ ((CombineVectorData) value).getTotalDistance();
			}
			// create a thread to write the centroid file
			thread = new HandleCentroidsPrintingThread(centroids, loopCount + 1);
			taskExecutor.execute(thread);
			// count time
			double iterationEndTime = System.currentTimeMillis();
			System.out.println("loop number: " + loopCount
					+ " Iteration Time: "
					+ (iterationEndTime - iterationStartTime)
					+ " Total Distance: " + totalDistance);
		}
		driver.close();
		// job 2
		double job2StartTime = System.currentTimeMillis();
		// another job for writing done the centroid assignment
		JobConf jobConf2 = new JobConf("kmeans-point-assn-map"
				+ UUIDGenerator.getInstance().generateTimeBasedUUID());
		jobConf2.setMapperClass(KMeansPointsAssnMapTask.class);
		jobConf2.setNumMapTasks(numMapTasks);
		jobConf2.addProperty("output_dir", TwisterConfigurations.getInstance()
				.getLocalDataDir() + "/output");
		jobConf2.setFaultTolerance();
		TwisterDriver driver2 = new TwisterDriver(jobConf2);
		driver2.configureMaps(partitionFile);
		MemCacheAddress memCacheKey = driver2.addToMemCache(centroids);
		TwisterMonitor monitor2 = driver2.runMapReduceBCast(memCacheKey);
		monitor2.monitorTillCompletion();
		driver2.cleanMemCache();
		driver2.close();
		double job2EndTime = System.currentTimeMillis();
		System.out.println("Total Job2 (Point Assignment) time: "
				+ (job2EndTime - job2StartTime));
		// shutdown the printing executor
		taskExecutor.shutdown(); // Disable new tasks from being submitted
		try {
			if (!taskExecutor.awaitTermination(1200, TimeUnit.SECONDS)) {
				System.out.println("It still prints after 20 minutes...");
				taskExecutor.shutdownNow();
				// Cancel currently executing // tasks
				// Wait a while for tasks to respond to being cancelled
				if (!taskExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
					System.err.println("Printing did not terminate");
				}
			}
		} catch (InterruptedException e) {
			// (Re-)Cancel if current thread also interrupted
			taskExecutor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

	private static List<Value> loadCentroidsTextToBroadcastValues(
			String centroidFile, int numBcastValues) throws Exception {
		int numData = countTotalLines(centroidFile);
		if (numData == 0) {
			throw new Exception("Fail in counting total lines number");
		}
		int vecLen = countDimensionality(centroidFile);
		if (vecLen == 0) {
			throw new Exception("Fail in getting dimensionality");
		}
		System.out.println("numData: " + numData + " " + "vecLen " + vecLen);
		List<Value> bcastValueList = new ArrayList<Value>();
		int numPerSplit = numData / numBcastValues;
		int restData = numData % numBcastValues;
		int tmpNumPerSplit = numPerSplit;
		int tmpVecLen = vecLen;
		BufferedReader bReader = null;
		boolean exception = false;
		try {
			bReader = new BufferedReader(new FileReader(new File(centroidFile)));
			// create centroids list!
			for (int i = 0; i < numBcastValues; i++) {
				if (restData > 0) {
					restData--;
					tmpNumPerSplit = numPerSplit + 1;
				} else {
					tmpNumPerSplit = numPerSplit;
				}
				byte data[][] = new byte[tmpNumPerSplit][vecLen];
				// we create centroids list based on the reading order
				for (int j = 0; j < tmpNumPerSplit; j++) {
					String line = bReader.readLine();
					String[] vectorValues = line.split(" ");
					// now the data structure is picID row col\tdim f1 f2 f3 f4
					// ... fdim
					if (vectorValues.length < (vecLen + lineOffset)) {
						tmpVecLen = vectorValues.length - lineOffset;
						System.out.println("Potential incorrect line: "
								+ vectorValues.length);
					} else {
						tmpVecLen = vecLen;
					}
					for (int k = 0; k < tmpVecLen; k++) {
						data[j][k] = (byte) (Integer.parseInt(vectorValues[k
								+ lineOffset]) - valueOffset);
					}
				}
				// read and add according to the order in the file
				bcastValueList.add(new BcastCentroidVectorData(tmpNumPerSplit,
						vecLen, data));
			}
			bReader.close();
		} catch (Exception e) {
			exception = true;
		}
		if (exception) {
			if (bReader != null) {
				try {
					bReader.close();
				} catch (Exception e) {
				}
			}
			throw new Exception("Fail in loading data.");
		}
		return bcastValueList;
	}

	static int countTotalLines(String dataFile) {
		int numData = 0;
		LineNumberReader lReader = null;
		String line = null;
		boolean exception = false;
		try {
			lReader = new LineNumberReader(new FileReader(new File(dataFile)));
			do {
				line = lReader.readLine();
			} while (line != null);
			numData = lReader.getLineNumber();
			lReader.close();
		} catch (Exception e) {
			e.printStackTrace();
			exception = true;
		}
		// handle exception
		if (exception) {
			if (lReader != null) {
				try {
					lReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			numData = 0;
		}
		return numData;
	}

	static int countDimensionality(String dataFile) {
		int dim = 0;
		LineNumberReader lReader = null;
		boolean exception = false;
		try {
			lReader = new LineNumberReader(new FileReader(new File(dataFile)));
			String line = lReader.readLine();
			String[] splits = line.split("\t");
			String[] numbers = splits[1].split(" ");
			dim = Integer.parseInt(numbers[0]);
			lReader.close();
		} catch (Exception e) {
			e.printStackTrace();
			exception = true;
		}
		// handle exception
		if (exception) {
			if (lReader != null) {
				try {
					lReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			dim = 0;
		}
		return dim;
	}
}

/**
 * row col values are set arbitarily no meaning.
 * 
 * @author zhangbj
 * 
 */
class HandleCentroidsPrintingThread implements Runnable {

	private List<Value> centroids;
	private int loopCount;

	HandleCentroidsPrintingThread(List<Value> centroids, int loopCount) {
		this.centroids = centroids;
		this.loopCount = loopCount;
	}

	@Override
	public void run() {
		String fileName = "centroids_iteration_" + this.loopCount;
		PrintWriter writer = null;
		boolean exception = false;
		long centroidIndex = 0;
		try {
			writer = new PrintWriter(new BufferedOutputStream(
					new FileOutputStream(fileName)));
			// Print
			for (Value value : centroids) {
				BcastCentroidVectorData centroid = (BcastCentroidVectorData) value;
				int numCentroids = centroid.getNumData();
				int vecLen = centroid.getVecLen();
				byte[][] cData = centroid.getData();
				// now the data structure is picID row col dim f1 f2 f3 f4
				// ...
				// fdim
				for (int i = 0; i < numCentroids; i++) {
					writer.print(centroidIndex + " ");
					writer.print(0 + " "); // row number, no meaning
					writer.print(0 + "\t"); // col number, no meaning, with \t
											// as original format
					writer.print(vecLen + " "); // dim size
					for (int j = 0; j < vecLen - 1; j++) {
						writer.print((cData[i][j] + KmeansClustering.valueOffset)
								+ " ");
					}
					writer.println(cData[i][vecLen - 1]
							+ KmeansClustering.valueOffset);
					centroidIndex++;
				}
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
			exception = true;
		}
		if (exception) {
			if (writer != null) {
				writer.close();
			}
		}
	}
}
