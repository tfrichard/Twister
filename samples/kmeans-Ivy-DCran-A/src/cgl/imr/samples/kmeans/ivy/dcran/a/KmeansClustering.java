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

package cgl.imr.samples.kmeans.ivy.dcran.a;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
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
		if (args.length != 5) {
			String errorReport = "KMeansClustering: the Correct arguments are \n"
					+ "java cgl.imr.samples.kmeans....KmeansClustering "
					+ "[centroid file]  [partition file] [num map tasks] [num reduce tasks] [numLoop]";
			System.out.println(errorReport);
			System.exit(1);
		}
		String centroidFile = args[0];
		String partitionFile = args[1];
		int numMapTasks = Integer.parseInt(args[2]);
		int numReduceTasks = Integer.parseInt(args[3]);
		int numLoop = Integer.parseInt(args[4]);
		// count total execution time
		double beginTime = System.currentTimeMillis();
		try {
			KmeansClustering.driveClusteringMapReduce(centroidFile,
					partitionFile, numMapTasks, numReduceTasks, numLoop);
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
			String partitionFile, int numMapTasks,
			int numReduceTasks, int numLoop) throws Exception {
		// load centroids
		double CentroidsLoadInitTime = System.currentTimeMillis();
		Value centroids = loadCentroidsTextToBroadcastValue(centroidFile);
		double CentroidsLoadEndTime = System.currentTimeMillis();
		System.out.println("Centroids Loading time: "
				+ (CentroidsLoadEndTime - CentroidsLoadInitTime));
		if (centroids == null) {
			System.out.println("Centroids is null.");
			return;
		}
		// initialize centroids printing executor, and do a print try
		ExecutorService taskExecutor = Executors.newSingleThreadExecutor();
		// create a thread to write the centroid file
		CentroidsPrintingThread thread = new CentroidsPrintingThread(
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
		System.out.println("Driver  Initilization time: "
				+ (jobInitEndTime1 - jobInitStartTime));
		System.out.println("Map Configuration time: "
				+ (jobInitEndTime2 - jobInitEndTime1));
		// Main iteration for K-Means clustering
		for (int loopCount = 0; loopCount < numLoop; loopCount++) {
			double iterationStartTime = System.currentTimeMillis();
			// now we broadcast the single and only value object
			MemCacheAddress memCacheKey = driver.addToMemCache(centroids);
			TwisterMonitor monitor = driver.runMapReduceBCast(memCacheKey);
			monitor.monitorTillCompletion();
			driver.cleanMemCache();
			// combine centroids list
			List<Value> newCentroids = ((KMeansClusteringCombiner) driver
					.getCurrentCombiner()).getResults();
			if (newCentroids.size() != jobConf.getNumReduceTasks()) {
				System.out.println("NOT ALL CENTROIDS COME BACK!");
			}
			double totalDistance = 0;
			// calculate the total distance
			for (Value value : newCentroids) {
				totalDistance = totalDistance
						+ ((CombineVectorData) value).getTotalDistance();
			}
			centroids = mergeCentroidsList(newCentroids);
			// create a thread to write the centroid file
			thread = new CentroidsPrintingThread(centroids, loopCount + 1);
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

	private static Value loadCentroidsTextToBroadcastValue(String centroidFile)
			throws Exception {
		int numData = countTotalLines(centroidFile);
		if (numData == 0) {
			throw new Exception("Fail in counting total lines number");
		}
		int vecLen = countDimensionality(centroidFile);
		if (vecLen == 0) {
			throw new Exception("Fail in getting dimensionality");
		}
		System.out.println("numData: " + numData + " vecLen " + vecLen);
		Value bcastData = null;
		byte data[][] = null;
		BufferedReader bReader = null;
		String line = null;
		int tmpVecLen = 0;
		boolean exception = false;
		try {
			bReader = new BufferedReader(new FileReader(new File(centroidFile)));
			data = new byte[numData][vecLen];
			for (int i = 0; i < numData; i++) {
				line = bReader.readLine();
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
				for (int j = 0; j < tmpVecLen; j++) {
					data[i][j] = (byte) (Integer.parseInt(vectorValues[j
							+ lineOffset]) - valueOffset);
				}
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
			data = null;
			throw new Exception("Fail in loading data.");
		}
		if (data != null) {
			bcastData = new BcastCentroidVectorData(numData, vecLen, data);
		}
		return bcastData;
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

	private static Value mergeCentroidsList(List<Value> newCentroids) {
		int numData = 0;
		int vecLen = ((CombineVectorData) newCentroids.get(0)).getCentroid()
				.getVecLen();
		for (Value value : newCentroids) {
			numData = numData
					+ ((CombineVectorData) value).getCentroid().getNumData();
		}
		byte[][] data = new byte[numData][vecLen];
		byte[][] perData = null;
		int perNumData = 0;
		int startRow = 0;
		for (Value value : newCentroids) {
			perData = ((CombineVectorData) value).getCentroid().getData();
			perNumData = ((CombineVectorData) value).getCentroid().getNumData();
			for (int i = 0; i < perNumData; i++) {
				System.arraycopy(perData[i], 0, data[startRow], 0, vecLen);
				startRow++;
			}
		}
		return new BcastCentroidVectorData(numData, vecLen, data);
	}
}

/**
 * row col values are set arbitarily no meaning.
 * 
 * @author zhangbj
 * 
 */
class CentroidsPrintingThread implements Runnable {

	private BcastCentroidVectorData centroids;
	private int loopCount;

	CentroidsPrintingThread(Value centroids, int loopCount) {
		this.centroids = (BcastCentroidVectorData) centroids;
		this.loopCount = loopCount;
	}

	@Override
	public void run() {
		String fileName = "centroids_iteration_" + this.loopCount;
		PrintWriter writer = null;
		long centroidIndex = 0;
		try {
			writer = new PrintWriter(new BufferedOutputStream(
					new FileOutputStream(fileName)));
			// Print
			int numCentroids = centroids.getNumData();
			int vecLen = centroids.getVecLen();
			byte[][] cData = centroids.getData();
			// now the data structure is picID row col\tdim f1 f2 f3 f4
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
			writer.flush();
			writer.close();
		} catch (Exception e) {
			if (writer != null) {
				writer.close();
			}
			e.printStackTrace();
		}
	}
}