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

package cgl.imr.samples.kmeans.hydra.a;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.DoubleVectorData;
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

	/**
	 * Main program to run K-means clustering.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 6) {
			String errorReport = "KMeansClustering: the Correct arguments are \n"
					+ "java cgl.imr.samples.kmeans.KmeansClustering "
					+ "<centroid file>  <partition file> <num bcast keyvalue pairs> <num map tasks> <num reduce tasks> <numLoop>";
			System.out.println(errorReport);
			System.exit(0);
		}
		String centroidFile = args[0];
		String partitionFile = args[1];
		int numBcastValues = Integer.parseInt(args[2]);
		int numMapTasks = Integer.parseInt(args[3]);
		int numReduceTasks = Integer.parseInt(args[4]);
		int numLoop = Integer.parseInt(args[5]);
		List<Value> centroids = null;
		double beginTime = System.currentTimeMillis();
		try {
			centroids = KmeansClustering.driveMapReduce(centroidFile,
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

		double[][] cData;
		int numCentroids;
		int vecLen;
		DoubleVectorData centroid = null;
		for (Value value : centroids) {
			centroid = (DoubleVectorData) value;
			cData = centroid.getData();
			numCentroids = centroid.getNumData();
			vecLen = centroid.getVecLen();
			for (int i = 0; i < numCentroids; i++) {
				for (int j = 0; j < vecLen; j++) {
					System.out.print(cData[i][j] + " , ");
				}
				System.out.println();
			}
		}

		System.exit(0);
	}

	public static List<Value> driveMapReduce(String centroidFile,
			String partitionFile, int numBcastValues, int numMapTasks,
			int numReduceTasks, int numLoop) throws Exception {
		// JobConfigurations
		JobConf jobConf = new JobConf("kmeans-map-reduce"
				+ UUIDGenerator.getInstance().generateTimeBasedUUID());
		jobConf.setMapperClass(KMeansMapTask.class);
		jobConf.setReducerClass(KMeansReduceTask.class);
		jobConf.setCombinerClass(KMeansCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReduceTasks);
		jobConf.setFaultTolerance();
		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps(partitionFile);
		List<Value> centroids = loadCentroidFileToBroadcastValues(centroidFile,
				numBcastValues);
		int loopCount = 0;
		// Main iteration for K-Means clustering
		boolean complete = false;
		while (!complete) {
			MemCacheAddress memCacheKey = driver.addToMemCache(centroids);
			TwisterMonitor monitor = driver.runMapReduceBCast(memCacheKey);
			monitor.monitorTillCompletion();
			driver.cleanMemCache();
			centroids = ((KMeansCombiner) driver.getCurrentCombiner())
					.getResults();
			loopCount++;
			System.out.println("loop number " + loopCount);
			if (loopCount == numLoop) {
				break;
			}
		}
		driver.close();
		return centroids;
	}

	private static List<Value> loadCentroidFileToBroadcastValues(
			String centroidFile, int numBcastValues) throws IOException {

		int numData = 0;
		int vecLen = 0;
		List<Value> bcastValueList = new ArrayList<Value>();

		DataInputStream din = new DataInputStream(new BufferedInputStream(
				new FileInputStream(centroidFile)));

		numData = din.readInt();
		vecLen = din.readInt();

		if (numData == 0 || vecLen == 0) {
			System.out.println("no correct centroids data. ");
			System.exit(1);
		}

		int restData = numData % numBcastValues;
		int numPerSplit = numData / numBcastValues;
		int tmpNumPerSplit = numPerSplit;

		for (int i = 0; i < numBcastValues; i++) {
			if (restData > 0) {
				restData--;
				tmpNumPerSplit = numPerSplit + 1;
			} else {
				tmpNumPerSplit = numPerSplit;
			}

			double data[][] = new double[tmpNumPerSplit][vecLen];

			for (int j = 0; j < tmpNumPerSplit; j++) {
				for (int k = 0; k < vecLen; k++) {
					data[j][k] = din.readDouble();
				}
			}

			bcastValueList.add(new DoubleVectorData(data, tmpNumPerSplit,
					vecLen));
		}
		din.close();
		return bcastValueList;
	}

	@SuppressWarnings("unused")
	private double getError(DoubleVectorData cData, DoubleVectorData newCData) {
		double totalError = 0;
		int numCentroids = cData.getNumData();

		double[][] centroids = cData.getData();
		double[][] newCentroids = newCData.getData();

		for (int i = 0; i < numCentroids; i++) {
			totalError += getEuclidean(centroids[i], newCentroids[i],
					cData.getVecLen());
		}
		return totalError;
	}

	/**
	 * Calculates the square value of the Euclidean distance. Although K-means
	 * clustering typically uses Euclidean distance, the use of its square value
	 * does not change the algorithm or the final results. Calculation of square
	 * root is costly. square value
	 * 
	 * @param v1
	 *            - First vector.
	 * @param v2
	 *            - Second vector.
	 * @param vecLen
	 *            - Length of the vectors.
	 * @return - Square of the Euclidean distances.
	 */
	private double getEuclidean(double[] v1, double[] v2, int vecLen) {
		double sum = 0;
		for (int i = 0; i < vecLen; i++) {
			sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
		}
		return sum;
	}
}
