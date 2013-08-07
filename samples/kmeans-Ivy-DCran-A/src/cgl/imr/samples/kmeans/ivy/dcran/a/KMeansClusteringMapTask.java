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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.data.file.FileData;
import cgl.imr.types.MemCacheAddress;
import cgl.imr.worker.MemCache;

/**
 * Map task for the K-Means clustering.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * @author Bingjing Zhang
 * 
 */
public class KMeansClusteringMapTask implements MapTask {

	private StaticImageVectorData image;
	private JobConf jobConf;

	public void close() throws TwisterException {
	}

	/**
	 * Loads the vector data from a file. Since the map tasks are cached across
	 * iterations, we only need to load this data once for all the iterations.
	 */
	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException {
		this.jobConf = jobConf;
		FileData fileData = (FileData) mapConf.getDataPartition();
		this.image = new StaticImageVectorData();
		try {
			this.image.loadDataFromTextFile(fileData.getFileName());
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}

	/**
	 * Map function for the K-means clustering. Calculates the Euclidean
	 * distance between the data points and the given cluster centers. Next it
	 * calculates the partial cluster centers as well.
	 */
	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
		// load centroids data from memcache
		MemCacheAddress memCacheKey = (MemCacheAddress) val;
		if (memCacheKey.getRange() != 1) {
			throw new TwisterException("MemCache size is not 1.");
		}
		BcastCentroidVectorData centroids = (BcastCentroidVectorData) (MemCache
				.getInstance().get(this.jobConf.getJobId(),
				memCacheKey.getMemCacheKeyBase() + memCacheKey.getStart()));
		if (centroids == null) {
			throw new TwisterException("No centroid data.");
		}
		if (centroids.getVecLen() != this.image.getVecLen()) {
			throw new TwisterException(
					"Centroids and Image data are not matched.");
		}
		// ExecutorService taskExecutor = Executors.newSingleThreadExecutor();
		// CentroidsPrintingThread thread = new CentroidsPrintingThread(centroids,
				// 0);
		// taskExecutor.execute(thread);
		int vecLen = this.image.getVecLen();
		// image data
		byte[][] imageData = this.image.getData();
		int numImageData = this.image.getNumData();
		// index recording
		double[] minDis = new double[numImageData];
		int[] minCentroidIndex = new int[numImageData];
		// centroid data
		int numCentroidsData = centroids.getNumData();
		byte[][] centroidData = centroids.getData();
		// tmp data
		double dis = 0;
		if (numCentroidsData > numImageData) {
			// for each row
			for (int i = 0; i < numCentroidsData; i++) {
				for (int j = 0; j < numImageData; j++) {
					dis = getEuclidean(imageData[j], centroidData[i], vecLen);
					// set init minDis for a new coming data point
					// we know i is 0, no need to set min block and row
					if (i == 0) {
						minDis[j] = dis;
					}
					if (dis < minDis[j]) {
						minDis[j] = dis;
						minCentroidIndex[j] = i;
					}
				}
			}
		} else {
			for (int i = 0; i < numImageData; i++) {
				for (int j = 0; j < numCentroidsData; j++) {
					dis = getEuclidean(imageData[i], centroidData[j], vecLen);
					// set init minDis for a new coming data point
					if (j == 0) {
						minDis[i] = dis;
					}
					if (dis < minDis[i]) {
						minDis[i] = dis;
						minCentroidIndex[i] = j;
					}
				}
			}
		}

		// set block and row map
		Map<Integer, Map<Integer, List<Integer>>> centroidsMap = new TreeMap<Integer, Map<Integer, List<Integer>>>();
		// record the assignment of the points
		for (int i = 0; i < numImageData; i++) {
			// System.out.println(" " + minCentroidIndex[i] + " " + numCentroidsData);
			int[]blockRowIndex = getBlockRowIndex(minCentroidIndex[i], numCentroidsData);
			// System.out.println(" " + blockRowIndex[0] + " " + blockRowIndex[1]);
			// block
			Map<Integer, List<Integer>> block = centroidsMap
					.get(blockRowIndex[0]);
			if (block == null) {
				block = new HashMap<Integer, List<Integer>>();
				centroidsMap.put(blockRowIndex[0], block);
			}
			// row
			List<Integer> row = block.get(blockRowIndex[1]);
			if (row == null) {
				row = new ArrayList<Integer>();
				block.put(blockRowIndex[1], row);
			}
			// add image index
			row.add(i);
		}
		//System.out.println("size of centroids map " + centroidsMap.size());
		// do collect
		int blockNumData = 0;
		double totalMinDis = 0;
		Map<Key, Value> keyvals = new HashMap<Key, Value>();
		for (int blockIndex : centroidsMap.keySet()) {
			blockNumData = getBlockNumData(blockIndex, numCentroidsData);
			// create new object
			int[] newCentroidRowCount = new int[blockNumData];
			int[][] newCentroidData = new int[blockNumData][vecLen];
			// get block records
			Map<Integer, List<Integer>> block = centroidsMap.get(blockIndex);
			// get row
			for (int rowIndex : block.keySet()) {
				List<Integer> row = block.get(rowIndex);
				// for each image index
				for (int imageIndex : row) {
					for (int i = 0; i < vecLen; i++) {
						newCentroidData[rowIndex][i] = newCentroidData[rowIndex][i]
								+ (int) imageData[imageIndex][i];
					}
					// image count on this centroid
					newCentroidRowCount[rowIndex] = newCentroidRowCount[rowIndex] + 1;
					totalMinDis = totalMinDis + minDis[imageIndex];
				}
			}
			keyvals.put(new ShuffleKey(blockIndex), new ShuffleVectorData(
					blockNumData, vecLen, totalMinDis, newCentroidRowCount,
					newCentroidData));

			if (keyvals.size() == 25) {
				collector.collect(keyvals);
				keyvals.clear();
			}
		}
		//add empty block, for benchmark shuffling only
		/*
		for (int i = 0; i < this.jobConf.getNumReduceTasks(); i++) {
			if (centroidsMap.get(i) == null) {
				blockNumData = getBlockNumData(i, numCentroidsData);
				int[] newCentroidRowCount = new int[blockNumData];
				int[][] newCentroidData = new int[blockNumData][vecLen];
				keyvals.put(new ShuffleKey(i), new ShuffleVectorData(
						blockNumData, vecLen, 0, newCentroidRowCount,
						newCentroidData));
			}
		}
		*/
		
		// collect
		if (!keyvals.isEmpty()) {
			collector.collect(keyvals);
		}
	}

	private int[] getBlockRowIndex(int centroidIndex, int numCentroidsData) {
		// chunk splitting
		int numSplit = this.jobConf.getNumReduceTasks();
		int dataPerBlock = numCentroidsData / numSplit;
		int rest = numCentroidsData % numSplit;
		int splitPoint = (dataPerBlock + 1) * rest;
		int blockIndex = 0;
		int rowIndex = 0;
		if (centroidIndex < splitPoint) {
			blockIndex = centroidIndex / (dataPerBlock + 1);
			rowIndex = centroidIndex % (dataPerBlock + 1);
		} else {
			blockIndex = rest + (centroidIndex - splitPoint) / dataPerBlock;
			rowIndex = (centroidIndex - splitPoint) % dataPerBlock;
		}
		int index[] = { blockIndex, rowIndex };
		return index;
	}

	private int getBlockNumData(int blockIndex, int numCentroidsData) {
		int numSplit = this.jobConf.getNumReduceTasks();
		int dataPerBlock = numCentroidsData / numSplit;
		int rest = numCentroidsData % numSplit;
		if (blockIndex < rest) {
			return dataPerBlock + 1;
		} else {
			return dataPerBlock;
		}
	}
	
	static private int[] getBlockRowIndexAlter(int centroidIndex, int numCentroidsData) {
		// chunk splitting
		int numSplit = 125;
		int dataPerBlock = numCentroidsData / numSplit;
		int rest = numCentroidsData % numSplit;
		int splitPoint = (dataPerBlock + 1) * rest;
		int blockIndex = 0;
		int rowIndex = 0;
		if (centroidIndex < splitPoint) {
			blockIndex = centroidIndex / (dataPerBlock + 1);
			rowIndex = centroidIndex % (dataPerBlock + 1);
		} else {
			blockIndex = rest + (centroidIndex - splitPoint) / dataPerBlock;
			rowIndex = (centroidIndex - splitPoint) % dataPerBlock;
		}
		int index[] = { blockIndex, rowIndex };
		return index;
	}

	/**
	 * Though for the real value, each byte should add
	 * KmeansClustering.valueOffset (128),
	 * 
	 * but two items get cancelled in the distance calculation
	 * 
	 * @param v1
	 * @param v2
	 * @param vecLen
	 * @return
	 */
	public double getEuclidean(byte[] v1, byte[] v2, int vecLen) {
		int sum = 0;
		for (int i = 0; i < vecLen; i++) {
			int diff = (int) v1[i] - (int) v2[i];
			sum = sum + diff * diff;
		}
		return Math.sqrt((double) sum); // No need to use the sqrt.
	}
	
	public static void main(String args[]) {
		int a[] =KMeansClusteringMapTask.getBlockRowIndexAlter(4, 5);
		System.out.println(" " + a[0] + " " + a[1]);
	}
}
