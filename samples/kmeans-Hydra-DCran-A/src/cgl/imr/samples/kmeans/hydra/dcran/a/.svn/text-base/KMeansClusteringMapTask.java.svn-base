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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
		int centroidsSize = 0;
		MemCacheAddress memCacheKey = (MemCacheAddress) val;
		List<BcastCentroidVectorData> centroids = new ArrayList<BcastCentroidVectorData>();
		int index = memCacheKey.getStart();
		for (int i = 0; i < memCacheKey.getRange(); i++) {
			BcastCentroidVectorData data = (BcastCentroidVectorData) (MemCache
					.getInstance().get(this.jobConf.getJobId(),
					memCacheKey.getMemCacheKeyBase() + index));
			if (data != null) {
				centroids.add(data);
				centroidsSize = centroidsSize + data.getNumData();
			}
			index++;
		}
		if (centroids.size() != memCacheKey.getRange()) {
			throw new TwisterException(
					"Fail in getting all the centroids. Centroids Size:  "
							+ centroids.size() + " " + memCacheKey.getRange());
		}
		// image data
		byte[][] imageData = this.image.getData();
		int numImageData = this.image.getNumData();
		int vecLen = this.image.getVecLen();
		// index recording
		double[] minDis = new double[numImageData];
		int[][] minCentroidBlockRow = new int[numImageData][2];
		// temp values
		double dis = 0;
		if (centroidsSize > numImageData) {
			for (int i = 0; i < centroids.size(); i++) {
				BcastCentroidVectorData centroid = centroids.get(i);
				byte[][] centroidData = centroid.getData();
				int numCentroidData = centroid.getNumData();
				// for each row
				for (int j = 0; j < numCentroidData; j++) {
					for (int k = 0; k < numImageData; k++) {
						dis = getEuclidean(imageData[k], centroidData[j],
								vecLen);
						// set init minDis for a new coming data point
						if (i == 0 && j == 0) {
							minDis[k] = dis;
							minCentroidBlockRow[k][0] = i;
							minCentroidBlockRow[k][1] = j;
						}
						if (dis < minDis[k]) {
							minDis[k] = dis;
							minCentroidBlockRow[k][0] = i;
							minCentroidBlockRow[k][1] = j;
						}
					}
				}
			}
		} else {
			for (int i = 0; i < numImageData; i++) {
				// for each centroids block
				for (int j = 0; j < centroids.size(); j++) {
					BcastCentroidVectorData centroid = centroids.get(j);
					byte[][] centroidData = centroid.getData();
					int numCentroidData = centroid.getNumData();
					// for each row
					for (int k = 0; k < numCentroidData; k++) {
						dis = getEuclidean(imageData[i], centroidData[k],
								vecLen);
						// set init minDis for a new coming data point
						if (j == 0 && k == 0) {
							minDis[i] = dis;
							minCentroidBlockRow[i][0] = j;
							minCentroidBlockRow[i][1] = k;
						}
						if (dis < minDis[i]) {
							minDis[i] = dis;
							minCentroidBlockRow[i][0] = j;
							minCentroidBlockRow[i][1] = k;
						}
					}
				}
			}
		}
		/*
		 * centroid block index : centroid row index : image data index list.
		 * this about 65000*4 + centroid row number (1M)*4 = 260 KB. we keep the
		 * order
		 */
		Map<Integer, Map<Integer, List<Integer>>> centroidsMap = new TreeMap<Integer, Map<Integer, List<Integer>>>();
		// record the assignment of the points
		for (int i = 0; i < numImageData; i++) {
			// block
			Map<Integer, List<Integer>> block = centroidsMap
					.get(minCentroidBlockRow[i][0]);
			if (block == null) {
				block = new HashMap<Integer, List<Integer>>();
				centroidsMap.put(minCentroidBlockRow[i][0], block);
			}
			// row
			List<Integer> row = block.get(minCentroidBlockRow[i][1]);
			if (row == null) {
				row = new ArrayList<Integer>();
				block.put(minCentroidBlockRow[i][1], row);
			}
			// add image index
			row.add(i);
		}
		// do collect
		Map<Key, Value> keyvals = new HashMap<Key, Value>();
		for (int blockIndex : centroidsMap.keySet()) {
			// create new object
			int[] newCentroidRowCount = new int[centroids.get(blockIndex)
					.getNumData()];
			int[][] newCentroidData = new int[centroids.get(blockIndex)
					.getNumData()][centroids.get(blockIndex).getVecLen()];
			double totalMinDis = 0;
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
			keyvals.put(new ShuffleKey(blockIndex),
					new ShuffleVectorData(centroids.get(blockIndex)
							.getNumData(), centroids.get(blockIndex)
							.getVecLen(), totalMinDis, newCentroidRowCount,
							newCentroidData));
			if (keyvals.size() == 25) {
				collector.collect(keyvals);
				keyvals.clear();
			}
			/*
			collector.collect(new ShuffleKey(blockIndex),
					new ShuffleVectorData(centroids.get(blockIndex)
							.getNumData(), centroids.get(blockIndex)
							.getVecLen(), totalMinDis, newCentroidRowCount,
							newCentroidData));
			*/
		}
		if (!keyvals.isEmpty()) {
			collector.collect(keyvals);
		}
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
}
