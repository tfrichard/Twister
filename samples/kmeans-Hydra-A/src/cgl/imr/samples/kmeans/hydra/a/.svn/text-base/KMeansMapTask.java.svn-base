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

import java.util.ArrayList;
import java.util.List;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.data.file.FileData;
import cgl.imr.types.DoubleVectorData;
import cgl.imr.types.MemCacheAddress;
import cgl.imr.worker.MemCache;

/**
 * Map task for the K-Means clustering.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * @author Bingjing Zhang
 * 
 */
public class KMeansMapTask implements MapTask {

	private DoubleVectorData vectorData;
	private JobConf jobConf;

	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}

	/**
	 * Loads the vector data from a file. Since the map tasks are cached across
	 * iterations, we only need to load this data once for all the iterations.
	 */
	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException {
		this.jobConf = jobConf;
		this.vectorData = new DoubleVectorData();
		FileData fileData = (FileData) mapConf.getDataPartition();
		try {
			vectorData.loadDataFromBinFile(fileData.getFileName());
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}

	public double getEuclidean2(double[] v1, double[] v2, int vecLen) {
		double sum = 0;
		for (int i = 0; i < vecLen; i++) {
			sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
		}
		return sum; // No need to use the sqrt.
	}

	/**
	 * Map function for the K-means clustering. Calculates the Euclidean
	 * distance between the data points and the given cluster centers. Next it
	 * calculates the partial cluster centers as well.
	 */
	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
		//long start1Time = System.currentTimeMillis();	
		MemCacheAddress memCacheKey = (MemCacheAddress) val;
		List<DoubleVectorData> centroids = new ArrayList<DoubleVectorData>();
		List<NewDoubleVectorData> newCentroids = new ArrayList<NewDoubleVectorData>();

		int index = memCacheKey.getStart();
		for (int i = 0; i < memCacheKey.getRange(); i++) {
			DoubleVectorData data = (DoubleVectorData) (MemCache.getInstance()
					.get(jobConf.getJobId(), memCacheKey.getMemCacheKeyBase()
							+ index));
			if (data != null) {
				centroids.add(data);
			}
			index++;
		}
		// long start2Time = System.currentTimeMillis();
		// System.out.println("centroid size: " + centroids.size());
		// all arrays are zero based
		for (DoubleVectorData centroid : centroids) {
			newCentroids.add(new NewDoubleVectorData(new double[centroid
					.getNumData()][centroid.getVecLen() + 1], centroid
					.getNumData(), centroid.getVecLen() + 1));
		}

		double[][] data = vectorData.getData();
		int numData = vectorData.getNumData();
		int vecLen = vectorData.getVecLen();

		// temp values
		double[][] centroidData;
		int numCentroidData = 0;
		double minDis = 0;
		double dis = 0;
		int minCentroidBlock = 0;
		int minCentroidRow = 0;

		//long start3Time = System.currentTimeMillis();
		for (int i = 0; i < numData; i++) {
			// for each block
			for (int j = 0; j < centroids.size(); j++) {
				DoubleVectorData centroid = centroids.get(j);
				centroidData = centroid.getData();
				numCentroidData = centroid.getNumData();
				// for each row
				for (int k = 0; k < numCentroidData; k++) {
					dis = getEuclidean2(data[i], centroidData[k], vecLen);

					if (j == 0 && k == 0) {
						minDis = dis;
					}

					if (dis < minDis) {
						minDis = dis;
						minCentroidBlock = j;
						minCentroidRow = k;
					}
				}
			}

			for (int j = 0; j < vecLen; j++) {
				newCentroids.get(minCentroidBlock).getData()[minCentroidRow][j] += data[i][j];
			}
			newCentroids.get(minCentroidBlock).getData()[minCentroidRow][vecLen] += 1;
		}

		//long start4Time = System.currentTimeMillis();
		/*
		 * additional location carries the number of partial points to a
		 * particular centroid.
		 */
		for (int i = 0; i < newCentroids.size(); i++) {
			collector.collect(new NewIntKey(i), newCentroids.get(i));
		}
		// long endTime = System.currentTimeMillis();
		/*
		 * if ((endTime - start4Time) > 1000 || (start2Time - start1Time) > 1000
		 * || (start3Time - start2Time) > 10000) {
		 * System.out.println("This task takes " + (endTime - start1Time) + " "
		 * + (endTime - start2Time) + " " + (endTime - start3Time) + " " +
		 * (endTime - start4Time)); }
		 */
	}
}
