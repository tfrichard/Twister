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

import java.util.List;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;

/**
 * Calculates the new centroids using the partial centroids.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * @author Bingjing Zhang
 * 
 */
public class KMeansClusteringReduceTask implements ReduceTask {

	public void close() throws TwisterException {
	}

	public void configure(JobConf jobConf, ReducerConf reducerConf)
			throws TwisterException {
	}

	public void reduce(ReduceOutputCollector collector, Key key,
			List<Value> values) throws TwisterException {
		if (values.size() <= 0) {
			throw new TwisterException("Reduce input error no values.");
		}
		// get num and len
		ShuffleVectorData firstCentroid = (ShuffleVectorData) (values.get(0));
		int numData = firstCentroid.getNumData();
		int lenData = firstCentroid.getVecLen();
		// intermediate data objects
		long[] totalCount = new long[numData];
		long[][] totalSum = new long[numData][lenData];
		double totalDis = 0;
		// add all together
		for (int i = 0; i < values.size(); i++) {
			ShuffleVectorData centroid = (ShuffleVectorData) (values.get(i));
			int[][] centroidData = centroid.getData();
			for (int j = 0; j < numData; j++) {
				for (int k = 0; k < lenData; k++) {
					totalSum[j][k] = totalSum[j][k] + centroidData[j][k];
				}
				totalCount[j] = totalCount[j] + centroid.getRowCount()[j];
			}
			totalDis = totalDis + centroid.getTotalMinDis();
		}
		byte[][] newCentroids = new byte[numData][lenData];
		// divided by the count
		for (int i = 0; i < numData; i++) {
			for (int j = 0; j < lenData; j++) {
				if (totalCount[i] != 0) {
					newCentroids[i][j] = (byte) ((totalSum[i][j]) / (long) totalCount[i]);
				}
			}
		}
		collector.collect(key, new CombineVectorData(
				new BcastCentroidVectorData(numData, lenData, newCentroids),
				totalDis));
	}
}
