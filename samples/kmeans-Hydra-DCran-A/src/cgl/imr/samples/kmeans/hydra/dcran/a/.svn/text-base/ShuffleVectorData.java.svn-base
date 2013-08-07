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

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;

/**
 * Intermediate centroids Vector Data
 * 
 */
public class ShuffleVectorData implements Value {
	private int numData;
	private int vecLen;
	private double totalMinDis;
	private int rowCount[];
	private int data[][];

	public ShuffleVectorData() {
		this.numData = 0;
		this.vecLen = 0;
		this.totalMinDis = 0;
		this.rowCount = null;
		this.data = null;
	}

	public ShuffleVectorData(int numData, int vecLen, double totalMinDis,
			int rowCount[], int[][] data) {
		this.numData = numData;
		this.vecLen = vecLen;
		this.totalMinDis = totalMinDis;
		this.rowCount = rowCount;
		this.data = data;
	}

	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		this.numData = message.readInt();
		this.vecLen = message.readInt();
		this.totalMinDis = message.readDouble();
		if (this.numData > 0 && this.vecLen > 0) {
			this.rowCount = new int[this.numData];
			for (int i = 0; i < this.numData; i++) {
				this.rowCount[i] = message.readInt();
			}
			this.data = new int[this.numData][this.vecLen];
			for (int i = 0; i < this.numData; i++) {
				for (int j = 0; j < this.vecLen; j++) {
					this.data[i][j] = message.readInt();
				}
			}
		} else {
			this.rowCount = null;
			this.data = null;
		}
	}

	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		message.writeInt(this.numData);
		message.writeInt(this.vecLen);
		message.writeDouble(this.totalMinDis);
		if (this.numData > 0 && this.vecLen > 0) {
			for (int i = 0; i < this.numData; i++) {
				message.writeInt(this.rowCount[i]);
			}
			for (int i = 0; i < this.numData; i++) {
				for (int j = 0; j < this.vecLen; j++) {
					message.writeInt(this.data[i][j]);
				}
			}
		}
	}

	@Override
	public void mergeInShuffle(Value val) {
		if (val.getClass().getName().equals(this.getClass().getName())) {
			// long start = System.currentTimeMillis();
			// merge data
			ShuffleVectorData newData = (ShuffleVectorData) val;
			int[][] newdata = newData.getData();
			// long end0 = System.currentTimeMillis();
			// boolean flag = false;
			// long lastPause2 = 0;
			// long pause2 = 0;
			for (int i = 0; i < this.numData; i++) {
				// long pause1 = System.currentTimeMillis();
				for (int j = 0; j < this.vecLen; j++) {
					this.data[i][j] = this.data[i][j] + newdata[i][j];
				}
				// lastPause2 = pause2;
				// pause2 = System.currentTimeMillis();
				/*
				 * if ((pause2 - pause1) > 5) {
				 * System.out.println("Merge Operation " + i + " takes " +
				 * (pause2 - pause1)); }
				 */
				/*
				 * if ((pause2 - end0) > 100 && !flag) {
				 * System.out.println("Merge Operation from start to " + i +
				 * " takes " + (pause2 - end0) + " " + (lastPause2 - end0));
				 * flag = true; }
				 */
			}
			// long end1 = System.currentTimeMillis();
			// merge row count
			for (int i = 0; i < this.numData; i++) {
				this.rowCount[i] = this.rowCount[i] + newData.getRowCount()[i];
			}
			// long end2 = System.currentTimeMillis();
			// merge min distance
			this.totalMinDis = this.getTotalMinDis() + newData.getTotalMinDis();
			// long end3 = System.currentTimeMillis();
			/*
			 * if ((end3 - start) > 100) {
			 * System.out.println("Merge Operation takes " + (end3 - start) +
			 * " " + (end2 - start) + " " + (end1 - start) + " " + +(end0 -
			 * start) + " " + this.numData + " " + this.vecLen); }
			 */
		} else {
			System.out.println("Merge in shuffle is not allowed.");
		}
	}

	public int getNumData() {
		return numData;
	}

	public int getVecLen() {
		return vecLen;
	}

	public double getTotalMinDis() {
		return this.totalMinDis;
	}

	public int[] getRowCount() {
		return this.rowCount;
	}

	public int[][] getData() {
		return this.data;
	}
}
