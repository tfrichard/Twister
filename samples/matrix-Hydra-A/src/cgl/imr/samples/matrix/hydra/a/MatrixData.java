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

package cgl.imr.samples.matrix.hydra.a;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;

/**
 * Matrix Size: Height * Width | * -
 * 
 * @author zhangbj
 *
 */
public class MatrixData implements Value {

	private int col = -1; // Column (or column block index
	private double[][] data;
	private int height;
	private int row = -1; // Row (or row bloc) index
	private int width;

	public MatrixData() {
	}

	public MatrixData(double[][] data, int height, int width) {
		this.data = data;
		this.height = height;
		this.width = width;
	}

	public MatrixData(double[][] data, int height, int width, int row, int col) {
		this.data = data;
		this.height = height;
		this.width = width;
		this.row = row;
		this.col = col;
	}

	@Override
	public void fromTwisterMessage(TwisterMessage msg)
			throws SerializationException {
		height = msg.readInt();
		width = msg.readInt();
		row = msg.readInt();
		col = msg.readInt();
		this.data = new double[height][width];
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				data[i][j] = msg.readDouble();
			}
		}
	}

	@Override
	public void toTwisterMessage(TwisterMessage msg)
			throws SerializationException {
		msg.writeInt(height);
		msg.writeInt(width);
		msg.writeInt(row);
		msg.writeInt(col);
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				msg.writeDouble(data[i][j]);
			}
		}
	}

	public void setCol(int col) {
		this.col = col;
	}

	public int getCol() {
		return col;
	}

	public double[][] getData() {
		return data;
	}

	public void setRow(int row) {
		this.row = row;
	}

	public int getRow() {
		return row;
	}

	public int getHeight() {
		return height;
	}

	public int getWidth() {
		return width;
	}

	/**
	 * Write the vector data into a binary file.
	 * 
	 * @param fileName
	 * @throws IOException
	 */
	public void writeToBinFile(String fileName) throws IOException {
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(fileName)));
		// First two parameters are the dimensions.
		dout.writeInt(height);
		dout.writeInt(width);
		dout.writeInt(row);
		dout.writeInt(col);
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				dout.writeDouble(data[i][j]);
			}
		}
		dout.flush();
		dout.close();
	}

	/**
	 * Loads data from a binary file. First four integer values gives the number
	 * of rows and the number of columns to read and the row and column block
	 * numbers. The remaining double values contains the vector data.
	 */
	public double[][] loadDataFromBinFile(String fileName) throws IOException {
		File file = new File(fileName);
		DataInputStream din = new DataInputStream(new BufferedInputStream(
				new FileInputStream(file)));
		height = din.readInt();
		width = din.readInt();
		row = din.readInt();
		col = din.readInt();
		// you read data as a int, how could you get out of the range of the integer
		/*
		 * if (!(height > 0 && height <= Integer.MAX_VALUE && width > 0 && width
		 * <= Integer.MAX_VALUE)) { throw new
		 * IOException("Invalid number of rows or columns."); }
		 */
		this.data = new double[height][width];
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				data[i][j] = din.readDouble();
			}
		}
		din.close();
		return this.data;
	}

	public void print() {
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				System.out.print(data[i][j] + " ");
			}
			System.out.println();
		}
	}

	@Override
	public void mergeInShuffle(Value arg0) {
		// TODO Auto-generated method stub
	}
}
