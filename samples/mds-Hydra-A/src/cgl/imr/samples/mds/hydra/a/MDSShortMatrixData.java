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

package cgl.imr.samples.mds.hydra.a;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;

public class MDSShortMatrixData implements Value {

	short[][] data;
	int height;
	int width;
	int row = -1; // Row (or row bloc) index
	int rowOffset = -1; // row offset

	public MDSShortMatrixData() {
	}

	public MDSShortMatrixData(short[][] data, int height, int width) {
		this.data = data;
		this.height = height;
		this.width = width;
	}

	public MDSShortMatrixData(short[][] data, int height, int width, int row,
			int rowOffset) {
		this.data = data;
		this.height = height;
		this.width = width;
		this.row = row;
		this.rowOffset = rowOffset;
	}
	
	public void fromBytes(byte[] bytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			height = din.readInt();
			width = din.readInt();
			row = din.readInt();
			rowOffset = din.readInt();

			this.data = new short[height][width];
			for (int i = 0; i < height; i++) {
				for (int j = 0; j < width; j++) {
					data[i][j] = din.readShort();
				}
			}
			din.close();
			baInputStream.close();

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}

	}

	public byte[] getBytes() throws SerializationException {
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();

		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] marshalledBytes = null;

		try {

			dout.writeInt(height);
			dout.writeInt(width);
			dout.writeInt(row);
			dout.writeInt(rowOffset);
			for (int i = 0; i < height; i++) {
				for (int j = 0; j < width; j++) {
					dout.writeShort(data[i][j]);
				}
			}
			dout.flush();
			marshalledBytes = baOutputStream.toByteArray();
			baOutputStream = null;
			dout = null;
		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		return marshalledBytes;
	}

	public int getRowOffset() {
		return rowOffset;
	}

	public short[][] getData() {
		return data;
	}

	public int getHeight() {
		return height;
	}

	public int getRow() {
		return row;
	}

	public int getWidth() {
		return width;
	}

	/**
	 * Loads data from a binary file. First four integer values gives the number
	 * of rows and the number of columns to read and the row and column block
	 * numbers. The remaining double values contains the vector data.
	 */
	public short[][] loadDataFromBinFile(String fileName) throws IOException {
		File file = new File(fileName);
		BufferedInputStream bin = new BufferedInputStream(new FileInputStream(
				file));
		DataInputStream din = new DataInputStream(bin);

		//height = din.readInt();
		//width = din.readInt();
		//row = din.readInt();
		//rowOffset = din.readInt();

		if (!(height > 0 && height <= Integer.MAX_VALUE && width > 0 && width <= Integer.MAX_VALUE)) {
			throw new IOException("Invalid number of rows or columns.");
		}

		this.data = new short[height][width];
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				data[i][j] = din.readShort();
			}
		}
		din.close();
		bin.close();
		return this.data;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	public void setWidth(int width) {
		this.width = width;
	}

	public void setRowOFfset(int rowOffset) {
		this.rowOffset = rowOffset;
	}

	public void setRow(int row) {
		this.row = row;
	}

	/**
	 * Write the vector data into a binary file.
	 * 
	 * @param fileName
	 * @throws IOException
	 */
	public void writeToBinFile(String fileName) throws IOException {
		BufferedOutputStream bout = new BufferedOutputStream(
				new FileOutputStream(fileName));
		DataOutputStream dout = new DataOutputStream(bout);
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				dout.writeShort(data[i][j]);
			}
		}
		dout.flush();
		bout.flush();
		dout.close();
		bout.close();
	}

	@Override
	public void fromTwisterMessage(TwisterMessage arg0)
			throws SerializationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void toTwisterMessage(TwisterMessage arg0)
			throws SerializationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void mergeInShuffle(Value arg0) {
		// TODO Auto-generated method stub
		
	}
}
