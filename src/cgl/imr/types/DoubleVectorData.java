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

package cgl.imr.types;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;

/**
 * Represents a vector comprised of double values.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class DoubleVectorData implements Value {

	private double data[][];
	private int numData;
	private int vecLen;

	public DoubleVectorData() {
	}

	public DoubleVectorData(double[][] data, int numData, int vecLen) {
		this.data = data;
		this.numData = numData;
		this.vecLen = vecLen;
	}

	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {

		this.numData = message.readInt();
		this.vecLen = message.readInt();

		this.data = new double[numData][vecLen];

		for (int i = 0; i < numData; i++) {
			for (int j = 0; j < vecLen; j++) {
				data[i][j] = message.readDouble();
			}
		}
	}

	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {

		message.writeInt(numData);
		message.writeInt(vecLen);
		for (int i = 0; i < numData; i++) {
			for (int j = 0; j < vecLen; j++) {
				message.writeDouble(data[i][j]);
			}
		}
	}
	
	public void setData(double[][] data, int numData, int vecLen) {
		this.data = data;
		this.numData = numData;
		this.vecLen = vecLen;
	}

	public double[][] getData() {
		return data;
	}

	public int getNumData() {
		return numData;
	}

	public int getVecLen() {
		return vecLen;
	}

	/**
	 * Loads data from a binary file. First two integer values gives the number
	 * of rows and the number of columns to read. The remaining double values
	 * contains the vector data.
	 */
	public double[][] loadDataFromBinFile(String fileName) throws IOException {
		File file = new File(fileName);
		BufferedInputStream bin = new BufferedInputStream(new FileInputStream(
				file));
		DataInputStream din = new DataInputStream(bin);

		numData = din.readInt();
		vecLen = din.readInt();

		if (!(numData > 0 && numData <= Integer.MAX_VALUE && vecLen > 0 && vecLen <= Integer.MAX_VALUE)) {
			throw new IOException("Invalid number of rows or columns.");
		}

		this.data = new double[numData][vecLen];
		for (int i = 0; i < numData; i++) {
			for (int j = 0; j < vecLen; j++) {
				data[i][j] = din.readDouble();
			}
		}
		din.close();
		bin.close();
		return this.data;
	}

	/**
	 * Loads data from a text file. Sample input text file is shown below. First
	 * line indicates the number of lines. Second line gives the length of the
	 * vector. 5 2 1.2 2.3 5.6 3.3 1.0 2.5 3.0 6.5 5.5 6.3
	 * 
	 */
	public double[][] loadDataFromTextFile(String fileName) throws IOException {

		File file = new File(fileName);
		
		// ZBJ: could limit the buffer size, but slow 
		BufferedReader reader = new BufferedReader(new FileReader(file));

		String inputLine = reader.readLine();
		if (inputLine != null) {
			numData = Integer.parseInt(inputLine);
		} else {
			new IOException("First line = number of rows is null");
		}

		inputLine = reader.readLine();
		if (inputLine != null) {
			vecLen = Integer.parseInt(inputLine);
		} else {
			new IOException("Second line = size of the vector is null");
		}

		this.data = new double[numData][vecLen];
		// ZBJ: Do gc in order to load large file
		// Runtime.getRuntime().gc();
		
		
		String[] vectorValues = null;
		int numRecords = 0;
		while ((inputLine = reader.readLine()) != null) {
			vectorValues = inputLine.split(" ");
			if (vecLen != vectorValues.length) {
				throw new IOException("Vector length did not match at line "
						+ numRecords);
			}
			for (int i = 0; i < vecLen; i++) {
				data[numRecords][i] = Double.valueOf(vectorValues[i]);
			}
			numRecords++;
		}

		// ZBJ: Do close
		reader.close();
		
		// ZBJ: Do gc in order to load large file
		// Runtime.getRuntime().gc();
		
		return this.data;
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

		// First two parameters are the dimensions.
		dout.writeInt(numData);
		dout.writeInt(vecLen);
		for (int i = 0; i < numData; i++) {
			for (int j = 0; j < vecLen; j++) {
				dout.writeDouble(data[i][j]);
			}
		}
		dout.flush();
		bout.flush();
		dout.close();
		bout.close();
	}

	/**
	 * Write the vector data into a text file. First two lines give numData and
	 * vecLen.
	 * 
	 * @param fileName
	 *            - Name of the file to write.
	 * @throws IOException
	 */
	public void writeToTextFile(String fileName) throws IOException {
		// Data as string
		BufferedOutputStream bout = new BufferedOutputStream(
				new FileOutputStream(fileName));
		PrintWriter writer = new PrintWriter(bout);
		writer.println(numData);
		writer.println(vecLen);
		StringBuffer line;
		for (int i = 0; i < numData; i++) {
			line = new StringBuffer();
			for (int j = 0; j < vecLen; j++) {
				if (j == (vecLen - 1)) {
					line.append(data[i][j]);
				} else {
					line.append(data[i][j] + " ");
				}
			}
			writer.println(line.toString());
		}
		writer.flush();
		writer.close();
		bout.flush();
		bout.close();
	}

	@Override
	public void mergeInShuffle(Value value) {
	}
}
