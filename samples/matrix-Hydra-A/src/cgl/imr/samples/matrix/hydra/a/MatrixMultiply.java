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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.Key;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.Value;
import cgl.imr.base.impl.GenericCombiner;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.MemCacheAddress;

/**
 * MapReduce program performing Matrix multiplication. The algorithm used is as
 * follows.
 * 
 * Let the matrices be A x B = C
 * 
 * Main Program 1. Partition matrix B in to column blocks (number of column
 * blocks = number of map tasks) 2. Partition matrix A in to row blocks (number
 * of row blocks = number of iterations, typically should be decided by the size
 * of the memory requirements) 3. Configure map tasks with the column blocks of
 * B 4. foreach row block 5. Run MapReduce by sending a row block to all the map
 * tasks. (In iteration i send the ith row block) 6. Append the resulting row
 * block to the output matrix C. 7. end for
 * 
 * Map Task 1. Multiply the assigned column block with the current row block. 2.
 * Collect the resulting block of the output matrix.
 * 
 * Reduce Task 1. Collect all the matrix blocks for and put them in their
 * correct order to form a row block of the final output matrix. 2. Collect this
 * row block.
 * 
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */
public class MatrixMultiply {

	private static UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	private static void appendRowBlockToMatrix(double[][] data,
			MatrixData rowBlock, int start) {
		int width = rowBlock.getWidth();
		int end = rowBlock.getHeight() + start;
		double[][] rowData = rowBlock.getData();
		int count = 0;
		for (int i = start; i < end; i++) {
			for (int j = 0; j < width; j++) {
				data[i][j] = rowData[count][j];
			}
			count++;
		}
	}

	public static void main(String[] args) {
		String module = "ParallelMatMult.main() ->";
		if (args.length != 5) {
			String errorReport = module
					+ "The Correct arguments for the square matrix multiplication \n"
					+ "[data file A] - binary data file - should be read as Double \n"
					+ "[data file B] - binary data file - should be read as Double \n"
					+ "[output file] - this is the output file] \n"
					+ "[num map tasks] - the number of map tasks will determine the size of the partial data prodcuts] \n"
					+ "[num iterations] - the number of iterations in which the entire matrix operation should be performed.";
			System.out.println(errorReport);
			System.exit(0);
		}
		String dataFileA = args[0];
		String dataFileB = args[1];
		String outputFile = args[2];
		int numMapTasks = Integer.parseInt(args[3]);
		int numIterations = Integer.parseInt(args[4]);
		try {
			matrixMultiplyMapReduce(dataFileA, dataFileB, outputFile,
					numMapTasks, numIterations);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		System.exit(0);
	}

	/**
	 * Perform Matrix multiplication operation using MapReduce technique.
	 * 
	 * @param matAFileName
	 *            - File name of Matrix A.
	 * @param matBFileName
	 *            - File name of Matrix B.
	 * @param outFileName
	 *            - File name to store the ouput matrix.
	 * @param numMaps
	 *            - Number of map tasks.
	 * @param numIterations
	 *            - Number of iterations to use.
	 * @throws IOException
	 */
	private static void matrixMultiplyMapReduce(String matAFileName,
			String matBFileName, String outFileName, int numMaps,
			int numIterations) throws TwisterException, IOException {
		MatrixData matA = new MatrixData();
		MatrixData matB = new MatrixData();
		matA.loadDataFromBinFile(matAFileName);
		matB.loadDataFromBinFile(matBFileName);
		int matAHeight = matA.getHeight();
		int matBWidth = matB.getWidth();
		if (matA.getWidth() != matB.getHeight()) {
			System.err
					.println("Invalid dimensions in matrix. MatrixA.width needs to be equal to MatrixB.height.");
			System.exit(-1);
		}
		double beginTime = System.currentTimeMillis();
		int numReducers = 1;
		// JobConfigurations
		JobConf jobConf = new JobConf("fully-in-mem-mat-mult"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(MatrixMultiplyMapTask.class);
		jobConf.setReducerClass(MatrixMultiplyReduceTask.class);
		jobConf.setCombinerClass(GenericCombiner.class);
		jobConf.setNumMapTasks(numMaps);
		jobConf.setNumReduceTasks(numReducers);
		jobConf.addProperty("final_width", String.valueOf(matBWidth));
		jobConf.setFaultTolerance();
		// Split matB in to column blocks.
		MatrixData[] columns = splitMatrixColumnWise(matB, numMaps);
		// matB = null; // We don't need it anymore.
		MatrixData[] rows = splitMatrixRowWise(matA, numIterations);
		for (MatrixData mdata : rows) {
			System.out.println("rowBlock dimension: " + mdata.getHeight()
					+ " * " + mdata.getWidth() + " row/column "
					+ mdata.getRow() + "/" + mdata.getCol());
		}
		for (MatrixData mdata : columns) {
			System.out.println("columnsBlock dimension: " + mdata.getHeight()
					+ " * " + mdata.getWidth() + " row/column "
					+ mdata.getRow() + "/" + mdata.getCol());
		}
		// matA = null; // We don't need it anymore.
		double[][] outMat = new double[matAHeight][matBWidth];
		MatrixData outRow;
		int outMatrixStartRow = 0;
		double midTime = System.currentTimeMillis();
		System.out.println("Time to split data =" + (midTime - beginTime)
				/ 1000);
		beginTime = System.currentTimeMillis();
		TwisterModel driver = null;
		TwisterMonitor monitor = null;
		try {
			driver = new TwisterDriver(jobConf);
			// columns data are added to a list
			List<Value> columnsList = new ArrayList<Value>();
			for (MatrixData mdata : columns) {
				columnsList.add(mdata);
			}
			driver.configureMaps(columnsList);
			midTime = System.currentTimeMillis();
			System.out.println("Time to send data to maps ="
					+ (midTime - beginTime) / 1000);
			beginTime = System.currentTimeMillis();
			MemCacheAddress memCacheKey;
			for (int i = 0; i < numIterations; i++) {
				memCacheKey = driver.addToMemCache(rows[i]);
				monitor = driver.runMapReduceBCast(memCacheKey);
				monitor.monitorTillCompletion();
				driver.cleanMemCache();
				GenericCombiner combiner = (GenericCombiner) driver
						.getCurrentCombiner();
				if (!combiner.getResults().isEmpty()) {
					Key key = combiner.getResults().keySet().iterator().next();
					outRow = (MatrixData) combiner.getResults().get(key);
					appendRowBlockToMatrix(outMat, outRow, outMatrixStartRow);
					outMatrixStartRow += outRow.getHeight();
				}
				System.out.println("Iteration " + i);
			}
		} catch (TwisterException e) {
			driver.close();
			throw e;
		}
		double endTime = System.currentTimeMillis();
		System.out
				.println("------------------------------------------------------");
		System.out.println("Matrix multiplication took "
				+ (endTime - beginTime) / 1000 + " seconds.");
		System.out
				.println("------------------------------------------------------");
		// verify
		System.out.println("Verify the result: " + verify(matA, matB, outMat)
				+ ".");
		// Print 3x3 block. Just for clarification.
		// printFirstNRowsOfMatrix(outMat, 3, matBWidth);
		System.out.println("Writing result to disk...");
		(new MatrixData(outMat, matAHeight, matBWidth))
				.writeToBinFile(outFileName);
		endTime = System.currentTimeMillis();
		System.out
				.println("------------------------------------------------------");
		System.out.println("Result writing took " + (endTime - beginTime)
				/ 1000 + " seconds.");
		System.out
				.println("------------------------------------------------------");
		driver.close();
	}

	/**
	 * Splits a given matrix into a set of column blocks.
	 * 
	 * @param mat
	 * @param numMaps
	 * @return
	 */
	private static MatrixData[] splitMatrixColumnWise(MatrixData mat,
			int numMaps) {
		int width = mat.getWidth();
		int colWidth = width / numMaps;
		int rem = width % numMaps;
		MatrixData[] columns = new MatrixData[numMaps];
		double[][] data = mat.getData();
		double[][] column;
		int start = 0;
		int end = 0;
		int curWidth = 0;
		int count = 0;
		for (int i = 0; i < numMaps; i++) {
			end += colWidth;
			if (rem > 0) {
				end++;
				rem--;
			}
			curWidth = end - start;
			column = new double[mat.getHeight()][curWidth];
			count = 0;
			for (int j = start; j < end; j++) {
				column[i][count] = data[i][j];
				count++;
			}
			columns[i] = new MatrixData(data, mat.getHeight(), curWidth);
			columns[i].setCol(i);
			start = end;
		}
		return columns;
	}

	/**
	 * Splits a given matrix into a set of row blocks.
	 * 
	 * @param mat
	 * @param numIterations
	 * @return
	 */
	private static MatrixData[] splitMatrixRowWise(MatrixData mat,
			int numIterations) {
		int height = mat.getHeight();
		int rowHeight = (int) Math.floor(height / numIterations);
		int rem = height % numIterations;

		MatrixData[] rows = new MatrixData[numIterations];
		double[][] data = mat.getData();
		double[][] row;
		int start = 0;
		int end = 0;
		int curHeight = 0;
		int count = 0;
		for (int i = 0; i < numIterations; i++) {
			end += rowHeight;
			if (rem > 0) {
				end++;
				rem--;
			}
			curHeight = end - start;
			row = new double[curHeight][mat.getWidth()];
			count = 0;
			for (int j = start; j < end; j++) {
				row[count][i] = data[j][i];
				count++;
			}
			rows[i] = new MatrixData(data, curHeight, mat.getWidth());
			rows[i].setRow(i);
			start = end;
		}
		return rows;
	}

	private static boolean verify(MatrixData matA, MatrixData matB, double[][] C) {
		boolean verified = true;
		double[][] outMat = MatrixUtils.matrixMultiplySeq(matA.getData(),
				matB.getData(), matA.getHeight(), matB.getWidth(),
				matA.getWidth());
		for (int i = 0; i < matA.getHeight(); i++) {
			for (int j = 0; j < matB.getHeight(); j++) {
				if (outMat[i][j] != C[i][j]) {
					verified = false;
				}
			}
		}
		return verified;
	}
}