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

import cgl.imr.samples.matrix.hydra.a.MatrixException;

/**
 * 
 * @author Jaliya Ekanayake, jekanaya@cs.indiana.edu Some code segments contain
 *         in this class are directly inherited from the C# version of the
 *         shared memory MDS program written by my colleague Seung-Hee Bea.
 * 
 *         A collection of matrix related utility operations.
 * 
 */
public class MatrixUtils {

	// addition a double value to a matrix.
	public static double[][] add(double[][] m1, double s)
			throws MatrixException {
		int m1Len = m1.length;
		if (m1Len == 0) {
			throw new MatrixException("Incompatible Matrices");
		} else if (m1[0].length == 0) {
			throw new MatrixException("Incompatible Matrices");
		}
		int vecLen = m1[0].length;
		double[][] res = new double[m1Len][vecLen];
		for (int i = 0; i < m1Len; i++) {
			for (int j = 0; j < vecLen; j++) {
				res[i][j] = m1[i][j] + s;
			}
		}
		return res;
	}

	public static double[][] add(double[][] m1, double[][] m2)
			throws MatrixException {
		int m1Len = m1.length;
		int m2Len = m2.length;
		if (m1Len == 0 || m2Len == 0 || m1Len != m2Len) {
			throw new MatrixException("Incompatible Matrices");
		} else if (m1[0].length != m2[0].length || m1[0].length == 0
				|| m2[0].length == 0) {
			throw new MatrixException("Incompatible Matrices");
		}
		int vecLen = m1[0].length;
		double[][] res = new double[m1Len][vecLen];
		for (int i = 0; i < m1Len; i++) {
			for (int j = 0; j < vecLen; j++) {
				res[i][j] = m1[i][j] + m2[i][j];
			}
		}
		return res;
	}

	public static double[][] copy(double[][] m1) throws MatrixException {
		int m1Len = m1.length;
		if (m1Len == 0) {
			throw new MatrixException("Incompatible Matrices");
		} else if (m1[0].length == 0) {
			throw new MatrixException("Incompatible Matrices");
		}
		int vecLen = m1[0].length;
		double[][] res = new double[m1Len][vecLen];
		for (int i = 0; i < m1Len; i++) {
			for (int j = 0; j < vecLen; j++) {
				res[i][j] = m1[i][j];
			}
		}
		return res;
	}

	/**
	 * Block matrix multiplication.
	 * 
	 * @param A
	 *            - Matrix
	 * @param B
	 *            - Matrix
	 * @param aHeight
	 *            - height of matrix A
	 * @param bWidth
	 *            - width of matrix B
	 * @param comm
	 *            - size of the common face
	 * @return
	 */
	public static double[][] matrixMultiplyBlock(double[][] A, double[][] B,
			int aHeight, int bWidth, int comm, int bz) {
		double begin = System.currentTimeMillis();
		double[][] C = new double[aHeight][bWidth];
		int aHeightBlocks = aHeight / bz; // size = Height of A
		int aLastBlockHeight = aHeight - (aHeightBlocks * bz);
		if (aLastBlockHeight > 0) {
			aHeightBlocks++;
		}
		int bWidthBlocks = bWidth / bz; // size = Width of B
		int bLastBlockWidth = bWidth - (bWidthBlocks * bz);
		if (bLastBlockWidth > 0) {
			bWidthBlocks++;
		}
		int commnBlocks = comm / bz; // size = Width of A or Height of B
		int commLastBlockWidth = comm - (commnBlocks * bz);
		if (commLastBlockWidth > 0) {
			commnBlocks++;
		}
		int aBlockHeight = bz;
		int bBlockWidth = bz;
		int commBlockWidth = bz;
		int ib_bz;
		int kb_bz;
		int jb_bz;
		double end = System.currentTimeMillis();
		for (int ib = 0; ib < aHeightBlocks; ib++) {
			if (aLastBlockHeight > 0 && ib == (aHeightBlocks - 1)) {
				aBlockHeight = aLastBlockHeight;
			}
			bBlockWidth = bz;
			commBlockWidth = bz;
			ib_bz = ib * bz;
			for (int jb = 0; jb < bWidthBlocks; jb++) {
				if (bLastBlockWidth > 0 && jb == (bWidthBlocks - 1)) {
					bBlockWidth = bLastBlockWidth;
				}
				commBlockWidth = bz;
				jb_bz = jb * bz;
				for (int kb = 0; kb < commnBlocks; kb++) {
					if (commLastBlockWidth > 0 && kb == (commnBlocks - 1)) {
						commBlockWidth = commLastBlockWidth;
					}
					kb_bz = ib * bz;
					System.out.println(bz + "  " + ib_bz + " " + jb_bz + "  "
							+ kb_bz);
					begin = System.currentTimeMillis();
					for (int i = ib_bz; i < (ib_bz) + aBlockHeight; i++) {
						for (int k = kb_bz; k < (kb_bz) + commBlockWidth; k++) {
							for (int j = jb_bz; j < (jb_bz) + bBlockWidth; j++) {
								C[i][j] += A[i][k] * B[k][j];
							}
						}
					}
					end = System.currentTimeMillis();
				}
			}
		}
		System.out.println(aHeight + " " + bWidth + " " + comm + " " + bz
				+ " time=" + (end - begin) / 1000 + " seconds");
		return C;
	}

	/**
	 * 
	 * @param A
	 * @param B
	 * @param aHeight
	 * @param bWidth
	 * @param comm
	 * @param bz
	 * @return
	 */
	public static double[][] matrixMultiplySeq(double[][] A, double[][] B,
			int aHeight, int bWidth, int comm) {
		double[][] C = new double[aHeight][bWidth];
		for (int i = 0; i < aHeight; i++) {
			for (int k = 0; k < comm; k++) {
				for (int j = 0; j < bWidth; j++) {
					C[i][j] += A[i][k] * B[k][j];
				}
			}
		}
		return C;
	}

	public static double[][] mult(double[][] m1, double[][] m2)
			throws MatrixException {
		int m1Len = m1.length;
		int m2Len = m2.length;
		if (m1Len == 0 || m2Len == 0) {
			throw new MatrixException("Incompatible Matrices");
		} else if (m1[0].length == 0 || m2[0].length == 0) {
			throw new MatrixException("Incompatible Matrices");
		} else if (m1[0].length != m2.length) {
			throw new MatrixException(
					"Incompatible Matrices for multiplication");
		}
		int m1Width = m1[0].length;
		int m2Width = m2[0].length;
		double[][] res = new double[m1Len][m2Width];
		double val = 0;
		for (int i = 0; i < m1Len; i++) {
			for (int j = 0; j < m2Width; j++) {
				val = 0;
				for (int k = 0; k < m1Width; k++) {
					val += m1[i][k] * m2[k][j];
				}
				res[i][j] = val;
			}
		}
		return res;
	}

	// multiply a double value to a matrix.
	public static double[][] mult(double[][] m1, double s)
			throws MatrixException {
		int m1Len = m1.length;
		if (m1Len == 0) {
			throw new MatrixException("Incompatible Matrices");
		} else if (m1[0].length == 0) {
			throw new MatrixException("Incompatible Matrices");
		}
		int vecLen = m1[0].length;
		double[][] res = new double[m1Len][vecLen];

		for (int i = 0; i < m1Len; i++) {
			for (int j = 0; j < vecLen; j++) {
				res[i][j] = m1[i][j] * s;
			}
		}
		return res;
	}

	// addition a double value to a matrix.
	public static double[][] sub(double[][] m1, double s)
			throws MatrixException {
		int m1Len = m1.length;
		if (m1Len == 0) {
			throw new MatrixException("Incompatible Matrices");
		} else if (m1[0].length == 0) {
			throw new MatrixException("Incompatible Matrices");
		}
		int vecLen = m1[0].length;
		double[][] res = new double[m1Len][vecLen];
		for (int i = 0; i < m1Len; i++) {
			for (int j = 0; j < vecLen; j++) {
				res[i][j] = m1[i][j] - s;
			}
		}
		return res;
	}

	public static double[][] sub(double[][] m1, double[][] m2)
			throws MatrixException {
		int m1Len = m1.length;
		int m2Len = m2.length;
		if (m1Len == 0 || m2Len == 0 || m1Len != m2Len) {
			throw new MatrixException("Incompatible Matrices");
		} else if (m1[0].length != m2[0].length || m1[0].length == 0
				|| m2[0].length == 0) {
			throw new MatrixException("Incompatible Matrices");
		}
		int vecLen = m1[0].length;
		double[][] res = new double[m1Len][vecLen];
		for (int i = 0; i < m1Len; i++) {
			for (int j = 0; j < vecLen; j++) {
				res[i][j] = m1[i][j] - m2[i][j];
			}
		}
		return res;
	}
}
