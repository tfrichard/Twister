package cgl.imr.samples.mds.hydra.a;

import java.io.BufferedReader;
import java.io.FileReader;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.data.file.FileData;
import cgl.imr.types.MemCacheAddress;
import cgl.imr.types.StringKey;
import cgl.imr.worker.MemCache;

/**
 * @author Thilina Gunarathne tgunarat@cs.indiana.edu Reduce the memory
 *         footprint of this map task. Make it use shorts to store the matrix
 *         and convert to double only at the time of the computation. Also got
 *         rid of 2 large matrix declarations.
 * @author Yang Ruan yangruan@cs.indiana.edu Fixed the bug for data over 50k.
 * @author Jaliya Ekanayake, jekanaya@cs.indiana.edu Some code segments contain
 *         in this class are directly inherited from the C# version of the
 *         shared memory MDS program wirtten by my collegue Seung-Hee Bea.
 * 
 *         This class performs the partial calculation of the following matrrix
 *         multiplication. BC= BofZ * preX. Please see ParallelMDSMapReduce.java
 *         for a complete description of the terms and the algorithm.
 * 
 */

public class CalcBCShortMapTask implements MapTask {

	private int bz = 0; // this is to hold the block size of the block matrix
	private int N = 0;

	MDSShortMatrixData deltaMatData;
	int blockOffset = 0;
	int blockHeight = 0;

	private short[][] deltaBlock = null;
	// private double[][] deltaBlock = null;

	private JobConf jobConf;

	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
		MemCacheAddress memCacheKey = (MemCacheAddress) val;
		MDSMatrixData mData = (MDSMatrixData) (MemCache.getInstance().get(
				jobConf.getJobId(), memCacheKey.getMemCacheKeyBase()
						+ memCacheKey.getStart()));
		double[][] preX = mData.getData();
		double[][] BofZ = calculateBofZ(preX);
		// Calculate the BofZ * preX.
		double[][] C = MatrixUtils.matrixMultiply(BofZ, preX, blockHeight,
				preX[0].length, N, bz);
		for (int i = 0; i < C.length; i++)
			for (int j = 0; j < C[i].length; j++)
				C[i][j] = C[i][j] / N;
		// Send C with the map task number to a reduce task. Which will simply
		// combine
		// these parts and form the N x d matrix.
		MDSMatrixData newMData = new MDSMatrixData(C, blockHeight, C[0].length,
				mData.getRow(), blockOffset);
		collector.collect(new StringKey("BC-calc-to-reduce-key"), newMData);
	}

	/**
	 * Calculation of partial BofZ matrix block.
	 * 
	 * @param distMatBlock
	 * @return
	 */
	private double[][] calculateBofZ(double[][] preX) {
		double[][] BofZ = new double[blockHeight][N];
		double[] rowSums = new double[blockHeight]; // This will store the
													// summation
		// of each row value of BofZ.
		int end = blockOffset + blockHeight;

		int tmpI = 0;
		for (int i = blockOffset; i < end; i++) {
			tmpI = i - blockOffset;
			for (int j = 0; j < N; j++) {
				/*
				 * B_ij = - w_ij * delta_ij / d_ij(Z), if (d_ij(Z) != 0) 0,
				 * otherwise v_ij = - w_ij.
				 * 
				 * Therefore, B_ij = v_ij * delta_ij / d_ij(Z). 0 (if d_ij(Z) >=
				 * small threshold) --> the actual meaning is (if d_ij(Z) == 0)
				 * BofZ[i][j] = V[i][j] * deltaMat[i][j] / CalculateDistance(ref
				 * preX, i, j);
				 */
				// this is for the i!=j case. For i==j case will be calculated
				// separately.

				if (i != j) {
					double vBlockValue = (double) -1;
					double distance = calculateDistance(preX, preX[0].length,
							i, j);

					if (distance >= 1.0E-10) {
						BofZ[tmpI][j] = vBlockValue
								* ((double) deltaBlock[tmpI][j] / (double) Short.MAX_VALUE)
								/ distance;
					} else {
						BofZ[tmpI][j] = 0;
					}
					rowSums[tmpI] += BofZ[tmpI][j];
				}
			}
		}

		// for i == j case
		for (int i = blockOffset; i < blockOffset + blockHeight; i++) {
			BofZ[i - blockOffset][i] = (-1.0) * rowSums[i - blockOffset]; // This
			// will made sum_i (B_ij) = 0 for all j.
		}

		return BofZ;
	}

	/**
	 * Simple distance calculation
	 * 
	 * @param origMat
	 *            - original matrix
	 * @param vecLength
	 *            - width of the matrix.
	 * @param i
	 *            , j means the positions for the distance calculation.
	 * @return
	 */
	private static double calculateDistance(double[][] origMat, int vecLength,
			int i, int j) {
		/*
		 * i and j is the index of first dimension, actually the index of each
		 * points. the length of the second dimension is the length of the
		 * vector.
		 */
		double dist = 0;
		for (int k = 0; k < vecLength; k++) {
			double diff = origMat[i][k] - origMat[j][k];
			dist += diff * diff;
		}

		dist = Math.sqrt(dist);
		return dist;
	}

	/**
	 * During this configuration step, the map task will load two matrix blocks.
	 * 1. Block from the delta matrix 2. Block from V matrix. The data files
	 * contains the matrix block data in binary form.
	 */
	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException {
		String idxFile = jobConf.getProperty("IdxFile");
		deltaMatData = new MDSShortMatrixData();
		FileData fileData = (FileData) mapConf.getDataPartition();
		System.out.println(mapConf.getMapTaskNo() + " "
				+ fileData.getFileName());
		try {
			int thisNo = Integer.parseInt(fileData.getFileName().substring(
					fileData.getFileName().lastIndexOf("_") + 1,
					fileData.getFileName().length()));
			BufferedReader br = new BufferedReader(new FileReader(idxFile));
			String line;
			String[] tokens;
			// Read information from idx file,
			// find the height, width, row id and row offset for current mapper
			while ((line = br.readLine()) != null) {
				tokens = line.split("\t");
				if (Integer.parseInt(tokens[0]) == thisNo) {
					deltaMatData.setHeight(Integer.parseInt(tokens[1]));
					deltaMatData.setWidth(Integer.parseInt(tokens[2]));
					deltaMatData.setRow(Integer.parseInt(tokens[3]));
					deltaMatData.setRowOFfset(Integer.parseInt(tokens[4]));
					break;
				}
			}
			deltaMatData.loadDataFromBinFile(fileData.getFileName());
		} catch (Exception e) {
			throw new TwisterException(e);
		}
		this.jobConf = jobConf;
		bz = Integer.parseInt(jobConf.getProperty(MDSShort.PROP_BZ));
		deltaBlock = deltaMatData.getData();
		blockOffset = deltaMatData.getRowOffset();
		blockHeight = deltaMatData.getHeight();
		N = deltaMatData.getWidth();
	}

	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}
}
