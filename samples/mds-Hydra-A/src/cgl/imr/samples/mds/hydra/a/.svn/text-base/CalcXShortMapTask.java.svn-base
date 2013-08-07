package cgl.imr.samples.mds.hydra.a;

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
 * @author Yang Ruan, yangruan@cs.indiana.edu will change this part to support
 *         weighted function Jaliya Ekanayake, jekanaya@cs.indiana.edu This map
 *         task simply perform a matrix rowblock and matrix column block (a
 *         vector) multiplication. The resulting matrix block is sent to the
 *         appropriate reduce task.
 * 
 *         Not Implemented
 */
public class CalcXShortMapTask implements MapTask {

	JobConf jobConf;

	int bz = 0;
	int N = 0;
	int blockHeight = 0;
	double[][] invVBlock = null;
	int rowOffset = 0;

	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
		MemCacheAddress memCacheKey = (MemCacheAddress) val;
		MDSMatrixData mData = (MDSMatrixData) (MemCache.getInstance().get(
				jobConf.getJobId(), memCacheKey.getMemCacheKeyBase()
						+ memCacheKey.getStart()));
		double[][] BC = mData.getData();

		// Next we can calculate the BofZ * preX.
		double[][] X = MatrixUtils.matrixMultiply(invVBlock, BC, blockHeight,
				BC[0].length, N, bz);

		// Send C with the map task number to a reduce task. Which will simply
		// combine these parts and form the N x d matrix.
		// We don't need offset here.
		MDSMatrixData newMData = new MDSMatrixData(X, blockHeight, X[0].length,
				mData.getRow(), rowOffset);
		collector.collect(new StringKey("X-calc-map-to-reduce-key"), newMData);
	}

	/**
	 * Loads the necessary fixed data. For this computation the fixed data is
	 * the inverseV matrix block.
	 */
	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException {

		// FileData fileData = (FileData) mapConf.getDataPartition();
		// String filename = fileData.getFileName();
		//
		// MDSMatrixData deltaBlock = (MDSMatrixData)
		// (MemCache.getInstance().get(jobConf.getFriendJobList().get(0),
		// filename));
		// if(deltaBlock == null) {
		// System.out.println("add deltaBlock Cache " + filename);
		// deltaBlock = new MDSMatrixData();
		// try {
		// deltaBlock.loadDataFromBinFile(filename);
		// } catch (Exception e) {
		// throw new TwisterException(e);
		// }
		// MemCache.getInstance().add(jobConf.getFriendJobList().get(0),
		// filename, deltaBlock);
		// }
		MDSShortMatrixData deltaBlock = new MDSShortMatrixData();
		FileData fileData = (FileData) mapConf.getDataPartition();
		try {
			deltaBlock.loadDataFromBinFile(fileData.getFileName());
		} catch (Exception e) {
			throw new TwisterException(e);
		}
		this.jobConf = jobConf;

		bz = Integer.parseInt(jobConf.getProperty(MDSShort.PROP_BZ));

		rowOffset = deltaBlock.getRowOffset();
		blockHeight = deltaBlock.getHeight();
		int end = rowOffset + blockHeight;
		N = deltaBlock.getWidth();
		invVBlock = new double[blockHeight][N];

		double numSqure = N * N;
		int tmpJ = 0;
		for (int j = rowOffset; j < end; j++) {
			tmpJ = j - rowOffset;
			for (int k = 0; k < N; k++) {
				if (j != k) {
					invVBlock[tmpJ][k] = -1.0 / numSqure;

				} else {
					double N1 = N - 1;
					invVBlock[tmpJ][k] = N1 / numSqure;

				}
			}
		}
	}

	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}
}
