package cgl.imr.samples.mds.hydra.a;

import java.util.List;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.StringKey;

/**
 * @author Yang Ruan, yangruan@cs.indiana.edu
 * will change this part to support weighted function
 * @author Jaliya Ekanayake, jekanaya@cs.indiana.edu
 * 
 *         This class simply combine the matrix blocks produce by the
 *         CalcBCMapTasks
 * 
 * Not Implemented
 */
public class CalcXReduceTask implements ReduceTask {

	int N = 0;

	public void configure(JobConf jobConf, ReducerConf mapConf)
			throws TwisterException {
		N = Integer.parseInt(jobConf.getProperty(MDSShort.PROP_N));
	}

	public void reduce(ReduceOutputCollector collector, Key key,
			List<Value> values) throws TwisterException {

		// need to know the size of the centroids.
		MDSMatrixData firstData = (MDSMatrixData) values.get(0);
		int width = firstData.getWidth();

		double results[][] = new double[N][width];
		double tmpData[][] = null;
		MDSMatrixData mData = null;
		int height = 0;
		int offset = 0;
		for (Value val : values) {
			mData = (MDSMatrixData) val;
			height = mData.getHeight();
			offset = mData.getRowOffset();
			tmpData = mData.getData();
			for (int m = 0; m < height; m++) {
				for (int n = 0; n < width; n++) {
					results[m + offset][n] = tmpData[m][n];
				}
			}
		}

		MDSMatrixData rowX = new MDSMatrixData(results, N, width);
		collector.collect(new StringKey("calc-x-reduce-combiner-key"), rowX);

	}

	public void close() throws TwisterException {
		// TODO Auto-generated method stub

	}
}
