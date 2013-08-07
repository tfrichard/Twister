package cgl.imr.samples.mds.hydra.a;

import java.util.List;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.DoubleArray;
import cgl.imr.types.IntKey;

public class AvgOrigDistanceReduceTask implements ReduceTask {

	@Override
	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}

	@Override
	public void configure(JobConf jobConf, ReducerConf reducerConf)
			throws TwisterException {

	}

	@Override
	public void reduce(ReduceOutputCollector collector, Key key,
			List<Value> values) throws TwisterException {
		double average = 0;
		double avgSquare = 0;

		double[] averages;
		for (Value val : values) {
			averages = ((DoubleArray) val).getData();
			average += averages[0];
			avgSquare += averages[1];
		}
		// Only one key from here.
		double[] avgs = new double[2];
		avgs[0] = average;
		avgs[1] = avgSquare;
		collector.collect(new IntKey(0), new DoubleArray(avgs, 2));
	}
}
