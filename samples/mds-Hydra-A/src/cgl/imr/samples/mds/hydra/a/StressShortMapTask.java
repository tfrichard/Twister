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
import cgl.imr.types.DoubleValue;
import cgl.imr.types.MemCacheAddress;
import cgl.imr.types.StringKey;
import cgl.imr.worker.MemCache;

/**
 * @author Thilina Gunarathne tgunarat@cs.indiana.edu
 * 			Reduce the memory footprint of this map task. Make it use shorts
 * 			to store the matrix and convert to double only at the time of the 
 * 			computation. Also got rid of large matrix declarations by doing 
 * 			the calculations in place. Got rid of Math.Pow which is much slower
 * 			than x*x..

 * @author Jaliya Ekanayake, jekanaya@cs.indiana.edu Some code segments contain
 *         in this class are directly inherited from the C# version of the
 *         shared memory MDS program wirtten by my collegue Seung-Hee Bea.
 * 
 *         This class performs the partial calculation of the stress between two
 *         vectors.
 * 
 */
public class StressShortMapTask implements MapTask {

	MDSShortMatrixData deltaBlock;
	MapperConf mapConf;
	JobConf jobConf;

	@Override
	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException {
		FileData fileData = (FileData) mapConf.getDataPartition();

		String idxFile = jobConf.getProperty("IdxFile");
		deltaBlock = new MDSShortMatrixData();
		try {
			int thisNo = Integer.parseInt(
					fileData.getFileName().substring(
							fileData.getFileName().lastIndexOf("_") + 1, 
							fileData.getFileName().length()));
			BufferedReader br = new BufferedReader(new FileReader(idxFile));
			String line;
			String[] tokens;
			while((line = br.readLine())!=null){
				tokens = line.split("\t");
				if(Integer.parseInt(tokens[0]) == thisNo){
					deltaBlock.setHeight(Integer.parseInt(tokens[1]));
					deltaBlock.setWidth(Integer.parseInt(tokens[2]));
					deltaBlock.setRow(Integer.parseInt(tokens[3]));
					deltaBlock.setRowOFfset(Integer.parseInt(tokens[4]));
					break;
				}
			}
			deltaBlock.loadDataFromBinFile(fileData.getFileName());
		} catch (Exception e) {
			// MemCache.getInstance().unLock();
			throw new TwisterException(e);
		}
		//MemCache.getInstance().add(jobConf.getJobId(), filename, deltaBlock);
		
			
		this.mapConf = mapConf;
		this.jobConf = jobConf;
	}

	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
		MemCacheAddress memCacheKey = (MemCacheAddress) val;
		MDSMatrixData mData = (MDSMatrixData) (MemCache.getInstance().get(
				jobConf.getJobId(), memCacheKey.getMemCacheKeyBase()
						+ memCacheKey.getStart()));
		int N = deltaBlock.getWidth();
		int rowOffset = deltaBlock.getRowOffset();
		int rowHeight = deltaBlock.getHeight();
		int end = rowOffset + rowHeight;

		double[][] preXData = mData.getData();
		short deltaMatData[][] = deltaBlock.getData();
		int tmpI = 0;

		double sigma = 0;
		for (int i = rowOffset; i < end; i++) {
			tmpI = i - rowOffset;
			// for (int j = i + 1; j < N; j++) {
			// sigma += Math.pow(deltaMatData[tmpI][j]
			// - distMatData[tmpI][j], 2);
			// }

			for (int j = 0; j < N; j++) {
				double distance;
				if (j != i){
					distance = calculateDistance(preXData,preXData[0].length, i, j);}
				else{
					distance = 0;
				}
				double d = ((double)deltaMatData[tmpI][j]/(double)Short.MAX_VALUE) - distance;
				sigma += d*d;
			}
		}
		
		//System.out.println("sigma: " + sigma );
		
		// Send the partial sigma.
		collector.collect(new StringKey("stress-map-to-reduce-key"),
				new DoubleValue(sigma));
	}

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

	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}

}
