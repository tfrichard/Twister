package cgl.imr.samples.kmeans.ivy.dcran.a;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.data.file.FileData;
import cgl.imr.types.MemCacheAddress;
import cgl.imr.worker.MemCache;

public class KMeansPointsAssnMapTask implements MapTask {

	private String fileName;
	private StaticImageVectorData image;
	private JobConf jobConf;

	@Override
	public void close() throws TwisterException {
	}

	@Override
	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException {
		this.jobConf = jobConf;
		FileData fileData = (FileData) mapConf.getDataPartition();
		this.image = new StaticImageVectorData();
		try {
			this.fileName = fileData.getFileName().substring(
					fileData.getFileName().lastIndexOf("/"));
			this.image.loadDataFromTextFile(fileData.getFileName());
		} catch (Exception e) {
			throw new TwisterException(e);
		}

	}

	@Override
	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
		// load centroids data from memcache
		MemCacheAddress memCacheKey = (MemCacheAddress) val;
		if (memCacheKey.getRange() != 1) {
			throw new TwisterException("MemCache size is not 1.");
		}
		BcastCentroidVectorData centroids = (BcastCentroidVectorData) (MemCache
				.getInstance().get(this.jobConf.getJobId(),
				memCacheKey.getMemCacheKeyBase() + memCacheKey.getStart()));
		if (centroids == null) {
			throw new TwisterException("No centroid data.");
		}
		if (centroids.getVecLen() != this.image.getVecLen()) {
			throw new TwisterException(
					"Centroids and Image data are not matched.");
		}
		int vecLen = this.image.getVecLen();
		// image data
		byte[][] imageData = this.image.getData();
		int numImageData = this.image.getNumData();
		// index recording
		double[] minDis = new double[numImageData];
		int[] minCentroidIndex = new int[numImageData];
		// centroid data
		int numCentroidsData = centroids.getNumData();
		byte[][] centroidData = centroids.getData();
		// tmp data
		double dis = 0;
		if (numCentroidsData > numImageData) {
			// for each row
			for (int i = 0; i < numCentroidsData; i++) {
				for (int j = 0; j < numImageData; j++) {
					dis = getEuclidean(imageData[j], centroidData[i], vecLen);
					// set init minDis for a new coming data point
					// we know i is 0, no need to set min block and row
					if (i == 0) {
						minDis[j] = dis;
					}
					if (dis < minDis[j]) {
						minDis[j] = dis;
						minCentroidIndex[j] = i;
					}
				}
			}
		} else {
			for (int i = 0; i < numImageData; i++) {
				for (int j = 0; j < numCentroidsData; j++) {
					dis = getEuclidean(imageData[i], centroidData[j], vecLen);
					// set init minDis for a new coming data point
					if (i == 0) {
						minDis[i] = dis;
					}
					if (dis < minDis[i]) {
						minDis[i] = dis;
						minCentroidIndex[i] = j;
					}
				}
			}
		}
		// print
		String outFileName = this.jobConf.getProperty("output_dir")
				+ this.fileName + ".out";
		PrintWriter writer = null;
		boolean exception = false;
		try {
			writer = new PrintWriter(new BufferedOutputStream(
					new FileOutputStream(outFileName)));
			for (int i = 0; i < minCentroidIndex.length; i++) {
				// image ID, centroid ID
				writer.println(this.image.getIDs()[i][0] + " "
						+ this.image.getIDs()[i][1] + " "
						+ this.image.getIDs()[i][2] + "\t"
						+ minCentroidIndex[i]);
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (exception) {
			if (writer != null) {
				writer.close();
			}
		}
	}

	/**
	 * Though for the real value, each byte should add
	 * KmeansClustering.valueOffset (128),
	 * 
	 * but two items get cancelled in the distance calculation
	 * 
	 * @param v1
	 * @param v2
	 * @param vecLen
	 * @return
	 */
	public double getEuclidean(byte[] v1, byte[] v2, int vecLen) {
		int sum = 0;
		for (int i = 0; i < vecLen; i++) {
			int diff = (int) v1[i] - (int) v2[i];
			sum = sum + diff * diff;
		}
		return Math.sqrt((double) sum); // No need to use the sqrt.
	}
}
