package cgl.imr.samples.kmeans.hydra.dcran.a;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

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
		int centroidsSize = 0;
		MemCacheAddress memCacheKey = (MemCacheAddress) val;
		List<BcastCentroidVectorData> centroids = new ArrayList<BcastCentroidVectorData>();
		int index = memCacheKey.getStart();
		for (int i = 0; i < memCacheKey.getRange(); i++) {
			BcastCentroidVectorData data = (BcastCentroidVectorData) (MemCache
					.getInstance().get(this.jobConf.getJobId(),
					memCacheKey.getMemCacheKeyBase() + index));
			if (data != null) {
				centroids.add(data);
				centroidsSize = centroidsSize + data.getNumData();
			}
			index++;
		}
		// check the total centroids blocks
		if (centroids.size() != memCacheKey.getRange()) {
			throw new TwisterException("Fail in getting all the centroids");
		}
		// image data
		byte[][] imageData = this.image.getData();
		int numImageData = this.image.getNumData();
		int vecLen = this.image.getVecLen();
		/*
		 * image data index and the min distance to a centroid . this is about
		 * 65000*8= 520KB
		 */
		double[] minDis = new double[numImageData];
		int[] labels = new int[numImageData];
		// temp values
		double dis = 0;
		int centroidIndex = 0;
		if (centroidsSize > numImageData) {
			System.out.println(" centroidsSize " + centroidsSize
					+ " numImageData " + numImageData);
			for (int i = 0; i < centroids.size(); i++) {
				BcastCentroidVectorData centroid = centroids.get(i);
				byte[][] centroidData = centroid.getData();
				int numCentroidData = centroid.getNumData();
				for (int j = 0; j < numCentroidData; j++) {
					for (int k = 0; k < numImageData; k++) {
						dis = getEuclidean(imageData[k], centroidData[j],
								vecLen);
						if (i == 0 && j == 0) {
							minDis[k] = dis;
							labels[k] = centroidIndex;
						}
						if (dis < minDis[k]) {
							minDis[k] = dis;
							labels[k] = centroidIndex;
						}
					}
					centroidIndex++;
				}
			}
		} else {
			System.out.println(" numImageData " + numImageData
					+ " centroidsSize " + centroidsSize);
			for (int i = 0; i < numImageData; i++) {
				// for each centroids block
				centroidIndex = 0;
				for (int j = 0; j < centroids.size(); j++) {
					BcastCentroidVectorData centroid = centroids.get(j);
					byte[][] centroidData = centroid.getData();
					int numCentroidData = centroid.getNumData();
					// for each row
					for (int k = 0; k < numCentroidData; k++) {
						dis = getEuclidean(imageData[i], centroidData[k],
								vecLen);
						// set init minDis for a new coming data point
						if (j == 0 && k == 0) {
							minDis[i] = dis;
							labels[i] = centroidIndex;
						}
						if (dis < minDis[i]) {
							minDis[i] = dis;
							labels[i] = centroidIndex;
						}
						centroidIndex++;
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
			for (int i = 0; i < labels.length; i++) {
				// image ID, centroid ID
				writer.println(this.image.getIDs()[i][0] + " "
						+ this.image.getIDs()[i][1] + " "
						+ this.image.getIDs()[i][2] + "\t" + labels[i]);
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
