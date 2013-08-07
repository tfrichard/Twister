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
import cgl.imr.types.DoubleArray;
import cgl.imr.types.StringKey;

public class AvgOrigDistannceShortMapTask implements MapTask {

	MDSShortMatrixData rowData;
	MapperConf mapConf;

	@Override
	public void close() throws TwisterException {
	}

	@Override
	public void configure(JobConf jobConf, MapperConf mapConf)
			throws TwisterException {
		this.mapConf = mapConf;
		String idxFile = jobConf.getProperty("IdxFile");
		rowData = new MDSShortMatrixData();
		FileData fileData = (FileData) mapConf.getDataPartition();
		System.out.println(fileData.getFileName());
		try {
			int thisNo = Integer.parseInt(
					fileData.getFileName().substring(
							fileData.getFileName().lastIndexOf("_") + 1, 
							fileData.getFileName().length()));
			BufferedReader br = new BufferedReader(new FileReader(idxFile));
			String line;
			String[] tokens;
			//Read information from idx file, 
			//find the height, width, row id and row offset for current mapper
			while((line = br.readLine())!=null){
				tokens = line.split("\t");
				if(Integer.parseInt(tokens[0]) == thisNo){
					rowData.setHeight(Integer.parseInt(tokens[1]));
					rowData.setWidth(Integer.parseInt(tokens[2]));
					rowData.setRow(Integer.parseInt(tokens[3]));
					rowData.setRowOFfset(Integer.parseInt(tokens[4]));
					break;
				}
			}
			rowData.loadDataFromBinFile(fileData.getFileName());
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}

	@Override
	public void map(MapOutputCollector collector, Key key, Value val)
			throws TwisterException {
		short[][] data = rowData.getData();
		int height = rowData.getHeight();
		int width = rowData.getWidth();
		double average = 0;
		double avgSquare = 0;
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				average += (double)data[i][j]/(double)Short.MAX_VALUE;
				avgSquare += (((double)data[i][j] /(double)Short.MAX_VALUE) * ((double)data[i][j]/(double)Short.MAX_VALUE));
			}
		}
		double[] avgs = new double[2];
		avgs[0] = average;
		avgs[1] = avgSquare;
		collector
				.collect(new StringKey("stress-key"), new DoubleArray(avgs, 2));
	}

}
