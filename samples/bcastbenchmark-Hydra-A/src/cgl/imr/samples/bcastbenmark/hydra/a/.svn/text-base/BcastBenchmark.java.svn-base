package cgl.imr.samples.bcastbenmark.hydra.a;

import java.util.ArrayList;
import java.util.List;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.DoubleVectorData;

public class BcastBenchmark {

	public static void main(String[] args) throws Exception {
		int numChunks = Integer.parseInt(args[0]);
		int totalNumData = Integer.parseInt(args[1]);
		int numLoop = Integer.parseInt(args[2]);
		List<Value> values = new ArrayList<Value>();
		int vecLen = 512;
		int numData = totalNumData / numChunks;
		for (int i = 0; i < numChunks; i++) {
			values.add(new DoubleVectorData(new double[numData][vecLen],
					numData, vecLen));
		}
		JobConf jobConf = new JobConf("bcast-benchmark-"
				+ UUIDGenerator.getInstance().generateTimeBasedUUID());
		// driver initialization
		TwisterDriver driver = new TwisterDriver(jobConf);
		for (int loopCount = 0; loopCount < numLoop; loopCount++) {
			// addresses are set based on the order in the centroids list
			driver.addToMemCache(values);
			driver.cleanMemCache();
		}
		driver.close();
	}
}
