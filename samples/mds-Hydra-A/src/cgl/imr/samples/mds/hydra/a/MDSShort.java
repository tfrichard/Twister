package cgl.imr.samples.mds.hydra.a;

/**
 * @author Yang Ruan yangruan@cs.indiana.edu
 * 			Fixed the bug for data over 50k.
 * 			Planning to add weighted function in the future
 * @author Jaliya Ekanayake, jekanaya@cs.indiana.edu
 * 
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.Key;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.GenericCombiner;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.DoubleArray;
import cgl.imr.types.DoubleValue;
import cgl.imr.types.MemCacheAddress;

public class MDSShort {

	public static String PROP_BZ = "mat_mult_block_size";
	public static String PROP_N = "prop_N";
	private static UUIDGenerator uuidGen = UUIDGenerator.getInstance();
	private static int BLOCK_SIZE = 64;

	// private static final String DATE_FORMAT_NOW = "yyyy-MM-dd";
	/*
	 * private static String date() { Calendar cal = Calendar.getInstance();
	 * SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW); return
	 * sdf.format(cal.getTime()); }
	 */

	// This is the maximum determinant value for using Inverse or pseudo
	// inverse.
	// private static double MAX_DET = 10000000;
	private static int MAX_ITER = 10000;
	private static int N;
	private static double avgOrigDistance;
	private static double sumOrigDistanceSquare;
	private static double avgOrigDistanceSquare;

	public static void main(String[] args) {
		double beginTime = System.currentTimeMillis();
		if (args.length != 10) {
			System.out.println("Usage: ");
			System.out.println("[1. Num Map Tasks ]");
			System.out.println("[2. Partition File ]");
			System.out.println("[3. Idx File (partition information)]");
			System.out.println("[4. Label Data File]");
			System.out.println("[5. Output File ]");
			System.out.println("[6. Threshold value ]");//
			System.out
					.println("[7. The Weighted Flag (0: No weight, 1: weighted)]");
			System.out.println("[8. The Target Dimension ]");
			System.out.println("[9. Number of loops ]");
			System.out.println("[10. Data size]");
			System.exit(0);
		}
		int numMapTasks = Integer.valueOf(args[0]);
		String partitionFile = args[1];
		String idxFile = args[2];
		String labelsFile = args[3];
		String outputFile = args[4];
		double threshold = Double.valueOf(args[5]);
		int weighted = Integer.valueOf(args[6]);
		int D = Integer.valueOf(args[7]);
		int numLoops = Integer.valueOf(args[8]);
		N = Integer.parseInt(args[9]);
		try {
			performInitialCalculations(numMapTasks, partitionFile, idxFile);
			System.out.println(" N: " + N);
			System.out.println(" AvgOrgDistance: " + avgOrigDistance);
			System.out.println(" SumSquareOrgDistance: "
					+ sumOrigDistanceSquare);
			double[][] preX = generateInitMapping(N, D);
			// Currently we only support non weight function
			if (weighted != 0) {
				throw new Exception("Weighted option is not supported yet");
			}
			TwisterModel stressDriver = configureCalculateStress(numMapTasks,
					partitionFile, idxFile);
			Double stress = null;
			Double preStress = calculateStress(stressDriver, preX, numMapTasks);
			System.out.println("Pre Stress is " + preStress);
			double diffStress = threshold;
			int iter = 0;
			// TwisterModel xDriver = null;
			double X[][] = null;
			double BC[][] = null;
			// Configuring BC MapReduce driver.
			TwisterModel bcDriver = configureCalculateBC(numMapTasks,
					partitionFile, idxFile);
			double QoR1 = 0;
			double QoR2 = 0;
			double avgOrigDist = avgOrigDistance;
			double endTime = System.currentTimeMillis();
			System.out.println("Upto the loop took =" + (endTime - beginTime)
					/ 1000 + " Seconds.");
			int retryCount = 0;
			for (iter = 0; iter < numLoops; iter++) {
				BC = calculateBC(bcDriver, preX);
				if (BC == null) {
					System.out.println("BC==null, iter = " + iter);
					retryCount++;
					iter--;
					continue;
				}
				// Ignore this
				if (weighted != 0) {
					// put some code which do the weighted X
					/*
					 * X = calculateX(xDriver, BC); if (X == null) {
					 * retryCount++; iter--; continue; }
					 */
				} else {
					// We only needs to calculate BC
					X = BC;
				}
				// Stress calculation
				// Another MapReduce job in this iteration
				stress = calculateStress(stressDriver, X, numMapTasks);
				if (stress == null) {
					System.out.println("stress==null, iter = " + iter);
					retryCount++;
					iter--;
					continue;
				}
				diffStress = preStress - stress;
				preStress = stress;
				preX = MatrixUtils.copy(X);
				if ((iter % 10 == 0) || (iter >= MAX_ITER)) {
					System.out.println("Iteration #################### " + iter
							+ "  diffStress = " + diffStress + " Stress = "
							+ stress);
				}
				if (diffStress < 0) {
					System.err
							.println("Program Error! Result might be wrong!!");
					System.exit(2);
				}
				if (iter >= MAX_ITER || diffStress < threshold) {
					QoR1 = stress / (N * (N - 1) / 2);
					QoR2 = QoR1 / (avgOrigDist * avgOrigDist);

					System.out.println("Normalize1 = " + QoR1
							+ " Normalize2 = " + QoR2);
					System.out.println("Average of Delta(original distance) = "
							+ avgOrigDist);
					break;
				}
			}
			endTime = System.currentTimeMillis();
			if (labelsFile.endsWith("NoLabel")) {
				writeOuput(BC, outputFile);
			} else {
				writeOuput(BC, labelsFile, outputFile);
			}
			bcDriver.close();
			/*
			 * if (weighted != 0) { xDriver.close(); }
			 */
			stressDriver.close();
			System.out.println("Total retry count: " + retryCount);
			System.out
					.println("===================================================");
			System.out.println("Total Time" + (endTime - beginTime) / 1000
					+ " Seconds.");
			System.out
					.println("===================================================");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
		System.exit(0);
	}

	/**
	 * This will set values for the following parameters. (i) N - size of the
	 * matrix, (ii)ids -ID vector, and (iii) averageOriginalDisance - average of
	 * sum of all the distances.
	 * 
	 * @param numMapTasks
	 *            - Number of map tasks.
	 * @param partitionFile
	 *            - Partition file.
	 * @param idsFile
	 *            - File containing IDs.
	 * @throws Exception
	 */
	private static void performInitialCalculations(int numMapTasks,
			String partitionFile, String idxFile) throws Exception {
		// JobConfigurations for calculating original average distance.
		JobConf jobConf = new JobConf("avg-original-distance"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(AvgOrigDistannceShortMapTask.class);
		jobConf.setReducerClass(AvgOrigDistanceReduceTask.class);
		jobConf.setCombinerClass(GenericCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(1);// One reducer is enough since we just
		// summing some numbers.
		jobConf.addProperty("IdxFile", idxFile);
		jobConf.setFaultTolerance();
		TwisterModel driver = null;
		TwisterMonitor monitor = null;
		GenericCombiner combiner;
		try {
			driver = new TwisterDriver(jobConf);
			driver.configureMaps(partitionFile);
			monitor = driver.runMapReduce();
			monitor.monitorTillCompletion();
			combiner = (GenericCombiner) driver.getCurrentCombiner();
			if (!combiner.getResults().isEmpty()) {
				Key key = combiner.getResults().keySet().iterator().next();
				avgOrigDistance = ((DoubleArray) combiner.getResults().get(key))
						.getData()[0];
				sumOrigDistanceSquare = ((DoubleArray) combiner.getResults()
						.get(key)).getData()[1];
				// Ignoring the diagonal zeros from the average.
				avgOrigDistance /= (N * N - N);
				System.out.println("avg: " + avgOrigDistance);
				avgOrigDistanceSquare = sumOrigDistanceSquare / (N * N - N);
				System.out.println("avg square: " + avgOrigDistanceSquare);
			} else {
				System.err.println("Combiner did not return any values.");
				driver.close();
				System.exit(-1);
			}
		} catch (TwisterException e) {
			driver.close();
			throw e;
		}
		driver.close();
	}

	/**
	 * This method will be used to generate initial mapped data X(0), when there
	 * is no initial mapped data for the problem. Normally, target space
	 * 2-dimension space.
	 * 
	 * @param numDataPoints
	 * @param targetDim
	 * @return
	 */
	private static double[][] generateInitMapping(int numDataPoints,
			int targetDim) {
		double matX[][] = new double[numDataPoints][targetDim];
		// Use Random class for generating random initial mapping solution.
		// For the test, set the Random seed in order to produce the same result
		// for the same problem.
		// Random rand = new Random(47);
		// Random rand = new Random(System.currentTimeMillis()); // Real random
		Random rand = new Random(); // not real random, suppose to give the same
									// mapping every time
		// seed.
		for (int i = 0; i < numDataPoints; i++) {
			for (int j = 0; j < targetDim; j++) {
				matX[i][j] = rand.nextDouble();
			}
		}
		return matX;
	}

	private static TwisterModel configureCalculateBC(int numMapTasks,
			String partitionFile, String idxFile) throws TwisterException {
		String jobID = "calc-BC-" + uuidGen.generateRandomBasedUUID();
		// we need only one reduce task to aggregate the parts of BC
		int numReducers = 1;
		// JobConfigurations
		JobConf jobConf = new JobConf(jobID);
		jobConf.setMapperClass(CalcBCShortMapTask.class);
		jobConf.setReducerClass(CalcBCReduceTask.class);
		jobConf.setCombinerClass(GenericCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		jobConf.addProperty(PROP_BZ, String.valueOf(BLOCK_SIZE));
		jobConf.addProperty(PROP_N, String.valueOf(N));
		jobConf.addProperty("IdxFile", idxFile);
		jobConf.setFaultTolerance();
		TwisterModel bcDriver = new TwisterDriver(jobConf);
		bcDriver.configureMaps(partitionFile);
		return bcDriver;
	}

	private static double[][] calculateBC(TwisterModel bcDriver, double[][] preX)
			throws TwisterException {
		MDSMatrixData BCMatData = null;
		MDSMatrixData preXMatData = new MDSMatrixData(preX, N, preX[0].length);
		MemCacheAddress memCacheKey = bcDriver.addToMemCache(preXMatData);
		TwisterMonitor monitor = bcDriver.runMapReduceBCast(memCacheKey);
		cgl.imr.client.JobStatus status = monitor.monitorTillCompletion();
		bcDriver.cleanMemCache();
		GenericCombiner combiner = (GenericCombiner) bcDriver
				.getCurrentCombiner();
		if (!combiner.getResults().isEmpty()) {
			Key key = combiner.getResults().keySet().iterator().next();
			BCMatData = (MDSMatrixData) (combiner.getResults().get(key));
			return BCMatData.getData();
		} else {
			if (status.isSuccess()) {
				System.err
						.println("Combiner did not return any values. Terminating Job");
				bcDriver.close();
				System.exit(-1);
			}
			return null;
		}
	}

	/**
	 * 
	 * @param N
	 *            - Size of the data
	 * @param numMapTasks
	 *            - Number of map tasks
	 * @param tmpDir
	 *            - Temporary directory where the data is
	 * @param brokerHost
	 *            - Broker Host, e.g. 156.56.104.15
	 * @param brokerPort
	 *            - Broker Port e.g. 3045
	 * @param numNodes
	 *            - Number of nodes in the cluster
	 * @param bz
	 *            - block size of the block matrix multiplication. This has
	 *            nothing to do with the row block splitting which is done based
	 *            on the number of map tasks.
	 * @return
	 * @throws MRException
	 */
	@SuppressWarnings("unused")
	private static TwisterModel configureCalculateX(int numMapTasks,
			String partitionFile) throws TwisterException {
		// we need only one reduce task to aggregate the parts of X.
		String jobID = "calc-x-" + uuidGen.generateRandomBasedUUID();
		// we need only one reduce task to aggregate the parts of X.
		int numReducers = 1;
		// JobConfigurations
		JobConf jobConf = new JobConf(jobID);
		jobConf.setMapperClass(CalcXShortMapTask.class);
		jobConf.setReducerClass(CalcXReduceTask.class);
		jobConf.setCombinerClass(GenericCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		jobConf.addProperty(PROP_BZ, String.valueOf(BLOCK_SIZE));
		jobConf.addProperty(PROP_N, String.valueOf(N));
		jobConf.setFaultTolerance();
		TwisterModel xDriver = new TwisterDriver(jobConf);
		xDriver.configureMaps(partitionFile);
		return xDriver;
	}

	@SuppressWarnings("unused")
	private static double[][] calculateX(TwisterModel xDriver, double[][] BC)
			throws TwisterException {
		MDSMatrixData XMatData = null;
		MDSMatrixData BCMatData = new MDSMatrixData(BC, BC.length, BC[0].length);
		MemCacheAddress memCacheKey = xDriver.addToMemCache(BCMatData);
		TwisterMonitor monitor = xDriver.runMapReduceBCast(memCacheKey);
		cgl.imr.client.JobStatus status = monitor.monitorTillCompletion();
		xDriver.cleanMemCache();
		GenericCombiner combiner = (GenericCombiner) xDriver
				.getCurrentCombiner();
		if (!combiner.getResults().isEmpty()) {
			Key key = combiner.getResults().keySet().iterator().next();
			XMatData = (MDSMatrixData) (combiner.getResults().get(key));
			return XMatData.getData();
		} else {
			if (status.isSuccess()) {
				System.err
						.println("Combiner did not return any values. Terminating Job");
				xDriver.close();
				System.exit(-1);
			}
			return null;
		}
	}

	public static TwisterModel configureCalculateStress(int numMapTasks,
			String partitionFile, String idxFile) throws TwisterException {
		String jobID = "stress-calc-" + uuidGen.generateRandomBasedUUID();
		// we need only one reducer for the above algorithm.
		int numReducers = 1;

		// JobConfigurations
		JobConf jobConf = new JobConf(jobID);
		jobConf.setMapperClass(StressShortMapTask.class);
		jobConf.setReducerClass(StressReduceTask.class);
		jobConf.setCombinerClass(GenericCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		jobConf.setFaultTolerance();
		jobConf.addProperty("IdxFile", idxFile);

		TwisterModel stressDriver = new TwisterDriver(jobConf);
		stressDriver.configureMaps(partitionFile);

		return stressDriver;
	}

	public static Double calculateStress(TwisterModel stressDriver,
			double[][] preX, int numMapTasks) throws TwisterException {
		double stress = 0;
		MDSMatrixData preXMatData = new MDSMatrixData(preX, N, preX[0].length);
		MemCacheAddress memCacheKey = stressDriver.addToMemCache(preXMatData);
		TwisterMonitor monitor = stressDriver.runMapReduceBCast(memCacheKey);
		cgl.imr.client.JobStatus status = monitor.monitorTillCompletion();
		stressDriver.cleanMemCache();
		GenericCombiner combiner = (GenericCombiner) stressDriver
				.getCurrentCombiner();
		if (!combiner.getResults().isEmpty()) {
			Key key = combiner.getResults().keySet().iterator().next();
			stress = ((DoubleValue) combiner.getResults().get(key)).getVal();
			// System.out.println("STRESS: " + stress );
			return new Double(stress / (sumOrigDistanceSquare));
		} else {
			if (status.isSuccess()) {
				System.err
						.println("Combiner did not return any values. Terminating Job");
				stressDriver.close();
				System.exit(-1);
			}
			return null;
		}
		/*
		 * Stress divided by half of the sum of the square of the original
		 * distance to normalize it. We need it to be by half of the sum because
		 * when we calculate the stress we do it only for the upper triangular
		 * matrix. Otherwise we could just divide by the sum.
		 */
		// return stress/(sumOrigDistanceSquare/2);
	}

	private static void writeOuput(double[][] x, String outputFile)
			throws IOException {
		PrintWriter writer = new PrintWriter(new FileWriter(outputFile));
		int N = x.length;
		int vecLen = x[0].length;

		DecimalFormat format = new DecimalFormat("#.##########");
		for (int i = 0; i < N; i++) {
			writer.print(String.valueOf(i) + "\t"); // print ID.
			for (int j = 0; j < vecLen; j++) {
				writer.print(format.format(x[i][j]) + "\t"); // print
				// configuration
				// of each axis.
			}
			writer.println("1"); // print label value, which is ONE for all
			// data.
		}
		writer.flush();
		writer.close();

	}

	private static void writeOuput(double[][] X, String labelFile,
			String outputFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(labelFile));
		String line = null;
		String parts[] = null;
		Map<String, Integer> labels = new HashMap<String, Integer>();
		while ((line = reader.readLine()) != null) {
			parts = line.split("\t");
			if (parts.length < 2) {
				// Don't need to throw an error because this is the last part of
				// the computation
				System.out.println("ERROR: Invalid lable");
			}
			labels.put(parts[0].trim(), Integer.valueOf(parts[1]));
		}
		reader.close();

		File file = new File(outputFile);
		PrintWriter writer = new PrintWriter(new FileWriter(file));

		int N = X.length;
		int vecLen = X[0].length;

		DecimalFormat format = new DecimalFormat("#.##########");
		for (int i = 0; i < N; i++) {
			writer.print(String.valueOf(i) + "\t"); // print ID.
			for (int j = 0; j < vecLen; j++) {
				writer.print(format.format(X[i][j]) + "\t"); // print
				// configuration
				// of each axis.
			}
			writer.println(labels.get(String.valueOf(i))); // print label
															// value, which
															// is
			// ONE for all data.
		}
		writer.flush();
		writer.close();
	}
}
