package cgl.imr.samples.mds.hydra.a;

/*
 * @author Yang Ruan(yangruan@indiana.edu)
 */
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class MDSShortDataSplit {

	private static String Separator = ","; // Elements are tab separated.

	public static void main(String[] args) {
		if (args.length != 7) {
			System.out.println("Usage: ");
			System.out.println("[1. Data File ]");
			System.out.println("[2. Temporary directory to split data ]");
			System.out.println("[3. Temp file prefix ]");
			System.out.println("[4. Output IDs file ]");
			System.out.println("[5. Num map tasks ]");
			System.out.println("[6. data size ]");
			System.out
					.println("[7. Type of input file (0: text file; 1: bin file)]");
			System.exit(-1);
		}
		double beginTime = System.currentTimeMillis();

		String dataFile = args[0];
		String tmpDir = args[1];
		String tmpFilePrefix = args[2];
		if (tmpFilePrefix.lastIndexOf("_") != tmpFilePrefix.length() - 1) {
			System.out.println("The input prefix must end with _");
			System.exit(-1);
		}
		String idsFile = args[3];
		int numMapTasks = Integer.valueOf(args[4]);
		int size = Integer.valueOf(args[5]);
		int choice = Integer.parseInt(args[6]);

		// Create a temporary directory to hold data splits.
		if (!(new File(tmpDir)).exists()) {
			if (!(new File(tmpDir)).mkdir()) {
				System.err
						.print("Failed to create the temporary directory to split data");
				System.exit(-1);
			}
		}
		try {
			if (choice == 0)
				splitDataforText(dataFile, tmpDir, tmpFilePrefix, idsFile,
						numMapTasks);
			else if (choice == 1)
				splitDataforBin(dataFile, tmpDir, tmpFilePrefix, idsFile,
						numMapTasks, size);
			else {
				System.err.println("The choice must be 1 or 0");
				System.exit(2);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
		double endTime = System.currentTimeMillis();
		System.out
				.println("==========================================================");
		System.out.println("Time to split data = " + (endTime - beginTime)
				/ 1000 + " Seconds.");
		System.out
				.println("==========================================================");
		System.exit(0);
	}

	private static void splitDataforText(String dataFile, String tmpDir,
			String tmpFilePrefix, String idsFile, int numMapTasks)
			throws IOException {
		File deltaFile = new File(dataFile);
		BufferedReader reader = new BufferedReader(new FileReader(deltaFile));
		reader.readLine(); // First line is the comments line.
		int N = 0;
		while ((reader.readLine()) != null) {
			N++;
		}
		reader.close();

		BufferedWriter idsWriter = new BufferedWriter(new FileWriter(idsFile));

		String outputFile = null;
		reader = new BufferedReader(new FileReader(deltaFile));
		reader.readLine(); // First line is the comments line.
		String line = "";
		String[] lineValues = null;

		int blockHeight = N / numMapTasks;
		int rem = N % numMapTasks;

		MDSMatrixData rowData;
		double[][] row;
		int start = 0;
		int end = 0;
		int curHeight = 0;
		int count = 0;

		idsWriter.write(N + "\n");// First line is the count.
		for (int i = 0; i < numMapTasks; i++) {
			outputFile = tmpDir + "/" + tmpFilePrefix + i;

			end += blockHeight;
			if (rem > 0) {
				end++;
				rem--;
			}
			curHeight = end - start;
			row = new double[curHeight][N];
			count = 0;
			for (int j = start; j < end; j++) {
				line = reader.readLine();
				lineValues = line.split(Separator);
				// zbj: change for data without ID
				// use j as ID
				// read data from 0 not 1
				idsWriter.write(j + "\n");
				for (int k = 0; k < lineValues.length; k++) { // Note the start
					// from 1.----> from 0 now
					row[count][k] = Double.parseDouble(lineValues[k]);
				}
				count++;
			}
			rowData = new MDSMatrixData(row, curHeight, N, i, start);
			start = end;
			rowData.writeToBinFile(outputFile);
		}
		idsWriter.close();
	}

	/**
	 * data size should be provided for reading from bin file
	 * 
	 * @param dataFile
	 * @param tmpDir
	 * @param tmpFilePrefix
	 * @param idsFile
	 * @param numMapTasks
	 * @param size
	 * @throws IOException
	 */

	private static void splitDataforBin(String dataFile, String tmpDir,
			String tmpFilePrefix, String idsFile, int numMapTasks, int size)
			throws IOException {
		BufferedInputStream reader = new BufferedInputStream(
				new FileInputStream(dataFile));
		DataInputStream din = new DataInputStream(reader);
		BufferedWriter idsWriter = new BufferedWriter(new FileWriter(idsFile));
		String outputFile = null;
		int blockHeight = size / numMapTasks;
		int rem = size % numMapTasks;
		MDSShortMatrixData rowData;
		short[][] row;
		int start = 0;
		int end = 0;
		int curHeight = 0;
		int count = 0;
		for (int i = 0; i < numMapTasks; i++) {
			// System.out.println("The " + i + "th maptask");
			idsWriter.write(i + "\t");
			outputFile = tmpDir + "/" + tmpFilePrefix + i /* + ".bin" */;
			end += blockHeight;
			if (rem > 0) {
				end++;
				rem--;
			}
			curHeight = end - start;
			row = new short[curHeight][size];
			count = 0;
			for (int j = start; j < end; j++) {
				for (int k = 0; k < size; k++) { // Note the start
					// from 1.----> from 0 now
					row[count][k] = din.readShort();
				}
				count++;
			}
			rowData = new MDSShortMatrixData(row, curHeight, size, i, start);
			idsWriter.write(curHeight + "\t" + size + "\t" + i + "\t" + start
					+ "\n");
			start = end;
			rowData.writeToBinFile(outputFile);
		}
		idsWriter.close();
	}
}
