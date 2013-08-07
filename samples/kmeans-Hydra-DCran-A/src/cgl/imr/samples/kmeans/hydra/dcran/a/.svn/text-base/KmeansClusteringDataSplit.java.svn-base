package cgl.imr.samples.kmeans.hydra.dcran.a;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.PatternSyntaxException;

public class KmeansClusteringDataSplit {
	public static void partitionFromText(String srcDir, String fileFilter,
			int totalFiles, int partitions, String destDir) throws Exception {
		List<String> files = getListOfFiles(srcDir, fileFilter);
		System.out.println("File Size: " + files.size());
		if (files.size() == 0) {
			throw new Exception("No file is selected.");
		}
		int[] lineCount = new int[files.size()];
		for (int i = 0; i < files.size(); i++) {
			lineCount[i] = KmeansClustering.countTotalLines(files.get(i));
		}
		// skip empty files
		List<String> topFiles = new ArrayList<String>();
		int totalCount = 0;
		for (int i = 0; i < files.size(); i++) {
			if (lineCount[i] > 0) {
				topFiles.add(files.get(i));
				totalCount = totalCount + lineCount[i];
			}
			if (topFiles.size() == totalFiles) {
				break;
			}
		}
		System.out.println("Top Files Size: " + topFiles.size());
		System.out.println("Total Counts: " + totalCount);
		int linePerPartition = totalCount / partitions;
		int rest = totalCount % partitions;
		System.out.println("Line Per Partition: " + linePerPartition);
		System.out.println("Rest " + rest);
		if (linePerPartition == 0) {
			throw new Exception(
					"The number of lines is too small to partition.");
		}
		int tmpLinePerPartition = linePerPartition;
		List<String> bucket = new ArrayList<String>();
		Queue<String> filesQueue = new LinkedBlockingQueue<String>(topFiles);
		BufferedReader bReader = null;
		String line = null;
		String filePath = null;
		for (int i = 0; i < partitions; i++) {
			if (rest > 0) {
				rest--;
				tmpLinePerPartition = linePerPartition + 1;
			} else {
				tmpLinePerPartition = linePerPartition;
			}
			for (int j = 0; j < tmpLinePerPartition; j++) {
				// initially bReader is null, we can't read
				if (bReader != null) {
					line = bReader.readLine();
				}
				// close one and open next
				if (line == null) {
					// close last file,
					if (bReader != null) {
						bReader.close();
						bReader = null;
					}
					// open next, we sure there is next according to the
					// partition
					filePath = filesQueue.poll();
					bReader = new BufferedReader(new FileReader(new File(
							filePath)));
					line = bReader.readLine();
				}
				bucket.add(line);
			}
			// we name the new file with new ID and source file name
			String newFileName = filePath.substring(filePath.lastIndexOf("/"))
					+ "-" + i;
			// output the bucket
			PrintWriter writer = new PrintWriter(new FileWriter(new File(
					destDir + File.separator + newFileName)));
			// System.out.println("Bucket Size " + bucket.size());
			for (String item : bucket) {
				writer.println(item);
			}
			writer.flush();
			writer.close();
			bucket.clear();
		}
		// close the final file,
		if (bReader != null) {
			bReader.close();
			bReader = null;
		}
	}

	private static List<String> getListOfFiles(String directory,
			final String filePrefix) {
		System.out.println("Dir " + directory + " Filter: " + filePrefix);
		File dir = new File(directory);
		// new file filter to accept regular expression
		FileFilter fileFilter = new FileFilter() {
			public boolean accept(File file) {
				if (file.isDirectory()) {
					return false;
				}
				if (file.getName().startsWith(filePrefix)) {
					return true;
				}
				// we assume there is a pattern
				boolean match = false;
				// System.out.println(file.getAbsolutePath());
				try {
					match = file.getName().matches(filePrefix);
				} catch (PatternSyntaxException e) {
					match = false;
				}
				if (match) {
					return true;
				}
				return false;
			}
		};
		File[] files = dir.listFiles(fileFilter);
		List<String> selectedFiles = new ArrayList<String>();
		for (File file : files) {
			selectedFiles.add(file.getAbsolutePath());
		}
		return selectedFiles;
	}

	public static void main(String args[]) {
		if (args.length != 5) {
			System.out
					.println("Initial Data points assignment: the Correct arguments are \n"
							+ " [src dir] [file filter] [total count] [num partitions per file] [dest dir] ");
			System.exit(1);
		}
		String srcDir = args[0];
		String fileFilter = args[1]; // .*[147]
		int fileCount = Integer.parseInt(args[2]);
		int partitions = Integer.parseInt(args[3]);
		String destDir = args[4];
		System.out.println(args[0]);
		System.out.println(args[1]);
		System.out.println(args[2]);
		System.out.println(args[3]);
		System.out.println(args[4]);
		try {
			KmeansClusteringDataSplit.partitionFromText(srcDir, fileFilter,
					fileCount, partitions, destDir);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
