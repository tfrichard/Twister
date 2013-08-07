/*
 * Software License, Version 1.0
 *
 *  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 *  the list of authors in the original source code, this list of conditions and
 *  the disclaimer listed in this license;
 * 2) All redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the disclaimer listed in this license in
 *  the documentation and/or other materials provided with the distribution;
 * 3) Any documentation included with all redistributions must include the
 *  following acknowledgement:
 *
 * "This product includes software developed by the Community Grids Lab. For
 *  further information contact the Community Grids Lab at
 *  http://communitygrids.iu.edu/."
 *
 *  Alternatively, this acknowledgement may appear in the software itself, and
 *  wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or Twister,
 *  shall not be used to endorse or promote products derived from this software
 *  without prior written permission from Indiana University.  For written
 *  permission, please contact the Advanced Research and Technology Institute
 *  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 * 5) Products derived from this software may not be called Twister,
 *  nor may Indiana University or Community Grids Lab or Twister appear
 *  in their name, without prior written permission of ARTI.
 *
 *
 *  Indiana University provides no reassurances that the source code provided
 *  does not infringe the patent or any other intellectual property rights of
 *  any other entity.  Indiana University disclaims any liability to any
 *  recipient for claims brought by any other entity based on infringement of
 *  intellectual property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */

package cgl.imr.script;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;

/**
 * This class provide the functionality of enhancing "putall" command (used by
 * putallp command which means putall parallel). It will copy a set of files
 * available in a given directory to a collection of compute nodes. The compute
 * nodes are read from the $TWISTER_HOME/bin/nodes file and the user is expected
 * to give the destination directory (a common directory to in all the nodes) as
 * command line arguments. It uses a thread pool to schedule the file copy
 * processes which utilizes a shell script. Possible improvements to this class
 * would include different file copying mechanisms without the use of secure
 * copy vi a shell script.
 * 
 * Most part of this class is the same as FileDistributor. The difference
 * between FileDistributor and FileAllDistributor is that the first one is for
 * data distribution, every node will get a part of the whole data sets.
 * However, the second one is for data copying, every node will get the same set
 * of data.
 * 
 * @author Bingjing Zhang (jessezbj@gmail.com, zhangbj@cs.indiana.edu) 5/25/2010
 * 
 */

public class FileAllDistributor {

	public static String FILE_COPY_MV_PROGRAM = "scpmvfile.sh";

	// public static int NUM_FILE_COPY_THREADS = 10;

	/**
	 * Get the whole list of files for copying under the directory
	 * 
	 * @param directory
	 * @return
	 */
	private static List<String> getListOfFiles(String directory,
			final String filePrefix) {
		File dir = new File(directory);

		FileFilter fileFilter = new FileFilter() {
			public boolean accept(File file) {
				// System.out.println("File Name: " + file.getName());
				return (!(file.isDirectory()) && (file.getName()
						.startsWith(filePrefix)));
			}
		};

		File[] files = dir.listFiles(fileFilter);
		List<String> selectedFiles = new ArrayList<String>();
		for (File file : files) {
			selectedFiles.add(file.getAbsolutePath());
		}
		return selectedFiles;
	}

	/**
	 * Get the nodes host/IP in nodes file as copy destination
	 * 
	 * @return
	 * @throws ConfigurationException
	 */
	private static List<String> getListOfNodes() throws ConfigurationException {
		List<String> hosts = new ArrayList<String>();
		try {
			TwisterConfigurations configs = TwisterConfigurations.getInstance();
			File nodesFile = new File(configs.getNodeFile());
			BufferedReader reader = new BufferedReader(
					new FileReader(nodesFile));
			String host;

			while ((host = reader.readLine()) != null) {
				hosts.add(host);
			}
			reader.close();
		} catch (Exception e) {
			throw new ConfigurationException(e);
		}
		return hosts;
	}

	/**
	 * This function is the different from the same named function in
	 * FileDistributor, the file list to the node contains all files, not part
	 * of them
	 */
	private static List<List<String>> groupFilesToHosts(List<String> files,
			int numHosts) {
		List<List<String>> groups = new ArrayList<List<String>>();
		for (int i = 0; i < numHosts; i++) {
			groups.add(files);
		}
		return groups;
	}

	/**
	 * main function for parallel data copying
	 * 
	 * @param args
	 * @throws ConfigurationException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws ConfigurationException,
			IOException, InterruptedException {
		double startTime = System.currentTimeMillis();
		if (args.length != 4) {
			System.out
					.println("Usage:[src directory(local)][destination directory (remote)][file filter][num threads]");
			return;
		}

		int numThreads = Integer.parseInt(args[3]);

		TwisterConfigurations configs = null;
		try {
			configs = TwisterConfigurations.getInstance();
		} catch (ConfigurationException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}

		ExecutorService taskExecutor = Executors.newFixedThreadPool(numThreads);

		Object lock = new Object();
		boolean errors = false;

		File srcDir = new File(args[0]);
		if (!srcDir.exists()) {
			System.err
					.println("Invalid input directory. Directory does not exist.");
			System.exit(-1);
		}
		List<String> files = getListOfFiles(args[0], args[2]);
		if (files.size() == 0) {
			System.err.println("No files to copy.");
			System.exit(-1);
		}
		List<String> hosts = getListOfNodes();
		if (hosts.size() == 0) {
			System.err.println("No compute nodes specified in nodes file.");
			System.exit(-1);
		}
		List<List<String>> groups = groupFilesToHosts(files, hosts.size());

		int count = 0;
		String host;
		String dest;
		String dataDir = (configs.getLocalDataDir() + "/" + args[1] + "/").replace(
				"//", "/");
		for (List<String> group : groups) {
			host = hosts.get(count++);
			dest = host + ":" + dataDir;
			for (String file : group) {
				taskExecutor.execute(new FileCopyandMvThread(file, dest, lock,
						errors));
			}
		}

		taskExecutor.shutdown();
		try {
			taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			System.exit(-1);
		}
		double endTime = System.currentTimeMillis();
		System.out.println("Total Data Copy Time = " + (endTime - startTime)
				/ 1000 + " Seconds.");

		if (errors) {
			System.exit(-1);
		} else {
			System.exit(0);
		}
	}
}
