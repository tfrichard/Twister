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

package cgl.imr.data.file;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import cgl.imr.base.TwisterConstants;
import cgl.imr.data.DataPartitionException;

/**
 * <code>PartitionFile </code> is a specific <code> PartitionInfo</code>.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class PartitionFile {
	protected String fileName;
	protected Map<String, Queue<Integer>> partitions;

	public PartitionFile(String fileName) throws DataPartitionException {
		super();
		this.fileName = fileName;
		partitions = new HashMap<String, Queue<Integer>>();
		populatePartitionsNodeMap(fileName);
	}

	public String getFileName() {
		return fileName;
	}

	public Set<String> getPartitions() {
		return partitions.keySet();
	}

	public Queue<Integer> getDaemonsForDataPartition(String fileName) {
		return partitions.get(fileName);
	}

	public int getNumberOfFiles() {
		return this.partitions.size();
	}

	/**
	 * Assign data partitions - files - to map tasks.
	 */
	protected void populatePartitionsNodeMap(String partitionFile)
			throws DataPartitionException {
		String line = null;
		String[] parts = null;
		int daemon = 0;
		String fileName;
		Queue<Integer> daemonQueue = null;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					partitionFile));

			while ((line = reader.readLine()) != null) {
				parts = line
						.split(TwisterConstants.PARTITION_FILE_SPLIT_PATTERN);
				if (parts.length != 4) {
					throw new DataPartitionException(
							"Illformed partition file.");
				}
				daemon = Integer.parseInt(parts[2]);
				// ZBJ: IP is added to file name as new key
				// in order to separate same file on different node
				// fileName = parts[1] + ":" + parts[3];
				// change back to original code due to files with same name on
				// different nodes
				// are considered as replicas of one file
				fileName = parts[3];
				daemonQueue = partitions.get(fileName);
				if (daemonQueue == null) {
					daemonQueue = new LinkedBlockingQueue<Integer>();
					partitions.put(fileName, daemonQueue);
				}
				daemonQueue.add(daemon);
			}
			reader.close();
		} catch (Exception e) {
			throw new DataPartitionException(
					"Error in reading the partition file.", e);
		}
	}
}
