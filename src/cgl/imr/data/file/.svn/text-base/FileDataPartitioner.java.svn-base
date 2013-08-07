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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import cgl.imr.data.DataPartitionException;

/**
 * Assign map tasks for a given file data partition.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 *         Change all the algorithms here.
 * 
 *         Consider that there are replicas of each file on computing nodes.
 *         Files are not mandatory to be assigned to some node, but to be
 *         assigned evenly among all nodes.
 * 
 *         However, original straightforward algorithm can not achieve this.
 * 
 *         Max flow algorithm are used here for assignment. The edges from the
 *         source to files presents the availability of the file. Each edge
 *         between file and daemon is stand for the assignability of the file to
 *         the daemon. since one file can only be assigned to one daemon, the
 *         capability is 1. The edges between daemons to the sink is the
 *         capability of the daemon.
 * 
 *         In the first turn of assignment, the capability of daemon is set to
 *         the average number of files on each daemon. If there are some files
 *         not assigned after the first turn, the algorithm will pick them up
 *         and continually do the matching
 * 
 * @author Bingjing Zhang (zhangbj@indiana.edu)
 * 
 * 
 */
public class FileDataPartitioner {

	/**
	 * This function is invoked by the driver
	 * return a file partition name and a daemon
	 * @param daemons
	 * @param partitions
	 * @return
	 * @throws DataPartitionException
	 */
	public static Map<String, Integer> assignPartitionsToDaemons(
			List<Integer> daemons, PartitionFile partitions)
			throws DataPartitionException {
		// the data should be returned
		Map<String, Integer> partitionAndDaemons = new HashMap<String, Integer>();
		if (daemons.size() == 0) {
			// if no daemons available,
			throw new DataPartitionException("No nodes available.");
		}
		// try to use max flow to solve assignment problem.
		// source .... file_nodes...daemon_nodes...sink
		// files as source part
		Set<String> files = new HashSet<String>();
		// edge connections between files and daemons,
		// edges with file to daemon direction use file as key
		// edges with daemon to file use daemon as key
		Map<String, Set<String>> filedaemonAvailability = new HashMap<String, Set<String>>();
		// daemonCapability is set as sink, daemon number as key, capability of
		// each daemon as value
		Map<String, Integer> daemonCapability = new HashMap<String, Integer>();
		// prepare to do initialization,
		Set<String> fileList = partitions.getPartitions();
		// the list got from the partitions
		List<Integer> daemonsForFile = null;
		// the set for building edge information
		Set<String> availableDaemonsForFile = null;
		// build source part sink part, connection part of the max flow chart
		for (String fileName : fileList) {
			daemonsForFile = new ArrayList<Integer>(
					partitions.getDaemonsForDataPartition(fileName));
			// build the available daemons for the file
			availableDaemonsForFile = new HashSet<String>();
			for (int i = 0; i < daemonsForFile.size(); i++) {
				int daemonNO = daemonsForFile.get(i);
				// daemons refer to available daemons
				if (daemons.contains(daemonNO)) {
					availableDaemonsForFile.add(daemonNO + "");
					// if the daemon is available in daemons,
					// but not in daemonCapability, add it to daemonCapability
					if (!daemonCapability.containsKey(daemonNO)) {
						daemonCapability.put(daemonNO + "", 0);
					}
				}
			}
			// add this connection to edge information
			if (availableDaemonsForFile.size() != 0) {
				files.add(fileName);
				filedaemonAvailability.put(fileName, availableDaemonsForFile);
			}
		}
		// check if all files are on available daemons
		if (files.size() != partitions.getNumberOfFiles()) {
			throw new DataPartitionException(
					"There is no  node available for some files. Twister Error.");
		}
		int averageCapability = files.size() / daemonCapability.size();
		// reset values of daemonCapability to its real capability
		for (String d : daemonCapability.keySet()) {
			daemonCapability.put(d, averageCapability);
		}
		// do some output later
		// System.out.println("average capability: " + averageCapability);
		// a function to return a match between files and daemons, in
		// String-Integer Mapping
		Map<String, String> filesAndDaemons = matchByMaxFlow(files,
				filedaemonAvailability, daemonCapability);
		Set<String> matchedFiles = filesAndDaemons.keySet();
		Map<String, String> restFilesAndDaemons = null;
		// if some files are not matched yet
		while (matchedFiles.size() != partitions.getNumberOfFiles()) {
			for (String file : matchedFiles) {
				files.remove(file);
				filedaemonAvailability.remove(file);
			}
			// only one file left assign directly
			if (files.size() == 1) {
				String file = files.iterator().next();
				String dm = filedaemonAvailability.get(file).iterator().next();
				filesAndDaemons.put(file, dm);
				break;
			}
			daemonCapability.clear();
			// rebuild the daemon capability,set the number to 1,
			// keep the assignment to be evenly
			for (String file : filedaemonAvailability.keySet()) {
				for (String daemonNO : filedaemonAvailability.get(file)) {
					if (!daemonCapability.containsKey(daemonNO)) {
						daemonCapability.put(daemonNO, 1);
					}
				}
			}
			// if only one daemon available for the rest files
			if (daemonCapability.size() == 1) {
				String dm = daemonCapability.keySet().iterator().next();
				for (String file : files) {
					filesAndDaemons.put(file, dm);
				}
				break;
			}
			restFilesAndDaemons = matchByMaxFlow(files, filedaemonAvailability,
					daemonCapability);
			// add the mappings to original result
			for (String file : restFilesAndDaemons.keySet()) {
				filesAndDaemons.put(file, restFilesAndDaemons.get(file));
			}
			matchedFiles = filesAndDaemons.keySet();
		}
		for (String file : filesAndDaemons.keySet()) {
			partitionAndDaemons.put(file,
					Integer.parseInt(filesAndDaemons.get(file)));
		}
		// final return and some output test
		/*
		 * System.out.println("******************"); for (String file :
		 * partitionAndDaemons.keySet()) { System.out.println(file + ": " +
		 * partitionAndDaemons.get(file)); }
		 * System.out.println("******************");
		 */
		return partitionAndDaemons;
	}

	/**
	 * The max flow algorithm,
	 * 
	 * all the variables are referred to concepts in graph
	 * 
	 * not to files and daemons, but source and edge parts
	 * 
	 * are all computed based on capability = 1.
	 * 
	 * @param source
	 * @param edge
	 * @param sink
	 * @return
	 */
	private static Map<String, String> matchByMaxFlow(Set<String> source,
			Map<String, Set<String>> edge, Map<String, Integer> sink) {
		Map<String, String> maxFlows = new HashMap<String, String>();
		// explored list by DFS, nodes are referred to nodes in graph
		Set<String> nodesExploredByDFS = new HashSet<String>();
		Stack<String> currentPath = new Stack<String>();
		boolean matchFound = false;
		boolean nonvisitedNodeFound = false;
		// tmp reference
		Set<String> adjacentNodes = null;
		Iterator<String> nodeIterator = null;
		Integer capability = null;
		// source to file cap 1
		// file to daemons cap 1
		// max flow algorithm,
		// check each file from the source
		for (String fileName : source) {
			currentPath.push(fileName);
			// DFS starts, find a path to sink with available capability
			while (currentPath.size() != 0) {
				String node = currentPath.peek();
				// explore this node, check if this is the node to sink
				if (!nodesExploredByDFS.contains(node)) {
					nodesExploredByDFS.add(node);
					capability = sink.get(node);
					if (capability != null) {
						if (capability > 0) {
							capability = capability - 1;
							sink.put(node, capability);
							matchFound = true;
							// System.out.println("checking " + fileName + " "
							// + node + " " + sink.get(node));
							break;
						}
					}
				}
				adjacentNodes = edge.get(node);
				nonvisitedNodeFound = false;
				if (adjacentNodes != null) {
					nodeIterator = adjacentNodes.iterator();
					while (nodeIterator.hasNext()) {
						String adjNode = nodeIterator.next();
						if (!nodesExploredByDFS.contains(adjNode)) {
							// System.out.println("Next exploring: " + node +
							// " " + adjNode);
							currentPath.push(adjNode);
							nonvisitedNodeFound = true;
							break;
						}
					}
					if (!nonvisitedNodeFound) {
						currentPath.pop();
					}
				} else {
					currentPath.pop();
				}
			}
			// process current flow and remain graph
			if (matchFound) {
				List<String> path = new ArrayList<String>(currentPath);
				for (int i = 0; i < path.size() - 1; i++) {
					String curNode = path.get(i);
					String nextNode = path.get(i + 1);
					// put nodes to current flow
					// since max flow only contains one single file to one
					// single node
					// add and remove freely
					String flow = maxFlows.get(nextNode);
					if (flow != null) {
						maxFlows.remove(nextNode);
					} else {
						maxFlows.put(curNode, nextNode);
					}
					// remove and add edges
					adjacentNodes = edge.get(curNode);
					if (adjacentNodes != null) {
						adjacentNodes.remove(nextNode);
						if (adjacentNodes.size() == 0) {
							edge.remove(curNode);
						} else {
							edge.put(curNode, adjacentNodes);
						}
					}
					// add the edge
					adjacentNodes = edge.get(nextNode);
					if (adjacentNodes == null) {
						adjacentNodes = new HashSet<String>();
					}
					adjacentNodes.add(curNode);
					edge.put(nextNode, adjacentNodes);
				}
			}
			matchFound = false;
			currentPath.clear();
			nodesExploredByDFS.clear();
		}
		// some test output
		/*
		 * System.out.println(".............................."); for (String
		 * file : maxFlows.keySet()) { System.out.println(file + ": " +
		 * maxFlows.get(file)); }
		 * System.out.println("..............................");
		 */
		return maxFlows;
	}
}
