/**
 * Software License, Version 1.0
 * 
 * Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 * 
 *
 *Redistribution and use in source and binary forms, with or without 
 *modification, are permitted provided that the following conditions are met:
 *
 *1) All redistributions of source code must retain the above copyright notice,
 * the list of authors in the original source code, this list of conditions and
 * the disclaimer listed in this license;
 *2) All redistributions in binary form must reproduce the above copyright 
 * notice, this list of conditions and the disclaimer listed in this license in
 * the documentation and/or other materials provided with the distribution;
 *3) Any documentation included with all redistributions must include the 
 * following acknowledgement:
 *
 *"This product includes software developed by the Community Grids Lab. For 
 * further information contact the Community Grids Lab at 
 * http://communitygrids.iu.edu/."
 *
 * Alternatively, this acknowledgement may appear in the software itself, and 
 * wherever such third-party acknowledgments normally appear.
 * 
 *4) The name Indiana University or Community Grids Lab or Twister, 
 * shall not be used to endorse or promote products derived from this software 
 * without prior written permission from Indiana University.  For written 
 * permission, please contact the Advanced Research and Technology Institute 
 * ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 *5) Products derived from this software may not be called Twister, 
 * nor may Indiana University or Community Grids Lab or Twister appear
 * in their name, without prior written permission of ARTI.
 * 
 *
 * Indiana University provides no reassurances that the source code provided 
 * does not infringe the patent or any other intellectual property rights of 
 * any other entity.  Indiana University disclaims any liability to any 
 * recipient for claims brought by any other entity based on infringement of 
 * intellectual property rights or otherwise.  
 *
 *LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO 
 *WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 *NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF 
 *INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS. 
 *INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS", 
 *"VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.  
 *LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR 
 *ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION 
 *GENERATED USING SOFTWARE.
 */

package cgl.imr.base;

import java.util.List;

import cgl.imr.types.MemCacheAddress;

public interface TwisterModel {
	/**
	 * Cleanup the job related states, including the broker connections.
	 */
	public void close();

	/**
	 * Configure map tasks without any input. Can be used with typical map
	 * reduce computations which does not require the configure and use many
	 * times behavior as in iterative MapReduce computations.
	 * 
	 * @throws TwisterException
	 */
	public void configureMaps() throws TwisterException;

	/* Configure map tasks using a partition file. */
	public void configureMaps(String partitionFile) throws TwisterException;

	/**
	 * Configure the map tasks using static values/configurations. Here the
	 * <code>Value[]</code> can carry any list of values that are assigned to
	 * the map tasks equally (or nearly equally).
	 * 
	 * @param values
	 *            - Array of values.
	 * @throws TwisterException
	 */
	public void configureMaps(List<Value> values) throws TwisterException;

	/**
	 * Allow users to configure reduce tasks with static configuration. Here the
	 * <code>Value[]</code> can carry any list of values that are assigned to
	 * the reduce tasks equally (or nearly equally).
	 * 
	 * @param values
	 *            - Array of values.
	 * @throws TwisterException
	 */
	public void configureReduce(List<Value> values) throws TwisterException;

	public MemCacheAddress addToMemCache(Value value) throws TwisterException;

	public MemCacheAddress addToMemCache(List<Value> values)
			throws TwisterException;

	public void cleanMemCache() throws TwisterException;

	/**
	 * Starts the MapReduce without any variable data (Key, Value). Assumes that
	 * the map tasks are configured with data using the static data
	 * configuration step. Suitable for many map-only style operations.
	 * 
	 * @return TwisterMonitor - A monitor that allows the user program to
	 *         asynchronously wait on the progress of the MapReduce computation.
	 * @throws TwisterException
	 */
	public TwisterMonitor runMapReduce() throws TwisterException;

	public abstract TwisterMonitor runMapReduce(List<KeyValuePair> pairs)
			throws TwisterException;

	/**
	 * Starts the MapReduce with a single Value. This a broad cast style
	 * operation. One value is sent to all the map tasks. The
	 * <code>configureMap</code> method can be used to configure the Map tasks
	 * with variable data. We found this pattern is also a common usage scenario
	 * in MapReduce.
	 * 
	 * @return TwisterMonitor - A monitor that allows the user program to
	 *         asynchronously wait on the progress of the MapReduce computation.
	 * @throws TwisterException
	 */
	public TwisterMonitor runMapReduceBCast(Value val) throws TwisterException;

	public Combiner getCurrentCombiner() throws TwisterException;
}
