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

import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import org.apache.log4j.Logger;

import cgl.imr.base.TwisterConstants;
import cgl.imr.config.TwisterConfigurations;

/**
 * A utility class to stop daemons via the port that they listen.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class StopDaemons {

	private static Logger logger = Logger.getLogger(StopDaemons.class);

	public static void main(String[] args) {

		if (args.length != 3) {
			System.out
					.println("Usage StopSuperDaemons [host][host number][num daemons]");
			System.exit(0);
		}

		String daemonHost = args[0];
		int hostNumber = Integer.valueOf(args[1]).intValue();
		int numDaemons = Integer.valueOf(args[2]).intValue();
		int socketPort = 0;
		TwisterConfigurations configs = null;
		try {
			configs = TwisterConfigurations.getInstance();
		} catch (Exception e) {
			logger.error(e);
		}
		int daemonPortBase = configs.getDaemonPortBase();
		for (int i = 0; i < numDaemons; i++) {
			//ZBJ: catch exception when shutting down each daemon
			try {
				InetAddress addr = InetAddress.getByName(daemonHost);
				socketPort = daemonPortBase + i + (hostNumber * numDaemons);
				logger.debug("Stopping daemon on port number: " + socketPort);
				SocketAddress sockaddr = new InetSocketAddress(addr, socketPort);

				// Create an unbound socket
				Socket sock = new Socket();

				// This method will block no more than timeout Ms.
				// If the timeout occurs, SocketTimeoutException is thrown.
				int timeoutMs = 10000; // 2 seconds
				sock.connect(sockaddr, timeoutMs);
				
				DataOutputStream dout = new DataOutputStream(
						sock.getOutputStream());

				// Now send the stop signal to the TwisterDaemon
				dout.writeByte(TwisterConstants.DAEMON_QUIT_REQUEST);
				dout.flush();

				/*
				BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(
						sock.getOutputStream()));
				wr.write("quit\n");
				wr.flush();
				*/
				dout.close();
				sock.close();
			} catch (Exception e) {
				logger.error(e);
			}
		}
			logger
					.debug("Finished sending the term signal to daemons in host: "
							+ daemonHost);
			System.exit(0);

		
	}
}
