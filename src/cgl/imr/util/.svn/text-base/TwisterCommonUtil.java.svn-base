package cgl.imr.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.client.DaemonInfo;
import cgl.imr.config.TwisterConfigurations;

public class TwisterCommonUtil {

	public static Map<String, byte[]> getDataFromServer(String host, int port,
			List<String> keys, String clientName) throws TwisterException {
		Map<String, byte[]> data = new HashMap<String, byte[]>();
		boolean fault = false;
		int retryCount = 0;
		Socket socket = null;
		DataInputStream dint = null;
		DataOutputStream dout = null;
		do {
			try {
				InetAddress addr = InetAddress.getByName(host);
				SocketAddress sockaddr = new InetSocketAddress(addr, port);
				// Create an unbound socket
				socket = new Socket();
				// This method will block no more than timeout Ms.
				// If the timeout occurs, SocketTimeoutException is thrown.
				int timeoutMs = 0;
				socket.connect(sockaddr, timeoutMs);
				dint = new DataInputStream(socket.getInputStream());
				dout = new DataOutputStream(socket.getOutputStream());
				// sent cmd type
				dout.writeByte(TwisterConstants.DATA_REQUEST);
				dout.flush();
				// the key list to request data
				dout.writeInt(keys.size());
				for (String key : keys) {
					dout.writeUTF(key);
				}
				dout.flush();
				// download data
				byte[] buff = new byte[TwisterConstants.BCAST_SENDRECV_UNIT];
				ByteArrayOutputStream bout = new ByteArrayOutputStream();
				String key = null;
				int dataSize = 0;
				int len = 0;
				int recvDataSize = 0;
				for (int i = 0; i < keys.size(); i++) {
					key = dint.readUTF();
					dataSize = dint.readInt();
					while ((len = dint.read(buff)) > 0) {
						bout.write(buff, 0, len);
						recvDataSize = recvDataSize + len;
						if (recvDataSize == dataSize) {
							break;
						}
					}
					// ack to receiving data
					dout.writeByte(TwisterConstants.DATA_REQUEST_ACK);
					dout.flush();
					// put data to local
					bout.flush();
					bout.close();
					data.put(key, bout.toByteArray());
					// rest some variables
					bout.reset();
					len = 0;
					recvDataSize = 0;
				}
				// close resources
				dout.close();
				dint.close();
				socket.close();
				fault = false;
			} catch (Exception e) {
				System.out.println(clientName
						+ ": Error in downloading data from " + host + ":"
						+ port + ". Retrying... ");
				data.clear();
				fault = true;
				retryCount++;
				if (retryCount == TwisterConstants.SEND_TRIES) {
					e.printStackTrace();
					throw new TwisterException("Error in downloading data", e);
				}
			}
			if (fault) {
				try {
					if (dout != null) {
						dout.close();
					}
				} catch (IOException e) {
				}
				try {
					if (dout != null) {
						dint.close();
					}
				} catch (IOException e) {
				}
				try {
					if (socket != null) {
						socket.close();
					}
				} catch (IOException e) {
				}
			}
		} while (fault);
		return data;
	}

	/**
	 * Gets the number of nodes from the nodes file.
	 * 
	 * @return
	 * @throws IOException
	 */
	public static Map<Integer, DaemonInfo> getDaemonInfoFromConfigFiles(
			TwisterConfigurations config) throws IOException {
		int daemonPortBase = config.getDaemonPortBase();
		int numDaemonsPerNode = config.getDamonsPerNode();
		Map<Integer, DaemonInfo> daemonInfos = Collections
				.synchronizedSortedMap(new TreeMap<Integer, DaemonInfo>());
		int daemonID = 0;
		String line = "";
		BufferedReader reader = new BufferedReader(new FileReader(
				config.getNodeFile()));
		while ((line = reader.readLine()) != null) {
			for (int i = 0; i < numDaemonsPerNode; i++) {
				// port = daemonPortBase + daemonID
				daemonInfos.put(daemonID, new DaemonInfo(line, daemonPortBase
						+ daemonID));
				daemonID++;
			}
		}
		reader.close();
		return daemonInfos;
	}
}
