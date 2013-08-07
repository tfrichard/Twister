package cgl.imr.worker;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.apache.log4j.Logger;
import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.impl.GenericBytesMessage;
import cgl.imr.client.DaemonInfo;
import cgl.imr.communication.ChainBcaster;
import cgl.imr.communication.CodeConstants;
import cgl.imr.communication.Connection;
import cgl.imr.communication.DataManager;
import cgl.imr.communication.JobManager;
import cgl.imr.util.DataHolder;

public class CMDProssesor implements Runnable {

	private static Logger logger = Logger.getLogger(CMDProssesor.class);
	private Socket socket;
	private ConcurrentHashMap<String, DataHolder> dataCache;
	private DaemonWorker worker;
	private int daemonNo;
	private Map<Integer, DaemonInfo> daemonInfo;
	private Executor clientTaskExecutor;

	public CMDProssesor(Socket sc,
			ConcurrentHashMap<String, DataHolder> dataCache,
			DaemonWorker worker, int daemonNo,
			Map<Integer, DaemonInfo> daemonInfo, Executor executor) {
		this.dataCache = dataCache;
		this.socket = sc;
		this.worker = worker;
		this.daemonNo = daemonNo;
		this.daemonInfo = daemonInfo;
		this.clientTaskExecutor = executor;
	}

	public void run() {
		DataOutputStream dout = null;
		DataInputStream dint = null;
		boolean exception = false;
		try {
			dout = new DataOutputStream(socket.getOutputStream());
			dint = new DataInputStream(socket.getInputStream());
			byte msgType = dint.readByte();
			if (msgType == TwisterConstants.DAEMON_QUIT_REQUEST) {
				worker.termintate();
				logger.info("TwisterDaemon No " + daemonNo
						+ " quitting gracefully.");
				System.exit(0);
			} else if (msgType == TwisterConstants.FAULT_DETECTION) {
				// we do nothing here
			} else if (msgType == TwisterConstants.DATA_REQUEST) {
				handleDataRequest(dout, dint);
			} else if (msgType == TwisterConstants.MAPPER_REQUEST) {
				handleReceiveMapperRequest(dout, dint);
			} else if (msgType == TwisterConstants.MAP_TASK_REQUEST) {
				handleReceiveMapTaskRequest(dout, dint);
			} else if (msgType == CodeConstants.CHAIN_ALTER_BCAST_START) {
				Connection conn = new Connection(dout, dint, socket);
				long jobID = conn.getDataInputDtream().readLong();
				int root = conn.getDataInputDtream().readInt();
				// start the job, there is only one handler
				ChainBcaster bcaster = null;
				boolean justCreated = false;
				synchronized (JobManager.getInstance()) {
					bcaster = (ChainBcaster) JobManager.getInstance()
							.get(jobID);
					if (bcaster == null) {
						// DataManager.getInstance().remove(jobID);
						DataManager.getInstance().freeBuffer();
						bcaster = new ChainBcaster(jobID, this.daemonNo, root);
						JobManager.getInstance().add(jobID, bcaster);
						justCreated = true;
					}
				}
				if (justCreated) {
					bcaster.handle(conn.getDataOutputStream(),
							conn.getDataInputDtream());
				}
			} else if(msgType == TwisterConstants.MSG_DESERIAL_START) {
				Connection conn = new Connection(dout, dint, socket);
				long jobID = conn.getDataInputDtream().readLong();
				List<byte[]> bytes = DataManager.getInstance().remove(jobID);
				TwisterMessage msg = new GenericBytesMessage(bytes.get(0));
				msg.readByte(); // skip the first byte
				this.worker.handleMemCacheInput(msg);
			} else if (msgType == TwisterConstants.CHAIN_BCAST_FORWARD_START) {
				moveDataInMultiChainBcast(dout, dint, true);
			} else if (msgType == TwisterConstants.CHAIN_BCAST_BACKWARD_START) {
				moveDataInMultiChainBcast(dout, dint, false);
			} else if (msgType == TwisterConstants.MST_SCATTER_START) {
				forwardDataInMSTInScatterAllGatherMSTBcast(dout, dint);
			} else if (msgType == TwisterConstants.MST_BCAST_FORWARD) {
				forwardDataInSubMSTinScatterAllGatherMSTBcast(dout, dint);
			} else if (msgType == TwisterConstants.BKT_SCATTER_START) {
				receiveAndCacheDataInScatterAllGatherBKTBcast(dout, dint);
			} else if (msgType == TwisterConstants.BKT_BCAST_START) {
				forwardDataInBKTInScatterAllGatherBKTBcast(dout, dint);
			} else if (msgType == TwisterConstants.BKT_BCAST_FORWARD) {
				forwardDataInSubBKTInScatterAllGatherBKTBcast(dout, dint);
			} else if (msgType == TwisterConstants.MST_BCAST) {
				bcastDataInMSTBcast(dout, dint);
			}
			closeConnection(dout, dint, socket);
		} catch (Exception e) {
			exception = true;
			e.printStackTrace();
		}
		if (exception) {
			closeConnection(dout, dint, socket);
			dout = null;
			dint = null;
			socket = null;
		}
	}

	/**
	 * try to close the socket connection
	 */
	private void closeConnection(DataOutputStream dout, DataInputStream dint,
			Socket socket) {
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

	private void handleDataRequest(DataOutputStream dout, DataInputStream dint)
			throws IOException {
		// get the key list
		int keyListSize = dint.readInt();
		Map<String, Boolean> downloadStatus = new HashMap<String, Boolean>();
		for (int i = 0; i < keyListSize; i++) {
			downloadStatus.put(dint.readUTF(), false);
		}
		boolean allDownloaded = false;
		int downloadCount;
		while (!allDownloaded) {
			for (String key : downloadStatus.keySet()) {
				boolean downloaded = downloadStatus.get(key);
				if (!downloaded) {
					DataHolder holder = dataCache.get(key);
					if (holder != null) {
						byte[] data = holder.getData();
						// key, data size, data text
						dout.writeUTF(key);
						dout.writeInt(data.length);
						// let's try to write data part by part
						int sendDataLen = 0;
						int len = TwisterConstants.BCAST_SENDRECV_UNIT; // 8 KB
						while (sendDataLen < data.length) {
							if ((sendDataLen + len) > data.length) {
								len = data.length - sendDataLen;
							}
							dout.write(data, sendDataLen, len);
							sendDataLen = sendDataLen + len;
							dout.flush();
						}
						// dout.flush();
						byte ack = dint.readByte();
						if (ack == TwisterConstants.DATA_REQUEST_ACK) {
							holder.decrementDownloadCount();
							if (holder.getDowloadCount() <= 0) {
								dataCache.remove(key);
							}
						}
						downloadStatus.put(key, true);
					} else {
						continue;
					}
				}
			}
			downloadCount = 0;
			for (boolean status : downloadStatus.values()) {
				if (status == true) {
					downloadCount++;
				}
			}
			if (downloadCount == downloadStatus.size()) {
				allDownloaded = true;
			}
		}
	}

	/**
	 * Receive Mapper request with Value object configured, we use this to
	 * process configureMapsInternal(List<Value> values), in case values are
	 * large
	 * 
	 * @param dout
	 * @param dint
	 * @throws IOException
	 */
	private void handleReceiveMapperRequest(DataOutputStream dout,
			DataInputStream dint) throws IOException {
		// we utilize receiveDataInAlltoAllBcast here
		// because the processing is the same
		byte[] bytes = receiveDataInHandlingRequest(dout, dint);
		// receive the message
		TwisterMessage msg = new GenericBytesMessage(bytes);
		this.worker.handleMapperRequest(msg);
	}

	/**
	 * Receive KeyValue pairs and start Map task, we use this to process
	 * runMapReduce with KeyValue pairs, in case they are large
	 * 
	 * @param dout
	 * @param dint
	 * @throws IOException
	 */
	private void handleReceiveMapTaskRequest(DataOutputStream dout,
			DataInputStream dint) throws IOException {
		// we utilize receiveDataInAlltoAllBcast here
		// because the processing is the same
		byte[] bytes = receiveDataInHandlingRequest(dout, dint);
		// receive the message
		TwisterMessage msg = new GenericBytesMessage(bytes);
		this.worker.handleMapTaskFromTCP(msg);
	}
	
	/**
	 * receive mapper request or map task request through TCP
	 * 
	 * @param dout
	 * @param dint
	 * @return
	 * @throws IOException
	 */
	private byte[] receiveDataInHandlingRequest(DataOutputStream dout,
			DataInputStream dint) throws IOException {
		byte[] buff = new byte[TwisterConstants.BCAST_SENDRECV_UNIT]; // 8 KB
		// allocate byte stream
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// get msg length and forward
		int msgLen = dint.readInt();
		int len = 0;
		int recvMSGLen = 0;
		// reading msg body and forward
		while ((len = dint.read(buff)) > 0) {
			bout.write(buff, 0, len);
			recvMSGLen = recvMSGLen + len;
			if (recvMSGLen == msgLen) {
				break;
			}
		}
		bout.flush();
		bout.close();
		return bout.toByteArray();
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////////
	/**
	 * forward message in broadcasting if msg type is broadcasting connect to
	 * next daemon, send msg type bcast, send msg size. then get message length,
	 * send to next read to buffer, send to next read finish send ack to last
	 * daemon wait for ack from next daemon ...
	 * 
	 * @param dout
	 * @param dint
	 * @throws TwisterException
	 */
	private void forwardDataInChainBcast(DataOutputStream dout,
			DataInputStream dint) throws TwisterException {
		// get next daemon
		int nextDaemonID = this.daemonNo + 1;
		Socket socketNextD = null;
		DataOutputStream doutNextD = null;
		DataInputStream dintNextD = null;
		boolean exception = false; // if exception happens in creating
									// connection to next Daemon
		do {
			DaemonInfo nextDaemon = this.daemonInfo.get(nextDaemonID);
			if (nextDaemon == null) {
				// This is the end of the chain
				exception = false;
				System.out.println("This is the end of the chain:  "
						+ this.daemonInfo.get(this.daemonNo).getIp());
			} else {
				/*
				 * try to connect to next daemon, if failed, go to pick the next
				 * next one.
				 */
				String host = nextDaemon.getIp();
				int port = nextDaemon.getPort();
				try {
					InetAddress addr = InetAddress.getByName(host);
					SocketAddress sockaddr = new InetSocketAddress(addr, port);
					socketNextD = new Socket();
					int timeoutMs = 0;
					socketNextD.connect(sockaddr, timeoutMs);
					doutNextD = new DataOutputStream(
							socketNextD.getOutputStream());
					dintNextD = new DataInputStream(
							socketNextD.getInputStream());
					exception = false;
				} catch (IOException e) {
					closeConnection(doutNextD, dintNextD, socketNextD);
					doutNextD = null;
					dintNextD = null;
					socketNextD = null;
					exception = true;
				}
			}
			/*
			 * if no exception, transfer data, if here exception happens, close
			 * the connection to the next daemon, and throw the exception, the
			 * connection to this daemon will also be closed.
			 */
			if (exception) {
				nextDaemonID = nextDaemonID + 1;
			} else {
				try {
					handleForwardingDataInChainBcast(dout, dint, doutNextD,
							dintNextD);
					// close connection to next daemon
					closeConnection(doutNextD, dintNextD, socketNextD);
				} catch (IOException e) {
					// try to close remote connection
					closeConnection(doutNextD, dintNextD, socketNextD);
					doutNextD = null;
					dintNextD = null;
					socketNextD = null;
					throw new TwisterException(
							"Exceptions in forwardDataInChainBcast", e);
				}
			}
		} while (exception);
		// print the daemon
		if (nextDaemonID != (this.daemonNo + 1)) {
			DaemonInfo nextDaemon = this.daemonInfo.get(nextDaemonID);
			if (nextDaemon != null) {
				System.out.println("the next daemon in bcast chain "
						+ this.daemonInfo.get(nextDaemonID).getIp());
			}
		}
	}

	/**
	 * the core function to do chain bcast
	 * 
	 * @param dout
	 * @param dint
	 * @param doutNextD
	 * @param dintNextD
	 * @throws IOException
	 */
	private void handleForwardingDataInChainBcast(DataOutputStream dout,
			DataInputStream dint, DataOutputStream doutNextD,
			DataInputStream dintNextD) throws IOException {
		// how many messages I will get
		int msgSize = dint.readInt();
		// forward bcast info to next daemon
		if (doutNextD != null) {
			doutNextD.writeByte(TwisterConstants.CHAIN_BCAST_START);
			doutNextD.writeInt(msgSize);
			doutNextD.flush();
		}
		// allocate buffer
		byte[] buff = new byte[TwisterConstants.BCAST_SENDRECV_UNIT];
		// allocate byte stream
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// receive each message and forward
		for (int i = 0; i < msgSize; i++) {
			// get msg length and forward
			int msgLen = dint.readInt();
			if (doutNextD != null) {
				doutNextD.writeInt(msgLen);
				doutNextD.flush();
			}
			int len = 0;
			int recvMSGLen = 0;
			// reading msg body and forward
			while ((len = dint.read(buff)) > 0) {
				bout.write(buff, 0, len);
				if (doutNextD != null) {
					doutNextD.write(buff, 0, len);
					doutNextD.flush();
				}
				recvMSGLen = recvMSGLen + len;
				if (recvMSGLen == msgLen) {
					break;
				}
			}
			bout.flush();
			bout.close();
			// spawn a new thread to handle MemCache
			TwisterMessage msg = new GenericBytesMessage(bout.toByteArray());
			this.worker.handleMemCacheInput(msg);
			bout.reset();
			// tell the sender, I get the msg, do ack
			dout.writeByte(TwisterConstants.BCAST_ACK);
			dout.flush();
			// wait for ack from the next daemon, see if I can continue
			// forwarding
			if (dintNextD != null) {
				byte ack = dintNextD.readByte();
				if (ack != TwisterConstants.BCAST_ACK) {
					System.out.println("NO ACK " + ack);
				}
			}
		} // end of big for loop
	}

	// ///////////////////////////////////////////////////////////////////////////////////

	/**
	 * multiple chains. And we provide "forward" chain, and "backward" chain
	 * 
	 * @param dout
	 * @param dint
	 * @throws TwisterException
	 */
	private void moveDataInMultiChainBcast(DataOutputStream dout,
			DataInputStream dint, boolean forward) throws TwisterException {
		// get next daemon
		int nextDaemonID = this.daemonNo;
		if (forward) {
			nextDaemonID = nextDaemonID + 1;
		} else {
			nextDaemonID = nextDaemonID - 1;
		}
		Socket socketNextD = null;
		DataOutputStream doutNextD = null;
		DataInputStream dintNextD = null;
		boolean exception = false; // if exception happens in creating
									// connection to next Daemon
		do {
			DaemonInfo nextDaemon = this.daemonInfo.get(nextDaemonID);
			if (nextDaemon == null) {
				/*
				 * This is the end of the chain ,it can be the daemon with the
				 * highest ID, or with the ID 0. If the id of next daemon is
				 * higher than the highest or lower than 0, it is out of the
				 * range.
				 */
				exception = false;
				/*
				 * System.out.println("This is the end of the chain: " +
				 * this.daemonInfo.get(this.daemonNo).getIp());
				 */
			} else {
				/*
				 * try to connect to next daemon, if failed, go to pick the next
				 * next one.
				 */
				String host = nextDaemon.getIp();
				int port = nextDaemon.getPort();
				try {
					InetAddress addr = InetAddress.getByName(host);
					SocketAddress sockaddr = new InetSocketAddress(addr, port);
					socketNextD = new Socket();
					int timeoutMs = 0;
					socketNextD.connect(sockaddr, timeoutMs);
					doutNextD = new DataOutputStream(
							socketNextD.getOutputStream());
					dintNextD = new DataInputStream(
							socketNextD.getInputStream());
					exception = false;
				} catch (IOException e) {
					closeConnection(doutNextD, dintNextD, socketNextD);
					doutNextD = null;
					dintNextD = null;
					socketNextD = null;
					exception = true;
				}
			}
			/*
			 * if no exception, transfer data, if here exception happens, close
			 * the connection to the next daemon, and throw the exception, the
			 * connection to this daemon will also be closed.
			 */
			if (exception) {
				if (forward) {
					nextDaemonID = nextDaemonID + 1;
				} else {
					nextDaemonID = nextDaemonID - 1;
				}
			} else {
				try {
					handleMovingDataInMultiChainBcast(dout, dint, doutNextD,
							dintNextD, forward);
					// close connection to next daemon
					closeConnection(doutNextD, dintNextD, socketNextD);
				} catch (IOException e) {
					// try to close remote connection
					closeConnection(doutNextD, dintNextD, socketNextD);
					doutNextD = null;
					dintNextD = null;
					socketNextD = null;
					throw new TwisterException(
							"Exceptions in moveDataInMultiChainBcast", e);
				}
			}
		} while (exception);
		if (forward) {
			if (nextDaemonID != (this.daemonNo + 1)) {
				System.out.println("the next daemon in bcast forward chain "
						+ this.daemonInfo.get(nextDaemonID).getIp());
			}
		} else {
			if (nextDaemonID != (this.daemonNo - 1)) {
				System.out.println("the next daemon in bcast backward chain "
						+ this.daemonInfo.get(nextDaemonID).getIp());
			}
		}
	}

	/**
	 * used for bcastRequestsAndReceiveResponsesXEx2.
	 * 
	 * New protocol agreement
	 * 
	 * @param dout
	 * @param dint
	 * @param doutNextD
	 * @param dintNextD
	 * @throws IOException
	 */
	private void handleMovingDataInMultiChainBcast(DataOutputStream dout,
			DataInputStream dint, DataOutputStream doutNextD,
			DataInputStream dintNextD, boolean forward) throws IOException {
		// move forward or backward bcast info to next daemon
		if (doutNextD != null) {
			if (forward) {
				doutNextD.writeByte(TwisterConstants.CHAIN_BCAST_FORWARD_START);
			} else {
				doutNextD
						.writeByte(TwisterConstants.CHAIN_BCAST_BACKWARD_START);
			}
			doutNextD.flush();
		}
		// allocate buffer
		byte[] buff = new byte[TwisterConstants.BCAST_SENDRECV_UNIT];
		// allocate byte stream
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// receive each message and forward
		for (;;) {
			// get msg length and forward
			int msgLen = dint.readInt();
			if (doutNextD != null) {
				doutNextD.writeInt(msgLen);
				doutNextD.flush();
			}
			// if msgLen == 0, it is an end tag
			if (msgLen == 0) {
				// do nothing
				/*
				dout.writeByte(TwisterConstants.BCAST_ACK);
				dout.flush();
				// waiting for the end ack from the next daemon
				if (dintNextD != null) {
					byte ack = dintNextD.readByte();
					if (ack != TwisterConstants.BCAST_ACK) {
						System.out.println("Not expected ACK " + ack);
					}
				}
				*/
				break; // if msgLen == 0, it is an end tag. break the loop
			} else {
				int len = 0;
				int recvMSGLen = 0;
				// reading msg body and forward
				while ((len = dint.read(buff)) > 0) {
					if (doutNextD != null) {
						doutNextD.write(buff, 0, len);
						doutNextD.flush();
					}
					bout.write(buff, 0, len);
					recvMSGLen = recvMSGLen + len;
					if (recvMSGLen == msgLen) {
						break;
					}
				}
				bout.flush();
				bout.close();
				// spawn a new thread to handle MemCache
				TwisterMessage msg = new GenericBytesMessage(bout.toByteArray());
				// TwisterMessage msg = new GenericBytesMessage(byteMSG);
				this.worker.handleMemCacheInput(msg);
				bout.reset();
				/*
				 * tell the sender, I get the msg, do ack. This should be done
				 * before the next few lines! Otherwise, you have to wait till
				 * the last daemon to get the data!
				 */
				dout.writeByte(TwisterConstants.BCAST_ACK);
				dout.flush();
				/*
				 * wait for ack from the next daemon, see if I can continue
				 * forwarding This is pipeline!
				 */
				if (dintNextD != null) {
					byte ack = dintNextD.readByte();
					if (ack != TwisterConstants.BCAST_ACK) {
						System.out.println("Not expected ACK " + ack);
					}
				}
			}
		} // end of big for loop
	}

	// //////////////////////////////////////////////////////////////////////////
	
	/**
	 * receive message in All-to-All bcast
	 * 
	 * @param dout
	 * @param dint
	 * @return
	 * @throws IOException
	 */
	private byte[] receiveDataInScatterAllGatherBKTBcast(DataOutputStream dout,
			DataInputStream dint) throws IOException {
		byte[] buff = new byte[TwisterConstants.BCAST_SENDRECV_UNIT]; // 8 KB
		// allocate byte stream
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// get msg length and forward
		int msgLen = dint.readInt();
		int len = 0;
		int recvMSGLen = 0;
		// reading msg body and forward
		while ((len = dint.read(buff)) > 0) {
			bout.write(buff, 0, len);
			recvMSGLen = recvMSGLen + len;
			if (recvMSGLen == msgLen) {
				break;
			}
		}
		bout.flush();
		bout.close();
		// tell the sender, I get the msg, do ack
		/*
		 * dout.writeByte(TwisterConstants.BCAST_ACK); dout.flush();
		 */
		return bout.toByteArray();
	}
	
	private void receiveAndCacheDataInScatterAllGatherBKTBcast(DataOutputStream dout,
			DataInputStream dint) throws IOException {
		byte[] buff = new byte[TwisterConstants.BCAST_SENDRECV_UNIT]; // 8 KB
		// allocate byte stream
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// get msg length and forward
		int msgLen = dint.readInt();
		long tag = dint.readLong();
		int len = 0;
		int recvMSGLen = 0;
		// reading msg body and forward
		while ((len = dint.read(buff)) > 0) {
			bout.write(buff, 0, len);
			recvMSGLen = recvMSGLen + len;
			if (recvMSGLen == msgLen) {
				break;
			}
		}
		bout.flush();
		bout.close();
		String bcastDataKeyBase = tag+"";
		String id = UUIDGenerator.getInstance().generateTimeBasedUUID()
				.toString();
		String bcastDataKey = bcastDataKeyBase + ":" + id;
		byte[] bytes = bout.toByteArray();
		this.dataCache.put(bcastDataKey, new DataHolder(bytes, 1));
		// tell the sender, I get the msg, do ack
		// this is required to make sure if the data is cached
		dout.writeByte(TwisterConstants.BCAST_ACK);
		dout.flush();
		// spawn a new thread to handle MemCache
		TwisterMessage msg = new GenericBytesMessage(bytes);
		this.worker.handleMemCacheInput(msg);
	}

	/**
	 * receive data, process data, bcast data to all
	 * 
	 * @throws IOException
	 */
	private void forwardDataInBKTInScatterAllGatherBKTBcast(
			DataOutputStream dout, DataInputStream dint) throws IOException {
		String tag = dint.readUTF();
		int destDaemonID = (this.daemonNo + 1) % this.daemonInfo.size();
		if (destDaemonID != this.daemonNo) {
			for (String key : this.dataCache.keySet()) {
				if (key.startsWith(tag)) {
					HandleForwardingDataInBKTInScatterAllGatherBKTBcastThread handler = new HandleForwardingDataInBKTInScatterAllGatherBKTBcastThread(
							this.dataCache.get(key).getData(), this.daemonNo,
							destDaemonID);
					clientTaskExecutor.execute(handler);
				}
			}
		}
		this.dataCache.clear();
		// dout.writeByte(TwisterConstants.BCAST_ACK);
		// dout.flush();
	}

	private class HandleForwardingDataInBKTInScatterAllGatherBKTBcastThread
			implements Runnable {
		// the message for sending
		private byte[] bytes;
		private int startDaemonID;
		// the ID in workingDaemons
		private int destDaemonID;

		HandleForwardingDataInBKTInScatterAllGatherBKTBcastThread(byte[] msg,
				int startDaemonID, int destDaemonID) {
			this.bytes = msg;
			this.startDaemonID = startDaemonID;
			this.destDaemonID = destDaemonID;
		}

		public void run() {
			int nextDaemonID = this.destDaemonID;
			boolean exception = false;
			Socket socket = null;
			DataOutputStream dout = null;
			DataInputStream dint = null;
			int count = 0;
			/*
			 * get next daemon, the des. daemon ID under the range 0 ..
			 * daemonInfo.size() -1, it may not work, but it should not be null
			 */
			do {
				DaemonInfo nextDaemon = daemonInfo.get(nextDaemonID);
				if (nextDaemon == null) {
					exception = true;
				} else {
					String host = nextDaemon.getIp();
					int port = nextDaemon.getPort();
					try {
						// connect dest daemon
						InetAddress addr = InetAddress.getByName(host);
						SocketAddress sockaddr = new InetSocketAddress(addr,
								port);
						socket = new Socket();
						int timeoutMs = 0;
						socket.connect(sockaddr, timeoutMs);
						dout = new DataOutputStream(socket.getOutputStream());
						dint = new DataInputStream(socket.getInputStream());
						// here we send bytes, is only the body of MemCacheInput
						int len = bytes.length;
						// start broadcasting
						dout.writeByte(TwisterConstants.BKT_BCAST_FORWARD);
						// tell the nextDaemon the BKT start.
						dout.writeInt(startDaemonID);
						// send length
						dout.writeInt(len);
						dout.flush();
						// send message, no need to send the message type
						int start = 0;
						int sendUnit = TwisterConstants.BCAST_SENDRECV_UNIT;
						while ((start + sendUnit) <= bytes.length) {
							dout.write(bytes, start, sendUnit);
							start = start + sendUnit;
							dout.flush();
						}
						dout.write(bytes, start, bytes.length - start);
						dout.flush();
						// wait for ack for next message
						/*
						 * byte ack = dint.readByte(); if (ack !=
						 * TwisterConstants.BCAST_ACK) {
						 * System.out.println("Not expected ACK " + ack); }
						 */
						// close resources
						closeConnection(dout, dint, socket);
						exception = false;
					} catch (IOException e) {
						closeConnection(dout, dint, socket);
						dout = null;
						dint = null;
						socket = null;
						exception = true;
					}
				}
				// retry, if still not work then change to next daemon...
				if (exception) {
					System.out
							.println("meet exceptions in BKT forwarding! Daemon: "
									+ daemonNo);
					if (count < TwisterConstants.SEND_TRIES) {
						count++;
					} else {
						nextDaemonID = (nextDaemonID + 1) % daemonInfo.size();
						if (nextDaemonID == this.startDaemonID) {
							break;
						}
					}
				}
			} while (exception);
			if (this.destDaemonID != nextDaemonID) {
				System.out.println("Daemon: " + daemonNo + ". Next daemon ID: "
						+ nextDaemonID + " IP: "
						+ daemonInfo.get(nextDaemonID).getIp());
			}
		}
	}

	/**
	 * receive data which are bcasted by other daemon
	 * 
	 * @param dout
	 * @param dint
	 * @throws IOException
	 */
	private void forwardDataInSubBKTInScatterAllGatherBKTBcast(
			DataOutputStream dout, DataInputStream dint) throws IOException {
		int bktStartDaemon = dint.readInt();
		byte[] bytes = receiveDataInScatterAllGatherBKTBcast(dout, dint);
		int destDaemonID = (this.daemonNo + 1) % this.daemonInfo.size();
		// we check here again, make sure if we can end the circling
		if (destDaemonID != bktStartDaemon) {
			HandleForwardingDataInBKTInScatterAllGatherBKTBcastThread thread = new HandleForwardingDataInBKTInScatterAllGatherBKTBcastThread(
					bytes, bktStartDaemon, destDaemonID);
			this.clientTaskExecutor.execute(thread);
		}
		// spawn a new thread to handle MemCache
		TwisterMessage msg = new GenericBytesMessage(bytes);
		this.worker.handleMemCacheInput(msg);
	}

	// ///////////////////////////////////////////////////////////////////////////////

	/**
	 * receive message in All-to-All bcast
	 * 
	 * @param dout
	 * @param dint
	 * @return
	 * @throws IOException
	 */
	private byte[] receiveDataInScatterAllGatherMSTBcast(DataOutputStream dout,
			DataInputStream dint) throws IOException {
		byte[] buff = new byte[TwisterConstants.BCAST_SENDRECV_UNIT]; // 8 KB
		// allocate byte stream
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// get msg length and forward
		int msgLen = dint.readInt();
		int len = 0;
		int recvMSGLen = 0;
		// reading msg body and forward
		while ((len = dint.read(buff)) > 0) {
			bout.write(buff, 0, len);
			recvMSGLen = recvMSGLen + len;
			if (recvMSGLen == msgLen) {
				break;
			}
		}
		bout.flush();
		bout.close();
		// tell the sender, I get the msg, do ack
		/*
		 * dout.writeByte(TwisterConstants.BCAST_ACK); dout.flush();
		 */
		return bout.toByteArray();
	}

	/**
	 * let's do MSTBcast for each part alternatively receive the data, start a
	 * thread to process it send in MST, send ALL_BCAST_FORWARD information send
	 * root, left, right, each starts from a relative start position daemonNo+
	 * 0... damonNo + size-1
	 * 
	 * @throws IOException
	 */
	private void forwardDataInMSTInScatterAllGatherMSTBcast(
			DataOutputStream dout, DataInputStream dint) throws IOException {
		byte[] bytes = receiveDataInScatterAllGatherMSTBcast(dout, dint);
		HandleForwardDataInMSTInScatterAllGatherMSTBcastThread thread = new HandleForwardDataInMSTInScatterAllGatherMSTBcastThread(
				bytes);
		this.clientTaskExecutor.execute(thread);
		// read message and handle MemCache
		TwisterMessage msg = new GenericBytesMessage(bytes);
		this.worker.handleMemCacheInput(msg);
	}

	/**
	 * the core thread to bcast the scattered data in MST
	 * 
	 * @author zhangbj
	 * 
	 */
	private class HandleForwardDataInMSTInScatterAllGatherMSTBcastThread
			implements Runnable {
		// the message for sending
		private byte[] bytes;

		private HandleForwardDataInMSTInScatterAllGatherMSTBcastThread(
				byte[] msg) {
			bytes = msg;
		}

		@Override
		public void run() {
			/*
			 * Here root is the daemon itself, left, right information is a
			 * relative position, 0 actually is daemonNo, right actually is
			 * (daemonNo+daemonInfo.size() - 1)%daemonInfo.size()
			 */
			int root = 0;
			int left = 0;
			int right = daemonInfo.size() - 1;
			int mid;
			int dest;
			// start connection to next daemon
			Socket socketNextD = null;
			DataOutputStream doutNextD = null;
			DataInputStream dintNextD = null;
			boolean exception = false;
			while (left != right) {
				// calculate mid and dest
				mid = (int) Math.floor((left + right) / (double) 2);
				dest = right; // this daemonNo is considered as 0, so the dest
								// should be right
				// here we get the real dest
				DaemonInfo nextDaemon = daemonInfo.get((daemonNo + dest)
						% daemonInfo.size());
				if (nextDaemon == null) {
					exception = true;
				} else {
					// connect and send data, if we meet error, we will go to
					// next daemon
					String host = nextDaemon.getIp();
					int port = nextDaemon.getPort();
					try {
						InetAddress addr = InetAddress.getByName(host);
						SocketAddress sockaddr = new InetSocketAddress(addr,
								port);
						socketNextD = new Socket();
						int timeoutMs = 0;
						socketNextD.connect(sockaddr, timeoutMs);
						doutNextD = new DataOutputStream(
								socketNextD.getOutputStream());
						dintNextD = new DataInputStream(
								socketNextD.getInputStream());
						// start broadcasting
						doutNextD.writeByte(TwisterConstants.MST_BCAST_FORWARD);
						// send this daemonNo, root, left, right information
						doutNextD.writeInt(daemonNo);
						doutNextD.writeInt(root);
						doutNextD.writeInt(left);
						doutNextD.writeInt(right);
						doutNextD.flush();
						// send data request body
						int len = this.bytes.length;
						// send length
						doutNextD.writeInt(len);
						doutNextD.flush();
						// send message, no need to send the message type
						// doutNextD.write(bytes, 0, len);
						int start = 0;
						int sendUnit = TwisterConstants.BCAST_SENDRECV_UNIT;
						while ((start + sendUnit) <= this.bytes.length) {
							doutNextD.write(this.bytes, start, sendUnit);
							start = start + sendUnit;
							doutNextD.flush();
						}
						doutNextD.write(this.bytes, start, this.bytes.length
								- start);
						doutNextD.flush();
						// wait for ack for next message
						/*
						 * byte ack = dintNextD.readByte(); if (ack !=
						 * TwisterConstants.BCAST_ACK) {
						 * System.out.println("Not expected ACK " + ack); }
						 */
						// close resources
						closeConnection(doutNextD, dintNextD, socketNextD);
						exception = false;
					} catch (IOException e) {
						closeConnection(doutNextD, dintNextD, socketNextD);
						doutNextD = null;
						dintNextD = null;
						socketNextD = null;
						exception = true;
					}
					/*
					 * update new range for broadcasting. This is root,
					 * considered as 0 in ID, so it is always in left part
					 */
					if (exception) {
						right = right - 1;
					} else {
						right = mid;
					}
				}
			}
		}
	}

	/**
	 * process "forward" command, choose the appropriate range to start next
	 * sending as a root.
	 * 
	 * @param dout
	 * @param dint
	 * @throws IOException
	 */
	private void forwardDataInSubMSTinScatterAllGatherMSTBcast(
			DataOutputStream dout, DataInputStream dint) throws IOException {
		int msgDaemonID = dint.readInt();
		int root = dint.readInt();
		int left = dint.readInt();
		int right = dint.readInt();
		// get the data
		byte[] bytes = receiveDataInScatterAllGatherMSTBcast(dout, dint);
		HandleForwardDataInSubMSTinScatterAllGatherMSTBcastThread thread = new HandleForwardDataInSubMSTinScatterAllGatherMSTBcastThread(
				bytes, msgDaemonID, root, left, right);
		this.clientTaskExecutor.execute(thread);
		TwisterMessage msg = new GenericBytesMessage(bytes);
		worker.handleMemCacheInput(msg);
	}

	/**
	 * The thread used in function above
	 * 
	 * @author zhangbj
	 * 
	 */
	private class HandleForwardDataInSubMSTinScatterAllGatherMSTBcastThread
			implements Runnable {
		// the message for sending
		private byte[] bytes;
		private int msgDaemonID;
		private int root;
		private int left;
		private int right;

		private HandleForwardDataInSubMSTinScatterAllGatherMSTBcastThread(
				byte[] msg, int daemonID, int r, int lf, int rt) {
			bytes = msg;
			msgDaemonID = daemonID;
			root = r;
			left = lf;
			right = rt;
		}

		@Override
		public void run() {
			// calculate the dest, the relative ID of the current daemon
			int mid = (int) Math.floor((left + right) / (double) 2);
			int dest = right; // this is a default value
			if (root > mid) {
				dest = left;
			}
			// I get into this method because I am the dest
			int me = dest;
			/*
			 * choose next MSTBcast routine, since the current daemon is a dest
			 * we will be in (dest, left, mid) or (dest, mid+1, right) routine
			 */
			if (me <= mid && root > mid) {
				root = dest;
				right = mid;
			} else if (me > mid && root <= mid) {
				root = dest;
				left = mid + 1;
			}
			// start connection to next daemon
			Socket socketNextD = null;
			DataOutputStream doutNextD = null;
			DataInputStream dintNextD = null;
			boolean exception = false;
			while (left != right) {
				// update mid and dest
				mid = (int) Math.floor((left + right) / (double) 2);
				if (root <= mid) {
					dest = right;
				} else {
					dest = left;
				}
				// get real dest
				DaemonInfo nextDaemon = daemonInfo.get((msgDaemonID + dest)
						% daemonInfo.size());
				if (nextDaemon == null) {
					exception = true;
				} else {
					// connect and send data, if we meet error, we will go to
					// next
					// daemon
					String host = nextDaemon.getIp();
					int port = nextDaemon.getPort();
					try {
						InetAddress addr = InetAddress.getByName(host);
						SocketAddress sockaddr = new InetSocketAddress(addr,
								port);
						socketNextD = new Socket();
						int timeoutMs = 0;
						socketNextD.connect(sockaddr, timeoutMs);
						doutNextD = new DataOutputStream(
								socketNextD.getOutputStream());
						dintNextD = new DataInputStream(
								socketNextD.getInputStream());
						// start broadcasting
						doutNextD.writeByte(TwisterConstants.MST_BCAST_FORWARD);
						// send root, left, right information
						doutNextD.writeInt(msgDaemonID);
						doutNextD.writeInt(root);
						doutNextD.writeInt(left);
						doutNextD.writeInt(right);
						doutNextD.flush();
						// send data request body
						int len = bytes.length;
						// send length
						doutNextD.writeInt(len);
						doutNextD.flush();
						// send message, no need to send the message type
						// doutNextD.write(bytes, 0, len);
						int start = 0;
						int sendUnit = TwisterConstants.BCAST_SENDRECV_UNIT;
						while ((start + sendUnit) <= bytes.length) {
							doutNextD.write(bytes, start, sendUnit);
							start = start + sendUnit;
							doutNextD.flush();
						}
						doutNextD.write(bytes, start, bytes.length - start);
						doutNextD.flush();
						// wait for ack for next message
						/*
						 * byte ack = dintNextD.readByte(); if (ack !=
						 * TwisterConstants.BCAST_ACK) {
						 * System.out.println("Not expected ACK " + ack); }
						 */
						// close resources
						closeConnection(doutNextD, dintNextD, socketNextD);
						exception = false;
					} catch (IOException e) {
						closeConnection(doutNextD, dintNextD, socketNextD);
						doutNextD = null;
						dintNextD = null;
						socketNextD = null;
						exception = true;
					}
					// update new range for broadcasting
					if (exception) {
						if (dest == left) {
							left = left + 1;
						} else {
							right = right - 1;
						}
					} else {
						/*
						 * as a root, I won't be dest again, choose root routine
						 * to go on sending. calculate new range, left and right
						 */
						if (me <= mid && root <= mid) {
							right = mid;
						} else if (me > mid && root > mid) {
							left = mid + 1;
						}
					}
				}
			}
		}
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// MSTPosition is used in MSTBcast
	private class MSTPosition {
		private int root;
		private int left;
		private int right;

		MSTPosition(int root, int left, int right) {
			this.root = root;
			this.left = left;
			this.right = right;
		}

		@SuppressWarnings("unused")
		void setRoot(int root) {
			this.root = root;
		}

		int getRoot() {
			return root;
		}

		@SuppressWarnings("unused")
		void setLeft(int left) {
			this.left = left;
		}

		int getLeft() {
			return left;
		}

		@SuppressWarnings("unused")
		void setRight(int right) {
			this.right = right;
		}

		int getRight() {
			return right;
		}
	}

	/**
	 * send a single message in MST
	 * 
	 * @throws IOException
	 */
	private void bcastDataInMSTBcast(DataOutputStream dout, DataInputStream dint)
			throws IOException {
		int root = dint.readInt();
		int left = dint.readInt();
		int right = dint.readInt();
		MSTPosition p = new MSTPosition(root, left, right);
		ArrayList<byte[]> messages = receiveDataInMSTBcastAlter(dout, dint);
		bcastDataInSubMSTAlter(messages, p);
		// spawn a new thread to handle MemCache
		for (byte[] message : messages) {
			TwisterMessage msg = new GenericBytesMessage(message);
			this.worker.handleMemCacheInput(msg);
		}
	}

	/**
	 * this function does receiving only,
	 * 
	 * @param dout
	 * @param dint
	 * @return
	 * @throws IOException
	 */
	private ArrayList<byte[]> receiveDataInMSTBcastAlter(DataOutputStream dout,
			DataInputStream dint) throws IOException {
		// array of bytes to hold message received
		ArrayList<byte[]> messages = new ArrayList<byte[]>();
		// how many messages I will get
		int msgSize = dint.readInt();
		// allocate buffer
		byte[] buff = new byte[TwisterConstants.BCAST_SENDRECV_UNIT]; // 8 KB to
																		// 64
																		// KB?
		// allocate byte stream
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// receive each message and forward
		for (int i = 0; i < msgSize; i++) {
			// get msg length and forward
			int msgLen = dint.readInt();
			int len = 0;
			int recvMSGLen = 0;
			// reading msg body and forward
			while ((len = dint.read(buff)) > 0) {
				bout.write(buff, 0, len);
				recvMSGLen = recvMSGLen + len;
				if (recvMSGLen == msgLen) {
					break;
				}
			}
			bout.flush();
			bout.close();
			messages.add(bout.toByteArray());
			bout.reset();
			// tell the sender, I get the msg
			dout.writeByte(TwisterConstants.BCAST_ACK);
			dout.flush();
		} // end of big for loop
		return messages;
	}

	/**
	 * do sending as root
	 * 
	 * @param messages
	 * @param p
	 */
	private void bcastDataInSubMSTAlter(ArrayList<byte[]> messages,
			MSTPosition p) {
		/*
		 * choose the appropriate range to start next sending as a root.
		 */
		int root = p.getRoot();
		int left = p.getLeft();
		int right = p.getRight();
		int mid = (int) Math.floor((left + right) / (double) 2);
		int dest = this.daemonNo;
		/*
		 * choose next MSTBcast routine, since the current daemon is a dest we
		 * will be in (dest, left, mid) or (dest, mid+1, right) routine
		 */
		if (this.daemonNo <= mid && root <= mid) {
			right = mid;
		} else if (this.daemonNo <= mid && root > mid) {
			root = dest;
			right = mid;
		} else if (this.daemonNo > mid && root <= mid) {
			root = dest;
			left = mid + 1;
		} else if (this.daemonNo > mid && root > mid) {
			left = mid + 1;
		}
		// update mid and dest
		mid = (int) Math.floor((left + right) / (double) 2);
		if (root <= mid) {
			dest = right;
		} else {
			dest = left;
		}
		/*
		 * this process is similar as the sending in driver, get dest daemon, if
		 * there is exception, skip the sending
		 */
		// start connection to next daemon
		Socket socketNextD = null;
		DataOutputStream doutNextD = null;
		DataInputStream dintNextD = null;
		boolean exception = false;
		while (left != right) {
			DaemonInfo nextDaemon = this.daemonInfo.get(dest);
			if (nextDaemon == null) {
				exception = true;
			} else {
				// connect and send data, if we meet error, we will go to next
				// daemon
				String host = nextDaemon.getIp();
				int port = nextDaemon.getPort();
				try {
					InetAddress addr = InetAddress.getByName(host);
					SocketAddress sockaddr = new InetSocketAddress(addr, port);
					socketNextD = new Socket();
					int timeoutMs = 0;
					socketNextD.connect(sockaddr, timeoutMs);
					doutNextD = new DataOutputStream(
							socketNextD.getOutputStream());
					dintNextD = new DataInputStream(
							socketNextD.getInputStream());
					// check who is next daemon for sending
					/*
					 * System.out.println("Next daemon ID: " + dest + " IP: " +
					 * this.daemonInfo.get(dest).getIp());
					 */
					// start broadcasting
					doutNextD.writeByte(TwisterConstants.MST_BCAST);
					// send root, left, right information
					doutNextD.writeInt(root);
					doutNextD.writeInt(left);
					doutNextD.writeInt(right);
					// send message size
					doutNextD.writeInt(messages.size());
					doutNextD.flush();
					for (byte[] bytes : messages) {
						// send data request body
						int len = bytes.length;
						// send length
						doutNextD.writeInt(len);
						doutNextD.flush();
						// send message, no need to send the message type
						doutNextD.write(bytes, 0, len);
						doutNextD.flush();
						// wait for ack for next message
						byte ack = dintNextD.readByte();
						if (ack != TwisterConstants.BCAST_ACK) {
							System.out.println("NO ACK " + ack);
							break;
						}
					}
					// close resources
					closeConnection(doutNextD, dintNextD, socketNextD);
					exception = false;
				} catch (IOException e) {
					closeConnection(doutNextD, dintNextD, socketNextD);
					doutNextD = null;
					dintNextD = null;
					socketNextD = null;
					exception = true;
				}
				// update new range for broadcasting
				if (exception) {
					if (dest == left) {
						left = left + 1;
					} else {
						right = right - 1;
					}
				} else {
					/*
					 * as a root, I won't be dest again, choose root routine to
					 * go on sending. calculate new range, left and right
					 */
					if (this.daemonNo <= mid && root <= mid) {
						right = mid;
					} else if (this.daemonNo > mid && root > mid) {
						left = mid + 1;
					}
				}
				// update new mid and dest
				mid = (int) Math.floor((left + right) / (double) 2);
				if (root <= mid) {
					dest = right;
				} else {
					dest = left;
				}
			}
		}
	}
}
