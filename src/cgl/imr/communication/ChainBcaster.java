package cgl.imr.communication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

public class ChainBcaster implements CollectiveComm {
	// bcast job description
	private long jobID;
	private int nodeID;
	private int root;
	private Config config;

	public ChainBcaster(long jobID, int nodeID, int root) throws Exception {
		this.jobID = jobID;
		this.nodeID = nodeID;
		this.root = root;
		this.config = Config.getInstance();
	}

	public boolean start() {
		if (this.root != -1 || this.nodeID != -1) {
			// may throw exception
			return false;
		}
		// take out the data
		List<byte[]> bytes = DataManager.getInstance().remove(this.jobID);
		if (bytes == null || bytes.size() != 1) {
			return false;
		}
		// add for response
		List<byte[]> responses = new ArrayList<byte[]>();
		DataManager.getInstance().put(this.jobID, responses);
		// start
		int nextDaemonID = nodeID + 1;
		Connection conn = null;
		boolean exception = false;
		do {
			NodeInfo nextDaemon = config.getNodesInfo().get(nextDaemonID);
			// meet the end, break;
			if (nextDaemon == null) {
				exception = false;
				break;
			} else {
				String host = nextDaemon.getIp();
				int port = nextDaemon.getPort();
				try {
					conn = new Connection(host, port, 0);
					exception = false;
				} catch (Exception e) {
					System.out.println("exception in connecting " + nextDaemon);
					e.printStackTrace();
					conn = null;
					exception = true;
				}
			}
			if (exception) {
				nextDaemonID = nextDaemonID + 1;
			} else {
				try {
					send(bytes.get(0), conn.getDataOutputStream(),
							conn.getDataInputDtream());
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
					conn.close();
					return false;
				}
			}
		} while (exception);
		waitForCompletion();
		// remove all responses
		// DataManager.getInstance().remove(this.jobID);
		return true;
	}

	private void send(byte[] bytes, DataOutputStream dout, DataInputStream din)
			throws Exception {
		int len = bytes.length;
		// Initial meta data to create ChainBcast object at remote
		dout.writeByte(CodeConstants.CHAIN_ALTER_BCAST_START);
		dout.writeLong(this.jobID);
		dout.writeInt(this.root);
		dout.writeInt(len);
		dout.flush();
		// send content
		int start = 0;
		int sendUnit = CodeConstants.SENDRECV_UNIT;
		while ((start + sendUnit) <= len) {
			dout.write(bytes, start, sendUnit);
			start = start + sendUnit;
			dout.flush();
		}
		if (start < len) {
			dout.write(bytes, start, len - start);
			dout.flush();
		}
	}

	private void waitForCompletion() {
		// System.out.println("DataManager Job Data Size: "
		// + DataManager.getInstance().get(this.jobID).size());
		while (DataManager.getInstance().get(this.jobID).size() != (this.config
				.getNodesInfo().size() - 1)) {
			// System.out.println("Loop DataManager Job Data Size: "
			// + DataManager.getInstance().get(this.jobID).size());
		}
	}

	/**
	 * There are multiple handler
	 */
	public void handle(DataOutputStream dout, DataInputStream din)
			throws Exception {
		// get next daemon
		int nextDaemonID = this.nodeID + 1;
		Connection nextConn = null;
		boolean exception = false;
		do {
			NodeInfo nextDaemon = this.config.getNodesInfo().get(nextDaemonID);
			// meet the end
			if (nextDaemon == null) {
				// may cause nextConn be null
				exception = false;
			} else {
				String host = nextDaemon.getIp();
				int port = nextDaemon.getPort();
				try {
					nextConn = new Connection(host, port, 0);
					exception = false;
				} catch (Exception e) {
					e.printStackTrace();
					exception = true;
				}
			}
			if (exception) {
				nextDaemonID = nextDaemonID + 1;
			} else {
				try {
					receive(dout, din, nextConn);
					if (nextConn != null) {
						nextConn.close();
					}
				} catch (Exception e) {
					if (nextConn != null) {
						nextConn.close();
					}
					throw new Exception(
							"Exceptions in sending data in ChainBcast on "
									+ this.nodeID, e);
				}
			}
		} while (exception);
	}

	private void receive(DataOutputStream dout, DataInputStream din,
			Connection nextConn) throws Exception {
		DataOutputStream doutNextD = null;
		// check if there is the next daemon
		if (nextConn != null) {
			doutNextD = nextConn.getDataOutputStream();
		}
		// get msg length
		int msgLen = din.readInt();
		// to construct MultiChainBroadcaster at remote
		if (doutNextD != null) {
			doutNextD.writeByte(CodeConstants.CHAIN_ALTER_BCAST_START);
			doutNextD.writeLong(this.jobID);
			doutNextD.writeInt(this.root);
			doutNextD.writeInt(msgLen);
			doutNextD.flush();
		}
		// leave some space
		byte[] msgBuffer = null;
		msgBuffer = DataManager.getInstance().allocateBuffer(
				msgLen + CodeConstants.SENDRECV_UNIT);
		if (msgBuffer == null) {
			System.out.println("Didn't get the buffer.");
			msgBuffer = new byte[msgLen + CodeConstants.SENDRECV_UNIT];
		}
		int len = 0;
		int recvMSGLen = 0;
		// it may not read the whole 8192 byte
		// even there is still data left
		while ((len = din.read(msgBuffer, recvMSGLen,
				CodeConstants.SENDRECV_UNIT)) > 0) {
			if (doutNextD != null) {
				doutNextD.write(msgBuffer, recvMSGLen, len);
				doutNextD.flush();
			}
			recvMSGLen = recvMSGLen + len;
			if (recvMSGLen == msgLen) {
				break;
			}
		}
		// there is no other msgBuffer in other thread or in this thread
		// because this is the only single chain and msg chunk
		DataManager.getInstance().put(this.jobID, msgBuffer);
		JobManager.getInstance().remove(this.jobID);
		notifyForCompletion();
		/*
		 * The following line is important, adding this line can improve
		 * performance by 1.2 seconds on bcasting 1 GB on 100+ nodes. The reason
		 * is still unknown.
		 */
		/*
		 * A possible reason is that, if we hold 1 GB data memory, every time
		 * for bacsting, the code above has to request another 1 GB for the new
		 * bcast data. then JVM is always has 2 GB memory. But if we delete, JVM
		 * can release the first 1GB and reuse it for another 1GB in next bcast
		 * so JVM is always running at 1GB level. Memory is important!!!! JVM
		 * with smaller memory runs faster
		 */
		/*
		 * New observation shows that since we use 2 GB, a small xms setting
		 * could affect the performance if we enlarge the xms setting to 8 GB,
		 * we can accelerate the performance
		 */
		// DataManager.getInstance().remove(this.jobID);
		/*
		List<byte[]> bytes = DataManager.getInstance().remove(this.jobID);
		byte[] byteArray = bytes.get(0);
		if (byteArray[byteArray.length - CodeConstants.SENDRECV_UNIT - 1] != 1) {
			System.out.println("The end is not correct" + byteArray.length);
		}
		*/
	}

	/**
	 * seems there could be failure in connecting to driver for small data
	 * sneding, let's retry
	 */
	private void notifyForCompletion() {
		// send a ACK to the root
		Connection conn = null;
		boolean exception = false;
		int count = 0;
		do {
			try {
				conn = new Connection(this.config.getNodesInfo().get(this.root)
						.getIp(), this.config.getNodesInfo().get(this.root)
						.getPort(), 0);
				exception = false;
			} catch (Exception e) {
				if (conn != null) {
					conn.close();
				}
				conn = null;
				count++;
				if (count == CodeConstants.RETRY_COUNT) {
					e.printStackTrace();
					return;
				}
				exception = true;
			}
			if (exception) {
				try {
					Thread.sleep(CodeConstants.SLEEP_COUNT);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} while (exception);
		try {
			DataOutputStream dout = conn.getDataOutputStream();
			dout.writeByte(CodeConstants.ACK);
			dout.writeLong(this.jobID);
			dout.flush();
			conn.close();
		} catch (Exception e) {
			conn.close();
			e.printStackTrace();
		}
	}
}
