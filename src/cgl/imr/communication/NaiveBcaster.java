package cgl.imr.communication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class NaiveBcaster implements CollectiveComm {
	// bcast job description
	private long jobID;
	private int nodeID;
	private int root;
	private Config config;

	public NaiveBcaster(long jobID, int nodeID, int root) throws Exception {
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
		// for loop send to one node by one node
		int curDaemonID = 0;
		Connection conn = null;
		int retryCount = 0;
		boolean exception = false;
		boolean fault = false;
		while (true) {
			NodeInfo nextDaemon = config.getNodesInfo().get(curDaemonID);
			if (nextDaemon == null) {
				break;
			}
			String host = nextDaemon.getIp();
			int port = nextDaemon.getPort();
			try {
				conn = new Connection(host, port, 0);
				send(bytes.get(0), conn.getDataOutputStream(),
						conn.getDataInputDtream());
				exception = false;
			} catch (Exception e) {
				System.out
						.println("exception in sending data to " + nextDaemon);
				e.printStackTrace();
				if (conn != null) {
					conn.close();
				}
				conn = null;
				exception = true;
			}
			if (exception) {
				retryCount++;
				if (retryCount == CodeConstants.RETRY_COUNT) {
					retryCount = 0;
					curDaemonID++;
					fault = true;
				}
				// sleep for a while and retest
				try {
					Thread.sleep(CodeConstants.SLEEP_COUNT);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				curDaemonID++;
			}
		}
		if (fault) {
			return false;
		}
		return true;
	}

	private void send(byte[] bytes, DataOutputStream dout, DataInputStream din)
			throws Exception {
		int len = bytes.length;
		// Initial meta data to create ChainBcast object at remote
		dout.writeByte(CodeConstants.NAIVE_BCAST_START);
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
		dout.flush();
	}
	
	public boolean startWithMultiThreads() {
		if (this.root != -1 || this.nodeID != -1) {
			// may throw exception
			return false;
		}
		// take out the data
		List<byte[]> bytes = DataManager.getInstance().remove(this.jobID);
		if (bytes == null || bytes.size() != 1) {
			return false;
		}
		// remove driver from nodes info
		int availableWorkers = Runtime.getRuntime().availableProcessors();
		ExecutorService executor = Executors
				.newFixedThreadPool(availableWorkers);
		AtomicBoolean fault = new AtomicBoolean(false);
		int numDaemons = this.config.getNodesInfo().size() -1;
		for(int i =0; i<numDaemons; i++) {
			// send
			NaiveBcastThread thread = new NaiveBcastThread(
				bytes.get(0), i, fault);
			executor.execute(thread);
		}
		Util.closeExecutor(executor, "Naive Bcaster with Multi-Thread", 1500);
		if (fault.get()) {
			return false;
		}
		return true;
	}
	
	private class NaiveBcastThread implements Runnable {
		private byte[] bytes;
		private int curDaemonID;
		private AtomicBoolean fault;

		private NaiveBcastThread(byte[] bytes, int daemonID, AtomicBoolean fault) {
			this.bytes = bytes;
			this.curDaemonID = daemonID;
			this.fault = fault;
		}

		@Override
		public void run() {
			NodeInfo nextDaemon = config.getNodesInfo().get(curDaemonID);
			if (nextDaemon == null) {
				return;
			}
			String host = nextDaemon.getIp();
			int port = nextDaemon.getPort();
			Connection conn = null;
			boolean exception = false;
			int retryCount = 0;
			do {
				try {
					conn = new Connection(host, port, 0);
					send(bytes, conn.getDataOutputStream(),
							conn.getDataInputDtream());
					exception = false;
				} catch (Exception e) {
					System.out.println("exception in sending data to "
							+ nextDaemon.getIp());
					e.printStackTrace();
					if (conn != null) {
						conn.close();
					}
					conn = null;
					exception = true;
				}
				if (exception) {
					retryCount++;
					if (retryCount == CodeConstants.RETRY_COUNT) {
						fault.set(true);
						break;
					}
				}
			} while (exception);
		}
	}

	public void handle(DataOutputStream dout, DataInputStream din)
			throws Exception {
		int msgLen = din.readInt();
		// byte[] msgBuffer = new byte[msgLen + CodeConstants.SENDRECV_UNIT]; 
		byte[] msgBuffer = null;
		msgBuffer = DataManager.getInstance().allocateBuffer(
				msgLen + CodeConstants.SENDRECV_UNIT);
		if (msgBuffer == null) {
			System.out.println("Didn't get the buffer.");
			msgBuffer = new byte[msgLen + CodeConstants.SENDRECV_UNIT];
		}
		int len = 0;
		int recvMSGLen = 0;
		// reading msg body and forward
		while ((len = din.read(msgBuffer, recvMSGLen,
				CodeConstants.SENDRECV_UNIT)) > 0) {
			recvMSGLen = recvMSGLen + len;
			if (recvMSGLen == msgLen) {
				break;
			}
		}
		// there is no other msgBuffer in other thread or in this thread
		// because this is the only single msg chunk
		DataManager.getInstance().put(this.jobID, msgBuffer);
		JobManager.getInstance().remove(this.jobID);
	}
}
