package cgl.imr.communication;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MultiChainBcaster implements CollectiveComm {
	// bcast job description
	private long jobID;
	private int nodeID;
	private int totalMsgCount;
	private int root;
	private Config config;
	// bcast job status
	private AtomicLong curMsgCount;
	private AtomicBoolean isFault; // sender checks if there is any fault
	private AtomicBoolean isDone; // receiver checks if all are received

	MultiChainBcaster(long jobID, int nodeID, int totalMsgCount, int root)
			throws Exception {
		// System.out.println("Use MultiChain Bcast.");
		// basic info
		this.jobID = jobID;
		this.nodeID = nodeID;
		this.totalMsgCount = totalMsgCount;
		this.root = root;
		this.config = Config.getInstance();
		// status control
		this.curMsgCount = new AtomicLong(0);
		this.isFault = new AtomicBoolean(false);
		this.isDone = new AtomicBoolean(false);
	}

	public boolean start() {
		if (this.root != -1 || this.nodeID != -1) {
			// may throw exception
			return false;
		}
		//take out the datas
		List<byte[]> bytes = DataManager.getInstance().remove(this.jobID);
		if (bytes == null || bytes.size() == 0) {
			return false;
		}
		// get workers
		int availableWorkers = Runtime.getRuntime().availableProcessors();
		if (bytes.size() < availableWorkers) {
			availableWorkers = bytes.size();
		}
		// add for response
		List<byte[]> responses = new ArrayList<byte[]>();
		DataManager.getInstance().put(this.jobID, responses);
		// establish queue
		ConcurrentLinkedQueue<byte[]> queue = new ConcurrentLinkedQueue<byte[]>(
				bytes);
		ExecutorService senderExecutor = Executors
				.newFixedThreadPool(availableWorkers);
		for (int i = 0; i < availableWorkers; i++) {
			MultiChainBcastThread thread = new MultiChainBcastThread(queue);
			senderExecutor.execute(thread);
		}
		Util.closeExecutor(senderExecutor, "Bcast Executor");
		// check if any thread got fault
		if (this.isFault.get()) {
			return false;
		}
		waitForCompletion();
		return true;
	}

	private class MultiChainBcastThread implements Runnable {
		// a shared queue for sorting serialized messages
		private ConcurrentLinkedQueue<byte[]> msgQueue;

		private MultiChainBcastThread(ConcurrentLinkedQueue<byte[]> queue) {
			this.msgQueue = queue;
		}

		@Override
		public void run() {
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
						e.printStackTrace();
						conn = null;
						exception = true;
					}
				}
				if (exception) {
					nextDaemonID = nextDaemonID + 1;
				} else {
					try {
						send(conn.getDataOutputStream(),
								conn.getDataInputDtream());
						conn.close();
					} catch (Exception e) {
						e.printStackTrace();
						conn.close();
						isFault.set(true);
						return;
					}
				}
			} while (exception);
		}

		private void send(DataOutputStream dout, DataInputStream din)
				throws Exception {
			// Initial meta data to create the MultiChainBcast object at remote
			// System.out.println("send out initial information");
			dout.writeByte(CodeConstants.CHAIN_BCAST_START);
			dout.writeLong(jobID);
			dout.writeInt(root);
			dout.writeInt(totalMsgCount);
			dout.flush();
			// send
			while (curMsgCount.get() < totalMsgCount) {
				byte[] bytes = msgQueue.poll();
				if (bytes == null) {
					continue;
				} else {
					curMsgCount.addAndGet(1);
				}
				int len = bytes.length;
				// send length
				dout.writeInt(len);
				dout.flush();
				// send content
				int start = 0;
				int sendUnit = CodeConstants.SENDRECV_UNIT;
				while ((start + sendUnit) <=len) {
					dout.write(bytes, start, sendUnit);
					start = start + sendUnit;
					dout.flush();
				}
				dout.write(bytes, start, len - start);
				dout.flush();
				// synchronization
				din.readByte();
			}
			// ending tag,
			dout.writeInt(0);
			dout.flush();
		}
	}

	private void waitForCompletion() {
		while (DataManager.getInstance().get(this.jobID).size() != (this.config
				.getNodesInfo().size() - 1)) {
			/*
			System.out.println(DataManager.getInstance().get(this.jobID).size()
					+ " " + this.config.getNodesInfo().size());
			*/
		}
	}

	/**
	 * There are multiple handler
	 */
	public void handle(DataOutputStream dout, DataInputStream din)
			throws Exception {
		// get next daemon
		int nextDaemonID = this.nodeID + 1;
		// System.out.println("next daemon id " + nextDaemonID);
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
							"Exceptions in sending data InMultiChainBcast on "
									+ this.nodeID, e);
				}
			}
		} while (exception);
	}

	private void receive(DataOutputStream dout, DataInputStream din,
			Connection nextConn) throws IOException {
		// allocate buffer
		// byte[] buffer = new byte[CodeConstants.SENDRECV_UNIT];
		// ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream doutNextD = null;
		DataInputStream dinNextD = null;
		//check if there is the next daemon
		if (nextConn != null) {
			doutNextD = nextConn.getDataOutputStream();
			dinNextD = nextConn.getDataInputDtream();
		}
		// meta data
		// to construct MultiChainBroadcaster at remote
		if (doutNextD != null) {
			doutNextD.writeByte(CodeConstants.CHAIN_BCAST_START);
			doutNextD.writeLong(this.jobID);
			doutNextD.writeInt(this.root);
			doutNextD.writeInt(this.totalMsgCount);
			doutNextD.flush();
		}
		// receive and forward
		while (true) {
			// get msg length and forward
			int msgLen = din.readInt();
			if (doutNextD != null) {
				doutNextD.writeInt(msgLen);
				doutNextD.flush();
			}
			// if msgLen == 0, it is an end tag
			// reply to driver
			if (msgLen == 0) {
				synchronized (this.isDone) {
					if (!this.isDone.get()
							&& this.curMsgCount.get() == this.totalMsgCount) {
						notifyForCompletion();
						this.isDone.set(true);
						JobManager.getInstance().remove(this.jobID);
						DataManager.getInstance().remove(this.jobID);
					}
				}
				break;
			} else {
				// leave some space 
				byte[] msgBuffer = new byte[msgLen
				 + CodeConstants.SENDRECV_UNIT];
				int len = 0;
				int recvMSGLen = 0;
				// reading msg body and forward
				/*
				while ((len = din.read(buffer)) > 0) {
					if (doutNextD != null) {
						doutNextD.write(buffer, 0, len);
						doutNextD.flush();
					}
					bout.write(buffer, 0, len);
					recvMSGLen = recvMSGLen + len;
					if (recvMSGLen == msgLen) {
						break;
					}
				}
				bout.flush();
				*/
				// it may not read the whole 8192 byte even there is still data left
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
				// synchronization
				//write->do->read
				dout.writeByte(CodeConstants.ACK);
				dout.flush();
				DataManager.getInstance().add(this.jobID, msgBuffer);
				// DataManager.getInstance().add(this.jobID,
				// bout.toByteArray());
				// bout.reset();
				this.curMsgCount.addAndGet(1);
				if (dinNextD != null) {
					dinNextD.readByte();
				}
			}
		} // end of big for loop
		// bout.close();
	}

	private void notifyForCompletion() {
		// send a ACK to the root
		Connection conn = null;
		try {
			conn = new Connection(this.config.getNodesInfo().get(this.root)
					.getIp(), this.config.getNodesInfo().get(this.root)
					.getPort(), 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			DataOutputStream dout = conn.getDataOutputStream();
			dout.writeByte(CodeConstants.ACK);
			dout.writeLong(this.jobID);
			dout.flush();
			conn.close();
		} catch (Exception e) {
			// close connection
			conn.close();
		}
	}
}
