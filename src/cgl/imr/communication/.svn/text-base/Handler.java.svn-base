package cgl.imr.communication;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Handler implements Runnable {
	private byte msgType;
	private Connection conn;
	private int selfID;

	public Handler(byte msgType, Connection conn, int selfID) {
		this.msgType = msgType;
		this.conn = conn;
		this.selfID = selfID;
	}

	@Override
	public void run() {
		try {
			if (msgType == CodeConstants.CHAIN_BCAST_START) {
				long jobID = conn.getDataInputDtream().readLong();
				int root = conn.getDataInputDtream().readInt();
				int totalMsgCount = conn.getDataInputDtream().readInt();
				// start the job, there may be multiple handler
				MultiChainBcaster bcaster = null;
				synchronized (JobManager.getInstance()) {
					bcaster = (MultiChainBcaster) JobManager.getInstance()
							.get(jobID);
					if (bcaster == null) {
						bcaster = new MultiChainBcaster(jobID, this.selfID,
								totalMsgCount, root);
						JobManager.getInstance().add(jobID, bcaster);
					}
				}
				bcaster.handle(conn.getDataOutputStream(),
						conn.getDataInputDtream());
			} else if (msgType == CodeConstants.CHAIN_ALTER_BCAST_START) {
				long jobID = conn.getDataInputDtream().readLong();
				int root = conn.getDataInputDtream().readInt();
				// start the job, there is only one handler
				CollectiveComm bcaster = null;
				boolean justCreated = false;
				synchronized (JobManager.getInstance()) {
					bcaster = JobManager.getInstance().get(jobID);
					if (bcaster == null) {
						/*
						 * clean data manager for repeat jobID this is important
						 * since we could reuse the memory deleted for the new
						 * buffer, to reduce the total memory we also suggest
						 * other receiver delete the data as quickly as possible
						 * once finishing using it, to save the total memory
						 * used in JVM and improve the performance
						 */
						// DataManager.getInstance().remove(jobID);
						DataManager.getInstance().freeBuffer();
						// we assume the last job object has been deleted in the
						// last bcast operation
						bcaster = new ChainBcaster(jobID, this.selfID, root);
						JobManager.getInstance().add(jobID, bcaster);
						justCreated = true;
					}
				}
				if (justCreated) {
					bcaster.handle(conn.getDataOutputStream(),
							conn.getDataInputDtream());
				}
			}else if (msgType == CodeConstants.ACK) {
				long jobID = conn.getDataInputDtream().readLong();
				byte[] bytes = { CodeConstants.ACK };
				DataManager.getInstance().add(jobID, bytes);
			} else if (msgType == CodeConstants.NAIVE_BCAST_START) {
				long jobID = conn.getDataInputDtream().readLong();
				int root = conn.getDataInputDtream().readInt();
				// start the job, there is only one handler
				CollectiveComm bcaster = null;
				boolean justCreated = false;
				synchronized (JobManager.getInstance()) {
					bcaster = (NaiveBcaster) JobManager.getInstance()
							.get(jobID);
					if (bcaster == null) {
						DataManager.getInstance().freeBuffer();
						bcaster = new NaiveBcaster(jobID, this.selfID, root);
						JobManager.getInstance().add(jobID, bcaster);
						justCreated = true;
					}
				}
				if (justCreated) {
					try {
						bcaster.handle(conn.getDataOutputStream(),
								conn.getDataInputDtream());
					} catch (Exception e) {
						JobManager.getInstance().remove(jobID);
						conn.close();
					}
				}
			} else {
				long jobID = conn.getDataInputDtream().readLong();
				byte[] bytes = receiveMessage(conn.getDataOutputStream(),
						conn.getDataInputDtream());
				DataManager.getInstance().add(jobID, bytes);
			}
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			conn.close();
		}
	}

	private byte[] receiveMessage(DataOutputStream dout, DataInputStream din)
			throws IOException {
		byte[] buff = new byte[CodeConstants.SENDRECV_UNIT]; // 8 KB
		// allocate byte stream
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// get msg length and forward
		int msgLen = din.readInt();
		int len = 0;
		int recvMSGLen = 0;
		// reading msg body and forward
		while ((len = din.read(buff)) > 0) {
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
}
