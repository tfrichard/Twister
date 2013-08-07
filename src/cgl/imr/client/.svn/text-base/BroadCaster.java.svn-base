package cgl.imr.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterConstants.SendRecvStatus;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.impl.GenericBytesMessage;
import cgl.imr.base.impl.JobConf;
import cgl.imr.communication.ChainBcaster;
import cgl.imr.communication.CollectiveComm;
import cgl.imr.communication.Config;
import cgl.imr.communication.Connection;
import cgl.imr.communication.DataManager;
import cgl.imr.communication.Receiver;
import cgl.imr.communication.Util;
import cgl.imr.message.MapTaskGroupRequest;
import cgl.imr.message.MapTaskRequest;
import cgl.imr.message.PubSubMessage;
import cgl.imr.message.WorkerResponse;

public class BroadCaster {
	private static Logger logger = Logger.getLogger(BroadCaster.class);
	private JobConf jobConf;
	private Map<Integer, DaemonInfo> daemonInfo;
	private PubSubService pubSubService;
	private Map<String, WorkerResponse> responseMap;
	private FaultDetector faultDetector;

	BroadCaster(JobConf jobConf, Map<Integer, DaemonInfo> daemonInfo,
			FaultDetector faultDetector, PubSubService pubSubService,
			Map<String, WorkerResponse> responseMap) {
		this.jobConf = jobConf;
		this.daemonInfo = daemonInfo;
		this.pubSubService = pubSubService;
		this.responseMap = responseMap;
		this.faultDetector = faultDetector;
	}

	private String setRefMessage(PubSubMessage msg) {
		UUIDGenerator uuidGen = UUIDGenerator.getInstance();
		String refMsgId = uuidGen.generateTimeBasedUUID().toString();
		msg.setRefMessageId(refMsgId);
		return refMsgId;
	}

	/**
	 * serialize message to bytes, utilize multi-threading ability on driver
	 * node
	 * 
	 * @param messages
	 * @return
	 */
	private List<byte[]> serializeMessageToBytes(List<PubSubMessage> messages,
			AtomicLong bytesCount) {
		List<byte[]> bytes = Collections
				.synchronizedList(new ArrayList<byte[]>());
		int availableWorkers = Runtime.getRuntime().availableProcessors();
		ExecutorService executor = Executors
				.newFixedThreadPool(availableWorkers);
		for (PubSubMessage message : messages) {
			HandleSerializeMessageToBytesListThread thread = new HandleSerializeMessageToBytesListThread(
					message, bytes, bytesCount);
			executor.execute(thread);
		}
		executor.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!executor.awaitTermination(600, TimeUnit.SECONDS)) {
				executor.shutdownNow(); // Cancel currently executing tasks

				// Wait a while for tasks to respond to being cancelled
				if (!executor.awaitTermination(60, TimeUnit.SECONDS))
					System.err.println("Pool did not terminate");
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			executor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
		return bytes;
	}

	/**
	 * thread to serialize message to bytes
	 * 
	 * @author zhangbj
	 * 
	 */
	private class HandleSerializeMessageToBytesListThread implements Runnable {
		private PubSubMessage message; // the message for serialization
		private List<byte[]> bytesList;
		private AtomicLong bytesCount;

		private HandleSerializeMessageToBytesListThread(PubSubMessage msg,
				List<byte[]> bytes, AtomicLong count) {
			this.message = msg;
			this.bytesList = bytes;
			this.bytesCount = count;
		}

		@Override
		public void run() {
			byte[] bytes = null;
			TwisterMessage msg = new GenericBytesMessage(null);
			try {
				this.message.toTwisterMessage(msg);
				bytes = msg.getBytesAndDestroy();
			} catch (SerializationException e) {
				e.printStackTrace();
				bytes = null;
			}
			if (bytes != null) {
				this.bytesCount.addAndGet(bytes.length);
				this.bytesList.add(bytes);
			}
		}
	}

	/**
	 * Broadcast a given message and receive responses.
	 * 
	 * @param message
	 * @return
	 */
	SendRecvResponse bcastRequestsAndReceiveResponses(PubSubMessage message) {
		SendRecvResponse output = new SendRecvResponse();
		List<Integer> workingDaemons = faultDetector
				.getCurrentWorkingDaemonsView();
		// set reference IDs for replies, reference ID <-> daemon ID
		Map<String, Integer> refIDs = new HashMap<String, Integer>();
		String refIDBase = setRefMessage(message);
		for (int i : workingDaemons) {
			refIDs.put(refIDBase + i, i);
		}
		// bcast through pubsub services
		try {
			this.pubSubService.send(TwisterConstants.CLEINT_TO_WORKER_BCAST,
					message);
		} catch (PubSubException e) {
			output.setStatus(SendRecvStatus.LOCAL_EXCEPTION);
			return output;
		}
		waitingForSendRecvCompletion(workingDaemons.size(), refIDs, output,
				TwisterConstants.SEND_RECV_MAX_SLEEP_TIME);
		return output;
	}

	/**
	 * Auto bcast method selection
	 * 
	 * @param messages
	 * @return
	 * @throws PubSubException
	 * @throws SerializationException
	 */
	SendRecvResponse bcastRequestsAndReceiveResponses(
			List<PubSubMessage> messages) {
		SendRecvResponse output = new SendRecvResponse();
		List<Integer> workingDaemons = faultDetector
				.getCurrentWorkingDaemonsView();
		Map<String, Integer> refIDs = new HashMap<String, Integer>();
		// now per daemon one ack
		String refMsgId = UUIDGenerator.getInstance().generateTimeBasedUUID()
				.toString();
		for (PubSubMessage message : messages) {
			message.setRefMessageId(refMsgId);
		}
		for (int i : workingDaemons) {
			refIDs.put(refMsgId + i, i);
		}
		// serialize messages to bytes
		long startBroadcastingTime = System.currentTimeMillis();
		AtomicLong bytesCount = new AtomicLong(0);
		List<byte[]> bytes = serializeMessageToBytes(messages, bytesCount);
		if (bytes.size() != messages.size()) {
			output.setStatus(SendRecvStatus.LOCAL_EXCEPTION);
			return output;
		}
		System.out.println("Total bcast bytes: " + bytesCount.get()
				+ " Total num of chunks: " + bytes.size());
		long approximatedSerializeTime = System.currentTimeMillis()
				- startBroadcastingTime;
		// try different bcast methods
		// start a thread to receive message
		// startReceiver();
		try {
			/*
			if (bytesCount.get() <= TwisterConstants.BCAST_BYTES_NORMAL) {
				bcastRequestsAndReceiveResponsesByMultiChain(bytes);
			} else if (bytesCount.get() <= TwisterConstants.BCAST_BYTES_HELL) {
				if (messages.size() <= TwisterConstants.BCAST_LIST_NORMAL) {
					bcastRequestsAndReceiveResponsesByMST(bytes);
				} else if (messages.size() <= workingDaemons.size() / 2) {
					bcastRequestsAndReceiveResponsesByScatterAllGatherMST(
							bytes, workingDaemons);
				} else if (messages.size() <= workingDaemons.size()) {
					bcastRequestsAndReceiveResponsesByMultiChain(bytes);
				} else {
					bcastRequestsAndReceiveResponsesByMultiChain(bytes);
				}
			} else {
				if (messages.size() <= TwisterConstants.BCAST_LIST_NORMAL) {
					bcastRequestsAndReceiveResponsesByChain(bytes, workingDaemons);
				} else if (messages.size() <= TwisterConstants.BCAST_LIST_NIGHTMARE) {
					bcastRequestsAndReceiveResponsesByScatterAllGatherMST(
							bytes, workingDaemons);
				} else {
					bcastRequestsAndReceiveResponsesByMultiChain(bytes);
				}
			}
			*/
			if (messages.size() == 1) {
				bcastRequestsAndReceiveResponsesByChain(bytes, workingDaemons);
				//bcastRequestsAndReceiveResponsesByMultiChain(bytes);
			} else {
				bcastRequestsAndReceiveResponsesByMultiChain(bytes);
			}
		} catch (Exception e) {
			// socket sending, we sure this is probably because the next remote
			// daemon dies
			output.setStatus(SendRecvStatus.REMOTE_FALIURE);
			return output;
		}
		long approximatedSendTime = System.currentTimeMillis()
				- startBroadcastingTime;
		// wait for completion
		waitingForSendRecvCompletion(workingDaemons.size(), refIDs, output,
				TwisterConstants.SEND_RECV_MAX_SLEEP_TIME);
		//waitingForSendRecvCompletionThroughTCP(
				// messages.size() * workingDaemons.size(), refIDs, output,
				// TwisterConstants.SEND_RECV_MAX_SLEEP_TIME);
		long approximatedReceiveTime = System.currentTimeMillis()
				- startBroadcastingTime;
		System.out.println("bcast  time: " + (double) approximatedSerializeTime
				/ (double) 1000 + " " + (double) approximatedSendTime
				/ (double) 1000 + " " + (double) approximatedReceiveTime
				/ (double) 1000 + " seconds.");
		return output;
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

	/**
	 * we use chain bcast algorithm here
	 * 
	 * @param messages
	 * @return
	 * @throws Exception 
	 * @throws PubSubException
	 * @throws SerializationException
	 */
	private void bcastRequestsAndReceiveResponsesByChain(List<byte[]> bytes,
			List<Integer> workingDaemons) throws Exception {
		// long jobID = (long) (Math.random() * Long.MAX_VALUE);
		// lets assume there is only one bcast event at the same time point
		long jobID = 0;
		Config.getInstance().setSelfID(-1);
		Receiver.startReceiver();
		DataManager.getInstance().put(jobID, bytes);
		CollectiveComm bcaster = new ChainBcaster(jobID, -1, -1);
		long start = System.currentTimeMillis();
		boolean success = bcaster.start();
		long end = System.currentTimeMillis();
		System.out.println("Chain Bcast takes " + (end - start));
		Receiver.stopReceiver();
		while (Receiver.isRunning()) {
		}
		if (!success) {
			throw new Exception("bcast failed.");
		}
		// we didn't add job to job manager and data is removed from the inside
		// of bcast
		// DataManager.getInstance().remove(jobID);
		sendMessageDeserializationCommand(jobID, workingDaemons);
	}

	private void sendMessageDeserializationCommand(long jobID,
			List<Integer> workingDaemons) throws TwisterException {
		System.out.println("Sending message de-serialization command.");
		AtomicBoolean exception = new AtomicBoolean(false);
		ExecutorService executor = Executors.newFixedThreadPool(Runtime
				.getRuntime().availableProcessors());
		for (int daemonID : workingDaemons) {
			MessageDeserializationThread thread = new MessageDeserializationThread(
					daemonID, TwisterConstants.MSG_DESERIAL_START, jobID,
					exception);
			executor.execute(thread);
		}
		Util.closeExecutor(executor, "command executor");
		if (exception.get()) {
			throw new TwisterException(
					"Exceptions happen in allgather phase of All to All Bcast.");
		}
	}
	
	private class MessageDeserializationThread implements Runnable {
		private int daemonID;
		private byte byteCode;
		private long jobID;
		private AtomicBoolean fault;

		private MessageDeserializationThread(int daemonID, byte code,
				long jobID, AtomicBoolean fault) {
			this.daemonID = daemonID;
			this.byteCode = code;
			this.jobID = jobID;
			this.fault = fault;
		}

		public void run() {
			Config config = null;
			try {
				config = Config.getInstance();
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
			String ip = config.getNodesInfo().get(this.daemonID).getIp();
			int port = config.getNodesInfo().get(this.daemonID).getPort();
			int timeoutMs = 0;
			Connection conn = null;
			int count = 0;
			boolean exception = false;
			do {
				try {
					conn = new Connection(ip, port, timeoutMs);
					DataOutputStream dout = new DataOutputStream(
							conn.getDataOutputStream());
					dout.writeByte(this.byteCode);
					dout.writeLong(this.jobID);
					conn.close();
					exception = false;
				} catch (Exception e) {
					e.printStackTrace();
					if (conn != null) {
						conn.close();
					}
					exception = true;
				}
				if (exception) {
					if (count < TwisterConstants.SEND_TRIES) {
						count++;
					} else {
						fault.set(true);
						return;
					}
				}
			} while (exception);
		}
	}

	/**
	 * Another function for adding MemCache, we use chain/ring bcast algorithm
	 * here, we try to overlap serialization and transmission. Moreover, we use
	 * multi-thread to chain pipeline.
	 * 
	 * @param messages
	 * @return
	 * @throws PubSubException
	 * @throws SerializationException
	 */
	private void bcastRequestsAndReceiveResponsesByMultiChain(
			List<byte[]> messages) throws TwisterException {
		System.out.println("Use MultiChain Bcast.");
		ConcurrentLinkedQueue<byte[]> queue = new ConcurrentLinkedQueue<byte[]>(
				messages);
		int availableWorkers = Runtime.getRuntime().availableProcessors();
		ExecutorService senderExecutor = Executors
				.newFixedThreadPool(availableWorkers);
		AtomicLong msgCount = new AtomicLong(0);
		AtomicBoolean fault = new AtomicBoolean(false);
		boolean forward = true;
		for (int i = 0; i < availableWorkers; i++) {
			HandleMessageSendingInMultiChainThread thread = new HandleMessageSendingInMultiChainThread(
					queue, messages.size(), msgCount, fault, forward);
			senderExecutor.execute(thread);
			/*
			 * if(forward) { forward = false; }else { forward = true; }
			 */
		}
		senderExecutor.shutdown(); // Disable new tasks from being submitted
		try {
			if (!senderExecutor.awaitTermination(180, TimeUnit.SECONDS)) {
				System.out
						.println("It still does sending after 180 seconds...");
				senderExecutor.shutdownNow(); // Cancel currently executing
												// tasks
				// Wait a while for tasks to respond to being cancelled
				if (!senderExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
					System.err.println("Sending did not terminate");
				}
			}
		} catch (InterruptedException e) {
			// (Re-)Cancel if current thread also interrupted
			senderExecutor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
		// check if any thread got fault
		if (fault.get()) {
			throw new TwisterException("Exceptions happen in MultiChain Bcast.");
		}
	}

	/**
	 * This thread creates a chain connection in all nodes and sends data in
	 * pipelin style.
	 * 
	 * @author zhangbj
	 * 
	 */
	private class HandleMessageSendingInMultiChainThread implements Runnable {
		// a shared queue for sotring serialized messages
		private ConcurrentLinkedQueue<byte[]> msgQueue;
		private int totalMsgCount;
		private AtomicLong curMsgCount;
		private AtomicBoolean fault;
		private boolean forward;

		private HandleMessageSendingInMultiChainThread(
				ConcurrentLinkedQueue<byte[]> queue, int totalCount,
				AtomicLong curCount, AtomicBoolean failure, boolean isForward) {
			msgQueue = queue;
			totalMsgCount = totalCount;
			curMsgCount = curCount;
			fault = failure;
			forward = isForward;
		}

		@Override
		public void run() {
			int nextDaemonID = 0;
			if (!forward) {
				nextDaemonID = daemonInfo.size() - 1;
			}
			boolean exception = false;
			Socket socket = null;
			DataOutputStream dout = null;
			DataInputStream dint = null;
			do {
				DaemonInfo nextDaemon = daemonInfo.get(nextDaemonID);
				// connect to the next daemon
				if (nextDaemon == null) {
					/*
					 * since we want to normally exit from the loop, we set
					 * exception tag to be false
					 */
					exception = false;
					/*
					 * we break here because we believe If the info is not
					 * available, it is out of range.
					 */
					break;
				} else {
					String host = nextDaemon.getIp();
					int port = nextDaemon.getPort();
					try {
						// connect daemon 0
						InetAddress addr = InetAddress.getByName(host);
						SocketAddress sockaddr = new InetSocketAddress(addr,
								port);
						socket = new Socket();
						int timeoutMs = 0;
						socket.connect(sockaddr, timeoutMs);
						dout = new DataOutputStream(socket.getOutputStream());
						dint = new DataInputStream(socket.getInputStream());
						exception = false;
					} catch (Exception e) {
						closeConnection(dout, dint, socket);
						dout = null;
						dint = null;
						socket = null;
						exception = true;
					}
				}
				if (exception) {
					// if there is exception in connection starting, choose next
					// daemon.
					if (forward) {
						nextDaemonID = nextDaemonID + 1;
					} else {
						nextDaemonID = nextDaemonID - 1;
					}
				} else {
					// send, if exception happens, mark in AtomicBoolean fault
					try {
						sendMessageInMultiChain(dout, dint, msgQueue,
								curMsgCount, totalMsgCount, forward);
						closeConnection(dout, dint, socket);
					} catch (Exception e) {
						closeConnection(dout, dint, socket);
						dout = null;
						dint = null;
						socket = null;
						// irrecoverable fault
						this.fault.set(true);
						return;
					}
				}
			} while (exception);
		}
	}

	/**
	 * kernel function for sending data, din and dout should not be null, if
	 * they are null, just throw the exceptions.
	 * 
	 * @param dout
	 * @param dint
	 * @param msgQueue
	 * @param curMsgCount
	 * @param totalMsgCount
	 * @param forward
	 * @param bcastedBytesList
	 * @throws Exception
	 */
	private void sendMessageInMultiChain(DataOutputStream dout,
			DataInputStream dint, ConcurrentLinkedQueue<byte[]> msgQueue,
			AtomicLong curMsgCount, int totalMsgCount, boolean forward)
			throws Exception {
		// start broadcasting
		if (forward) {
			dout.writeByte(TwisterConstants.CHAIN_BCAST_FORWARD_START);
		} else {
			dout.writeByte(TwisterConstants.CHAIN_BCAST_BACKWARD_START);
		}
		dout.flush();
		while (curMsgCount.get() < totalMsgCount) {
			byte[] bytes = msgQueue.poll();
			if (bytes == null) {
				continue;
			} else {
				curMsgCount.addAndGet(1);
			}
			// send data request body, first byte is msg type,
			// ignore
			int len = bytes.length - 1;
			// send length
			dout.writeInt(len);
			dout.flush();
			// send content
			int start = 1;
			int sendUnit = TwisterConstants.BCAST_SENDRECV_UNIT;
			while ((start + sendUnit) <= bytes.length) {
				dout.write(bytes, start, sendUnit);
				start = start + sendUnit;
				dout.flush();
			}
			dout.write(bytes, start, bytes.length - start);
			dout.flush();
			// wait for ack for next message
			byte ack = dint.readByte();
			if (ack != TwisterConstants.BCAST_ACK) {
				System.out.println("Not expected ACK: " + ack);
			}
		}
		/*
		 * ending tag, it is an int! At this moment, receiver will do
		 * 'dint.readInt();' once it sees 0, it will break from receiving loop
		 */
		dout.writeInt(0);
		dout.flush();
		// try no wait for ACK
		/*
		 * // wait for ack for ending byte ack = dint.readByte(); if (ack !=
		 * TwisterConstants.BCAST_ACK) { System.out.println("Not expected ACK: "
		 * + ack); }
		 */
	}

	// //////////////////////////////////////////////////////////////////////////////////

	/**
	 * Scatter data to different daemons, each daemon broadcast the data in MST
	 * 
	 * @param messages
	 * @param workingDaemons
	 * @throws Exception
	 */
	private void bcastRequestsAndReceiveResponsesByScatterAllGatherMST(
			List<byte[]> messages, List<Integer> workingDaemons)
			throws TwisterException {
		System.out.println("Use Scatter-Gather-MST Bcast.");
		int produceWindowSize = Runtime.getRuntime().availableProcessors();
		ExecutorService executor = Executors
				.newFixedThreadPool(produceWindowSize);
		ArrayList<Integer> destDaemons = new ArrayList<Integer>(workingDaemons);
		int destDaemonID = destDaemons.remove((int) Math.random()
				* destDaemons.size()); // initialize destDaemonID
		AtomicBoolean fault = new AtomicBoolean(false);
		for (byte[] bytes : messages) {
			HandleMessageSendingInScatterAllGatherMSTThread thread = new HandleMessageSendingInScatterAllGatherMSTThread(
					bytes, destDaemonID, workingDaemons, fault);
			executor.execute(thread);
			// get next available daemonID
			if (destDaemons.size() != 0) {
				destDaemonID = destDaemons.remove((int) Math.random()
						* destDaemons.size());
			} else {
				destDaemons = new ArrayList<Integer>(workingDaemons);
				destDaemonID = destDaemons.remove((int) Math.random()
						* destDaemons.size());
			}
		}
		executor.shutdown(); // Disable new tasks from being submitted
		try {
			if (!executor.awaitTermination(180, TimeUnit.SECONDS)) {
				System.out
						.println("It still does sending after 180 seconds...");
				executor.shutdownNow(); // Cancel currently executing
										// tasks
				// Wait a while for tasks to respond to being cancelled
				if (!executor.awaitTermination(90, TimeUnit.SECONDS)) {
					System.err.println("Sending did not terminate");
					fault.set(true); // we add fault if it could not shut down
										// normally
				}
			}
		} catch (InterruptedException e) {
			// (Re-)Cancel if current thread also interrupted
			executor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
			fault.set(true);
		}
		// if fault, throws
		if (fault.get()) {
			// we may try to interrrupt those threads in future.
			throw new TwisterException(
					"Exceptions happen in scatter phase of All to All Bcast.");
		}
	}

	/**
	 * message sending thread used in All-to-All MST/BKT Bcast
	 * 
	 * @author zhangbj
	 * 
	 */
	private class HandleMessageSendingInScatterAllGatherMSTThread implements
			Runnable {
		private byte[] bytes; // the message for sending
		private int destIDinWorkingDaemon; // the ID in workingDaemons
		private List<Integer> workingDaemons; // the whole list as backup
		private AtomicBoolean fault; // check if any thread has fault

		private HandleMessageSendingInScatterAllGatherMSTThread(byte[] msg,
				int id, List<Integer> workingDaemons, AtomicBoolean fault) {
			this.bytes = msg;
			this.destIDinWorkingDaemon = id;
			this.workingDaemons = workingDaemons;
			this.fault = fault;
		}

		public void run() {
			Socket socket = null;
			DataOutputStream dout = null;
			DataInputStream dint = null;
			boolean exception = false;
			int count = 0;
			int nextID = this.destIDinWorkingDaemon;
			do {
				// get next daemon, the dest
				DaemonInfo nextDaemon = daemonInfo.get(nextID);
				/*
				 * daemon ID under the range 0 .. daemonInfo.size() -1, it
				 * should not be null, but if it is null or exception happens,
				 * we randomly generate one
				 */
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
						// start broadcasting
						dout.writeByte(TwisterConstants.MST_SCATTER_START);
						// first byte is msg type, ignore
						int len = this.bytes.length - 1;
						// send length
						dout.writeInt(len);
						dout.flush();
						// send message, no need to send the message type
						int start = 1;
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
					} catch (Exception e) {
						closeConnection(dout, dint, socket);
						dout = null;
						dint = null;
						socket = null;
						exception = true;
					}
				}
				/*
				 * just pick another one, since it is send-arrive-forward mode,
				 * the load on evry node is unknown, just try.
				 */
				if (exception) {
					System.out
							.println("meet exceptions in scattering, try other daemons.");
					nextID = this.workingDaemons
							.get((int) (Math.random() * this.workingDaemons
									.size()));
					// here we switch TwisterConstants.SEND_TRIES times,
					// if still not work, return failure
					if (count < TwisterConstants.SEND_TRIES) {
						count++;
					} else {
						this.fault.set(true);
						return;
					}
				}
			} while (exception);
			// check who is the dest daemon
			if (this.destIDinWorkingDaemon != nextID) {
				System.out.println("Next daemon ID: "
						+ workingDaemons.get(nextID) + " IP: "
						+ daemonInfo.get(workingDaemons.get(nextID)).getIp());
			}
		}
	}

	// ////////////////////////////////////////////////////////////////////////////////
	/**
	 * Scatter data to different daemons, each daemon broadcast the data in BKT
	 * 
	 * @param messages
	 * @param workingDaemons
	 * @throws Exception
	 */
	@SuppressWarnings("unused")
	private void bcastRequestsAndReceiveResponsesByScatterAllGatherBKT(
			List<byte[]> messages, List<Integer> workingDaemons)
			throws TwisterException {
		System.out.println("Use Scatter-Gather-BKT Bcast.");
		int produceWindowSize = Runtime.getRuntime().availableProcessors();
		ExecutorService executor = Executors
				.newFixedThreadPool(produceWindowSize);
		ArrayList<Integer> destDaemons = new ArrayList<Integer>(workingDaemons);
		int destDaemonID = destDaemons.remove((int) Math.random()
				* destDaemons.size()); // initialize destDaemonID
		// make an rondom ID
		String tag = System.currentTimeMillis() + ""
				+ (long) (Math.random() * 100);
		System.out.println("Tag: " + tag);
		AtomicBoolean fault = new AtomicBoolean(false);
		for (byte[] bytes : messages) {
			HandleMessageSendingInScatterAllGatherBKTThread thread = new HandleMessageSendingInScatterAllGatherBKTThread(
					bytes, destDaemonID, workingDaemons, fault, tag);
			executor.execute(thread);
			// get next available daemonID
			if (destDaemons.size() != 0) {
				destDaemonID = destDaemons.remove((int) Math.random()
						* destDaemons.size());
			} else {
				destDaemons = new ArrayList<Integer>(workingDaemons);
				destDaemonID = destDaemons.remove((int) Math.random()
						* destDaemons.size());
			}
		}
		executor.shutdown(); // Disable new tasks from being submitted
		try {
			if (!executor.awaitTermination(180, TimeUnit.SECONDS)) {
				System.out
						.println("It still does sending after 180 seconds...");
				executor.shutdownNow(); // Cancel currently executing
										// tasks
				// Wait a while for tasks to respond to being cancelled
				if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
					System.err.println("Sending did not terminate");
					fault.set(true);
				}
			}
		} catch (InterruptedException e) {
			// (Re-)Cancel if current thread also interrupted
			executor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
			fault.set(true);
		}
		// if fault, throws
		if (fault.get()) {
			throw new TwisterException(
					"Exceptions happen in scatter phase of All to All Bcast.");
		}
		// for BKT we have the second all-gather phase to start
		System.out.println("Sending AllGather Command.");
		ExecutorService executor2 = Executors.newFixedThreadPool(Runtime
				.getRuntime().availableProcessors());
		for (int daemonID : workingDaemons) {
			HandleStartAllGatherInScatterAllGatherBKTThread thread = new HandleStartAllGatherInScatterAllGatherBKTThread(
					this.daemonInfo.get(daemonID), TwisterConstants.BKT_BCAST_START, tag, fault);
			executor2.execute(thread);
		}
		executor2.shutdown(); // Disable new tasks from being submitted
		try {
			if (!executor2.awaitTermination(180, TimeUnit.SECONDS)) {
				System.out
						.println("It still does cmd sending after 180 seconds...");
				executor2.shutdownNow(); // Cancel currently executing
											// tasks
				// Wait a while for tasks to respond to being cancelled
				if (!executor2.awaitTermination(60, TimeUnit.SECONDS)) {
					System.err.println("Sending cmd did not terminate");
				}
			}
		} catch (InterruptedException e) {
			// (Re-)Cancel if current thread also interrupted
			executor2.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
		if (fault.get()) {
			throw new TwisterException(
					"Exceptions happen in allgather phase of All to All Bcast.");
		}
	}

	/**
	 * message sending thread used in All-to-All MST/BKT Bcast
	 * 
	 * @author zhangbj
	 * 
	 */
	private class HandleMessageSendingInScatterAllGatherBKTThread implements
			Runnable {
		private byte[] bytes; // the message for sending
		private int destIDinWorkingDaemon; // the ID in workingDaemons
		private List<Integer> workingDaemons; // the whole list as backup
		private AtomicBoolean fault; // check if any thread has fault
		private String tag; // a tag used for BKT only for caching

		private HandleMessageSendingInScatterAllGatherBKTThread(byte[] msg, int id,
				List<Integer> workingDaemons, AtomicBoolean fault, String tag) {
			this.bytes = msg;
			this.destIDinWorkingDaemon = id;
			this.workingDaemons = workingDaemons;
			this.fault = fault;
			this.tag = tag;
		}

		public void run() {
			Socket socket = null;
			DataOutputStream dout = null;
			DataInputStream dint = null;
			boolean exception = false;
			int count = 0;
			int nextID = this.destIDinWorkingDaemon;
			do {
				// get next daemon, the dest
				DaemonInfo nextDaemon = daemonInfo.get(nextID);
				/*
				 * daemon ID under the range 0 .. daemonInfo.size() -1, it
				 * should not be null, but if it is null or exception happens,
				 * we randomly generate one
				 */
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
						// start broadcasting
						dout.writeByte(TwisterConstants.BKT_SCATTER_START);
						// first byte is msg type, ignore
						int len = this.bytes.length - 1;
						// send length
						dout.writeInt(len);
						// for BKT, we need a tag for caching
						// the tag currently using is a long integer
						dout.writeLong(Long.parseLong(this.tag));
						dout.flush();
						// send message, no need to send the message type
						int start = 1;
						int sendUnit = TwisterConstants.BCAST_SENDRECV_UNIT;
						while ((start + sendUnit) <= bytes.length) {
							dout.write(bytes, start, sendUnit);
							start = start + sendUnit;
							dout.flush();
						}
						dout.write(bytes, start, bytes.length - start);
						dout.flush();
						// wait for ack for next message
						// this is required to make sure the data is cached
						byte ack = dint.readByte();
						if (ack != TwisterConstants.BCAST_ACK) {
							System.out.println("Not expected ACK " + ack);
						}
						// close resources
						closeConnection(dout, dint, socket);
						exception = false;
					} catch (Exception e) {
						closeConnection(dout, dint, socket);
						dout = null;
						dint = null;
						socket = null;
						exception = true;
					}
				}
				if (exception) {
					System.out
							.println("meet exceptions in scattering, try other daemons.");
					nextID = this.workingDaemons
							.get((int) (Math.random() * this.workingDaemons
									.size()));
					// here we switch TwisterConstants.SEND_TRIES times,
					// if still not work, return failure
					if (count < TwisterConstants.SEND_TRIES) {
						count++;
					} else {
						this.fault.set(true);
						return;
					}
				}
			} while (exception);
			// check who is the dest daemon
			if (this.destIDinWorkingDaemon != nextID) {
				System.out.println("Next daemon ID: "
						+ workingDaemons.get(nextID) + " IP: "
						+ daemonInfo.get(workingDaemons.get(nextID)).getIp());
			}
		}
	}

	/**
	 * message sending thread used in All-to-All Bcast
	 * 
	 * @author zhangbj
	 * 
	 */
	private class HandleStartAllGatherInScatterAllGatherBKTThread implements
			Runnable {
		private DaemonInfo info; // the daemon needs to connect
		private byte byteCode;
		private String tag;
		private AtomicBoolean fault;

		private HandleStartAllGatherInScatterAllGatherBKTThread(DaemonInfo in,
				byte code, String tag, AtomicBoolean fault) {
			this.info = in;
			this.byteCode = code;
			this.tag = tag;
			this.fault = fault;
		}

		public void run() {
			Socket socket = null;
			DataOutputStream dout = null;
			DataInputStream dint = null;
			boolean exception = false;
			int count = 0;
			do {
				try {
					InetAddress addr = InetAddress.getByName(info.getIp());
					SocketAddress sockaddr = new InetSocketAddress(addr,
							info.getPort());
					socket = new Socket();
					int timeoutMs = 0;
					socket.connect(sockaddr, timeoutMs);
					dout = new DataOutputStream(socket.getOutputStream());
					dint = new DataInputStream(socket.getInputStream());
					dout.writeByte(this.byteCode);
					// dout.writeUTF(this.tag);
					dout.writeLong(Long.parseLong(this.tag));
					// wait for ack for next message
					/*
					 * byte ack = dint.readByte(); if (ack !=
					 * TwisterConstants.BCAST_ACK) {
					 * System.out.println("Not expected ACK " + ack); }
					 */
					closeConnection(dout, dint, socket);
					exception = false;
				} catch (Exception e) {
					e.printStackTrace();
					closeConnection(dout, dint, socket);
					exception = true;
				}
				if (exception) {
					// since every daemon needs a message, we do retry
					if (count < TwisterConstants.SEND_TRIES) {
						count++;
					} else {
						System.out
								.println("Failure happens in sending All-Gather command.");
						fault.set(true);
						return;
					}
				}
			} while (exception);
		}
	}

	// ///////////////////////////////////////////////////////////////////////////////

	/**
	 * minimum-spanning tree
	 * 
	 * @param messages
	 * @return
	 * @throws PubSubException
	 * @throws SerializationException
	 */
	private void bcastRequestsAndReceiveResponsesByMST(List<byte[]> messages)
			throws Exception {
		System.out.println("Use MST Bcast.");
		// get destination
		// send one by one
		// send to next destination
		// here root is driver, since daemon starts from 0,
		// we give an ID -1 to driver
		int root = -1;
		int left = -1;
		int right = this.daemonInfo.size() - 1;
		int mid = (int) Math.floor((left + right) / (double) 2);
		int dest = right;
		boolean exception = false;
		Socket socket = null;
		DataOutputStream dout = null;
		DataInputStream dint = null;
		while (left != right) {
			// get dest daemon, if there is exception, skip the sending
			// but should not be null, if the left and right are set according
			// to daemonInfo
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
					socket = new Socket();
					int timeoutMs = 0;
					socket.connect(sockaddr, timeoutMs);
					dout = new DataOutputStream(socket.getOutputStream());
					dint = new DataInputStream(socket.getInputStream());
					// check who is next daemon for sending
					System.out.println("Next daemon ID: " + dest + " IP: "
							+ this.daemonInfo.get(dest).getIp());
					// start broadcasting
					dout.writeByte(TwisterConstants.MST_BCAST);
					// send root, left, right information
					dout.writeInt(root);
					dout.writeInt(left);
					dout.writeInt(right);
					// send message size
					dout.writeInt(messages.size());
					dout.flush();
					long totalByteCount = 0;
					for (byte[] bytes : messages) {
						// send data request body, first byte is msg type,
						// ignore
						int len = bytes.length - 1;
						totalByteCount = totalByteCount + len;
						// send length
						dout.writeInt(len);
						dout.flush();
						// send message, no need to send the message type
						int start = 1;
						int sendUnit = TwisterConstants.BCAST_SENDRECV_UNIT;
						while ((start + sendUnit) <= bytes.length) {
							dout.write(bytes, start, sendUnit);
							start = start + sendUnit;
							dout.flush();
						}
						dout.write(bytes, start, bytes.length - start);
						dout.flush();
						// wait for ack for next message
						byte ack = dint.readByte();
						if (ack != TwisterConstants.BCAST_ACK) {
							System.out.println("Not expected ACK " + ack);
							break;
						}
					}
					// close resources
					closeConnection(dout, dint, socket);
					exception = false;
				} catch (Exception e) {
					closeConnection(dout, dint, socket);
					dout = null;
					dint = null;
					socket = null;
					exception = true;
				}
			}
			// if we get a daemon, we do sending
			// if exception, we calculate a new range
			if (exception) {
				right = right - 1;
				mid = (int) Math.floor((left + right) / (double) 2);
				dest = right;
			} else {
				// since driver (ID: -1) is always at left part,
				// we call MSTBcast(root, left, mid) directly.
				right = mid;
				mid = (int) Math.floor((left + right) / (double) 2);
				dest = right;
			}
		}
	}

	/**
	 * Similar to bcastRequestAndReceiveResponses, but waiting time is
	 * different.
	 * 
	 * @param message
	 * @return
	 */
	SendRecvResponse bcastNewJobRequestsAndReceiveResponses(
			PubSubMessage message) {
		SendRecvResponse output = new SendRecvResponse();
		List<Integer> workingDaemons = faultDetector
				.getCurrentWorkingDaemonsView();
		// set reference IDs for replies, ref ID <->Daemon ID
		Map<String, Integer> refIDs = new HashMap<String, Integer>();
		String refIDBase = setRefMessage(message);
		for (int i : workingDaemons) {
			refIDs.put(refIDBase + i, i);
		}
		// bcast through pubsub services
		try {
			this.pubSubService.send(TwisterConstants.CLEINT_TO_WORKER_BCAST,
					message);
		} catch (PubSubException e) {
			output.setStatus(SendRecvStatus.LOCAL_EXCEPTION);
			return output;
		}
		waitingForSendRecvCompletion(workingDaemons.size(), refIDs, output,
				TwisterConstants.SEND_RECV_NEWJOB_MAX_SLEEP_TIME);
		return output;
	}

	/**
	 * Similar to bcastRequestAndReceiveResponses, but waiting time is
	 * different.
	 * 
	 * @param message
	 * @return
	 */
	SendRecvResponse bcastEndJobRequestsAndReceiveResponses(
			PubSubMessage message) {
		SendRecvResponse output = new SendRecvResponse();
		List<Integer> workingDaemons = faultDetector
				.getCurrentWorkingDaemonsView();
		// set reference IDs for replies, ref ID <->Daemon ID
		Map<String, Integer> refIDs = new HashMap<String, Integer>();
		String refIDBase = setRefMessage(message);
		for (int i : workingDaemons) {
			refIDs.put(refIDBase + i, i);
		}
		// bcast through pubsub services
		try {
			this.pubSubService.send(TwisterConstants.CLEINT_TO_WORKER_BCAST,
					message);
		} catch (PubSubException e) {
			output.setStatus(SendRecvStatus.LOCAL_EXCEPTION);
			return output;
		}
		waitingForSendRecvCompletion(workingDaemons.size(), refIDs, output,
				TwisterConstants.SEND_RECV_ENDJOB_MAX_SLEEP_TIME);
		return output;
	}

	/**
	 * Used for submitting MapperRequest and ReducerRequest to the worker nodes.
	 * The method first sends all the requests and then wait till all the
	 * responses are received or a time out is reached.
	 * 
	 * @param tasksMap
	 * @return
	 */
	SendRecvResponse sendAllRequestsAndReceiveResponses(
			Map<Integer, TaskAssignment> tasksMap) {
		SendRecvResponse output = new SendRecvResponse();
		// refID and destination daemons
		HashMap<String, Integer> refIDs = new HashMap<String, Integer>();
		// Send mapper requests.
		for (int taskNo : tasksMap.keySet()) {
			TaskAssignment assignment = tasksMap.get(taskNo);
			PubSubMessage request = assignment.getTaskRequest();
			// each message has an unique reference
			refIDs.put(setRefMessage(request), assignment.getAssignedDaemon());
			String topic = TwisterConstants.MAP_REDUCE_TOPIC_BASE + "/"
					+ assignment.getAssignedDaemon();
			try {
				pubSubService.send(topic, request);
			} catch (PubSubException e) {
				output.setStatus(SendRecvStatus.LOCAL_EXCEPTION);
				return output;
			}
		}
		waitingForSendRecvCompletion(tasksMap.size(), refIDs, output,
				TwisterConstants.SEND_RECV_MAX_SLEEP_TIME);
		return output;
	}

	/**
	 * Scatter through the sockets
	 */
	SendRecvResponse sendAllRequestsAndReceiveResponsesByTCP(
			Map<Integer, TaskAssignment> tasksMap, byte taskTypeCode) {
		SendRecvResponse output = new SendRecvResponse();
		// refID and destination daemons
		HashMap<String, Integer> refIDs = new HashMap<String, Integer>();
		ExecutorService executor = Executors.newFixedThreadPool(Runtime
				.getRuntime().availableProcessors());
		AtomicBoolean exception = new AtomicBoolean(false);
		AtomicBoolean fault = new AtomicBoolean(false);
		// Send map task requests.
		for (int taskNo : tasksMap.keySet()) {
			TaskAssignment assignment = tasksMap.get(taskNo);
			PubSubMessage request = assignment.getTaskRequest();
			// each message has an unique reference
			refIDs.put(setRefMessage(request), assignment.getAssignedDaemon());
			// execute
			HandleTaskRequestSendingThread thread = new HandleTaskRequestSendingThread(
					assignment.getAssignedDaemon(), request, taskTypeCode,
					exception, fault);
			executor.execute(thread);
		}
		executor.shutdown(); // Disable new tasks from being submitted
		try {
			if (!executor.awaitTermination(600, TimeUnit.SECONDS)) {
				System.out
						.println("It still does cmd sending after 10 minutes...");
				executor.shutdownNow(); // Cancel currently executing
										// tasks
				// Wait a while for tasks to respond to being cancelled
				if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
					System.err.println("Sending cmd did not terminate");
				}
			}
		} catch (InterruptedException e) {
			// (Re-)Cancel if current thread also interrupted
			executor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
		// if exception throws
		if (exception.get()) {
			output.setStatus(SendRecvStatus.LOCAL_EXCEPTION);
			return output;
		}
		// if fault, throws
		if (fault.get()) {
			output.setStatus(SendRecvStatus.REMOTE_FALIURE);
			return output;
		}
		waitingForSendRecvCompletion(tasksMap.size(), refIDs, output,
				TwisterConstants.SEND_RECV_MAX_SLEEP_TIME);
		return output;
	}

	/**
	 * Send all requests without getting response message back, to remove
	 * communication overhead
	 */
	SendRecvResponse sendAllRequestsByTCP(
			Map<Integer, TaskAssignment> tasksMap, byte taskTypeCode) {
		SendRecvResponse output = new SendRecvResponse();
		ExecutorService executor = Executors.newFixedThreadPool(Runtime
				.getRuntime().availableProcessors());
		AtomicBoolean exception = new AtomicBoolean(false);
		AtomicBoolean fault = new AtomicBoolean(false);
		// Send map task requests.
		/*
		 * for (int taskNo : tasksMap.keySet()) { TaskAssignment assignment =
		 * tasksMap.get(taskNo); PubSubMessage request =
		 * assignment.getTaskRequest(); // execute
		 * HandleTaskRequestSendingThread thread = new
		 * HandleTaskRequestSendingThread( assignment.getAssignedDaemon(),
		 * request, taskTypeCode, exception, fault); executor.execute(thread); }
		 */
		Map<Integer, MapTaskGroupRequest> mapDistribution = new HashMap<Integer, MapTaskGroupRequest>();
		for (int taskNo : tasksMap.keySet()) {
			TaskAssignment assignment = tasksMap.get(taskNo);
			MapTaskRequest request = (MapTaskRequest) assignment
					.getTaskRequest();
			MapTaskGroupRequest requests = mapDistribution.get(assignment
					.getAssignedDaemon());
			if (requests == null) {
				requests = new MapTaskGroupRequest();
				mapDistribution.put(assignment.getAssignedDaemon(), requests);
			}
			requests.getMapRequests().add(request);
		}
		for (int daemonID : mapDistribution.keySet()) {
			// execute
			HandleTaskRequestSendingThread thread = new HandleTaskRequestSendingThread(
					daemonID, mapDistribution.get(daemonID), taskTypeCode,
					exception, fault);
			executor.execute(thread);
		}
		executor.shutdown(); // Disable new tasks from being submitted
		try {
			if (!executor.awaitTermination(600, TimeUnit.SECONDS)) {
				System.out
						.println("It still sends request after 10 minutes...");
				executor.shutdownNow(); // Cancel currently executing
										// tasks
				// Wait a while for tasks to respond to being cancelled
				if (!executor.awaitTermination(300, TimeUnit.SECONDS)) {
					System.err
							.println("Sending cmd does not terminate in 5 minutes.");
				}
			}
		} catch (InterruptedException e) {
			// (Re-)Cancel if current thread also interrupted
			executor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
		// if exception throws
		if (exception.get()) {
			output.setStatus(SendRecvStatus.LOCAL_EXCEPTION);
			return output;
		}
		// if fault, throws
		if (fault.get()) {
			output.setStatus(SendRecvStatus.REMOTE_FALIURE);
			return output;
		}
		// if all sent, we believe it is successful
		output.setStatus(SendRecvStatus.SUCCESS);
		return output;
	}

	private class HandleTaskRequestSendingThread implements Runnable {
		private int destDaemonID;
		private PubSubMessage request;
		private byte taskByteCode;
		private AtomicBoolean exception;
		private AtomicBoolean fault;

		private HandleTaskRequestSendingThread(int id, PubSubMessage request,
				byte code, AtomicBoolean exception, AtomicBoolean fault) {
			this.destDaemonID = id;
			this.request = request;
			this.taskByteCode = code;
			this.exception = exception;
			this.fault = fault;
		}

		@Override
		public void run() {
			// serialization failure is identified as local exception
			byte[] bytes = null;
			TwisterMessage msg = new GenericBytesMessage(null);
			try {
				this.request.toTwisterMessage(msg);
				bytes = msg.getBytesAndDestroy();
			} catch (SerializationException e) {
				e.printStackTrace();
				this.exception.set(true);
				return;
			}
			// sending failure is identified as fault
			Socket socket = null;
			DataOutputStream dout = null;
			DataInputStream dint = null;
			boolean exception = false;
			int count = 0;
			do {
				long startConnectionTime = System.currentTimeMillis();
				long startSendingTime = System.currentTimeMillis();
				long stage1Time = System.currentTimeMillis();
				long stage2Time = System.currentTimeMillis();
				long stage3Time = System.currentTimeMillis();
				try {
					DaemonInfo info = daemonInfo.get(this.destDaemonID);
					InetAddress addr = InetAddress.getByName(info.getIp());
					SocketAddress sockaddr = new InetSocketAddress(addr,
							info.getPort());
					socket = new Socket();
					int timeoutMs = 0;
					socket.connect(sockaddr, timeoutMs);
					dout = new DataOutputStream(socket.getOutputStream());
					dint = new DataInputStream(socket.getInputStream());
					startSendingTime = System.currentTimeMillis();
					dout.writeByte(this.taskByteCode);
					// first byte is msg type, ignore
					int len = bytes.length - 1;
					// send length
					dout.writeInt(len);
					dout.flush();
					stage1Time = System.currentTimeMillis();
					// send message, no need to send the message type
					int start = 1;
					int sendUnit = TwisterConstants.BCAST_SENDRECV_UNIT;
					while ((start + sendUnit) <= bytes.length) {
						dout.write(bytes, start, sendUnit);
						start = start + sendUnit;
						dout.flush();
					}
					dout.write(bytes, start, bytes.length - start);
					dout.flush();
					stage2Time = System.currentTimeMillis();
					// don't get ack, this can take long time!
					/*
					 * // wait for ack for next message byte ack =
					 * dint.readByte(); if (ack != TwisterConstants.BCAST_ACK) {
					 * System.out.println("Not expected ACK " + ack); }
					 */
					closeConnection(dout, dint, socket);
					stage3Time = System.currentTimeMillis();
					exception = false;
					/*
					 * System.out.println("Successfully send task to Daemon " +
					 * this.destDaemonID + ".");
					 */
				} catch (Exception e) {
					e.printStackTrace();
					closeConnection(dout, dint, socket);
					exception = true;
				}
				// print if long time is used
				if ((System.currentTimeMillis() - startConnectionTime) > 1000) {
					System.out
							.println("Sending task to Daemon "
									+ this.destDaemonID
									+ " takes "
									+ (System.currentTimeMillis() - startConnectionTime)
									+ " and "
									+ (System.currentTimeMillis() - startSendingTime)
									+ " and "
									+ (System.currentTimeMillis() - stage1Time)
									+ " and "
									+ (System.currentTimeMillis() - stage2Time)
									+ " and "
									+ (System.currentTimeMillis() - stage3Time)
									+ " milliseconds.");
				}
				if (exception) {
					// since every daemon needs a message, we do retry
					if (count < TwisterConstants.SEND_TRIES) {
						System.out.println("Retry sending task to Daemon "
								+ this.destDaemonID + ".");
						count++;
					} else {
						System.out
								.println("Failure happens in sending Map task request.");
						this.fault.set(true);
						return;
					}
				}
			} while (exception);
		}
	}

	/**
	 * used for waiting send/recv completion, the completion status is recorded
	 * in SendRecvResponse
	 * 
	 * @param numResponse
	 * @param refIDs
	 * @param output
	 * @param maxWaitingTime
	 */
	private void waitingForSendRecvCompletion(int numResponse,
			Map<String, Integer> refIDs, SendRecvResponse output,
			long maxWaitingTime) {
		// record which response is received
		HashSet<String> receivedResponse = new HashSet<String>();
		boolean resReceived = false;
		boolean anyExceptions = false;
		boolean anyFaults = false;
		boolean timeOut = false;
		// checking and timing
		long waitedTime = 0;
		long waitingStartTime = System.currentTimeMillis();
		WorkerResponse response = null;
		while (!(resReceived || timeOut || anyFaults || anyExceptions)) {
			for (String refID : refIDs.keySet()) {
				response = responseMap.remove(refID);
				if (response != null) {
					if (receivedResponse.contains(refID)) {
						System.out.println("Duplicated reply reference ID "
								+ refID);
					} else {
						receivedResponse.add(refID);
					}
					if (response.isHasException()) {
						anyExceptions = true;
						logger.error("Exceptions at broadcast operation on daemon no "
								+ response.getDaemonNo()
								+ " @ "
								+ response.getDaemonIp()
								+ " "
								+ response.getExceptionString());
					}
				}
			}
			// All responses received.
			if (receivedResponse.size() == numResponse) {
				resReceived = true;
			} else {
				if (jobConf.isFaultTolerance()) {
					if (faultDetector.isHasFault()) {
						anyFaults = true;
					}
				}
				// Wait and see.
				waitedTime = System.currentTimeMillis() - waitingStartTime;
				if (waitedTime > maxWaitingTime) {
					timeOut = true;
				}
			}
		}
		output.setReceivedResponse(receivedResponse);
		if (anyExceptions) {
			output.setStatus(SendRecvStatus.REMOTE_EXCEPTION);
			return;
		}
		if (anyFaults) {
			output.setStatus(SendRecvStatus.REMOTE_FALIURE);
			return;
		}
		// max time is larger than TwisterConstants.FAULT_DETECTION_INTERVAL
		// If fault happens, it should be detected
		if (timeOut) {
			for (String refID : refIDs.keySet()) {
				if (!receivedResponse.contains(refID)) {
					System.out.println("The response from daemon  "
							+ refIDs.get(refID) + " with ref ID " + refID
							+ " is not received");
				}
			}
			System.out.println("Current contents in responseMap.");
			for (String id : this.responseMap.keySet()) {
				System.out.println(" Response Message ID " + id);
			}
			output.setStatus(SendRecvStatus.TIMEOUT);
			return;
		}
		if (resReceived) {
			output.setStatus(SendRecvStatus.SUCCESS);
		}
	}
	
	/**
	 * try to start a socket accepter to get the output.
	 * 
	 * @param numResponse
	 * @param refIDs
	 * @param output
	 * @param maxWaitingTime
	 */
	/*
	private void waitingForSendRecvCompletionThroughTCP(int numResponse,
			Map<String, Integer> refIDs, SendRecvResponse output,
			long maxWaitingTime) {
		// wait
		waitingForSendRecvCompletion(numResponse, refIDs, output,
				maxWaitingTime);
		// stop the server
		stopReceiver();
	}
	*/
}
