package cgl.imr.communication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Receiver implements Runnable {
	private static Receiver receiver;
	private ExecutorService handlerExecutor;

	private Receiver() {
		this.handlerExecutor = Executors.newFixedThreadPool(Runtime
				.getRuntime().availableProcessors());
	}

	public static synchronized void startReceiver() {
		if (receiver == null) {
			receiver = new Receiver();
			Executor executor = Executors.newSingleThreadExecutor();
			executor.execute(receiver);
		}
	}
	
	/**
	 * we check if the receiver is there
	 * 
	 * @return
	 */
	public static synchronized boolean isRunning() {
		if (receiver != null) {
			return true;
		}
		return false;
	}

	public static synchronized void stopReceiver() {
		if (receiver == null) {
			return;
		}
		Config config = null;
		try {
			config = Config.getInstance();
		} catch (Exception e) {
			e.printStackTrace();
			config = null;
		}
		if (config == null) {
			return;
		}
		Util.closeReceiver(config.getNodesInfo().get(config.getSelfID())
				.getIp(), config.getNodesInfo().get(config.getSelfID())
				.getPort());
		System.out.println("Receiver is stopped on " + config.getSelfID());
	}

	@Override
	public void run() {
		// server socket
		ServerSocket serverSocket = null;
		Config config = null;
		try {
			config = Config.getInstance();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		try {
			serverSocket = new ServerSocket(config.getNodesInfo()
					.get(config.getSelfID()).getPort());
		} catch (Exception exp) {
			serverSocket = null;
			return;
		}
		while (true) {
			try {
				Socket socket = serverSocket.accept();
				//socket.setSendBufferSize(CodeConstants.SENDRECV_UNIT);
				//socket.setReceiveBufferSize(CodeConstants.SENDRECV_UNIT);
				DataOutputStream dout = new DataOutputStream(
						socket.getOutputStream());
				DataInputStream din = new DataInputStream(
						socket.getInputStream());
				Connection conn = new Connection(dout, din, socket);
				byte msgType = din.readByte();
				if (msgType == CodeConstants.RECEIVER_QUIT_REQUEST) {
					conn.close();
					break;
				} else if (msgType == CodeConstants.FAULT_DETECTION) {
				} else {
					Handler handler = new Handler(msgType, conn,
							config.getSelfID());
					this.handlerExecutor.execute(handler);
				}
			} catch (IOException e) {
				System.out.println("Exception on Node " +config.getSelfID());
				e.printStackTrace();
			}
		}
		System.out.println("Start closing executors");
		// shutdown all receiving tasks
		Util.closeExecutor(this.handlerExecutor, "Receiver Executor");
		// close server socket
		if (serverSocket != null) {
			try {
				serverSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Finish closing executors");
		// remove the reference
		synchronized (Receiver.class) {
			receiver = null;
		}
		System.out.println("Receiver ends");
	}
}