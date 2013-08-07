package cgl.imr.communication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

public class Connection {
	private DataOutputStream dout;
	private DataInputStream din;
	private Socket socket;

	public Connection(String host, int port, int timeOutMs) throws Exception {
		try {
			// connect daemon 0
			InetAddress addr = InetAddress.getByName(host);
			SocketAddress sockaddr = new InetSocketAddress(addr, port);
			this.socket = new Socket();
			//this.socket.setSendBufferSize(CodeConstants.SENDRECV_UNIT);
			//this.socket.setReceiveBufferSize(CodeConstants.SENDRECV_UNIT);
			int timeoutMs = timeOutMs;
			this.socket.connect(sockaddr, timeoutMs);
			//System.out.println("Socket buffer: " + this.socket.getSendBufferSize() + " " + this.socket.getReceiveBufferSize());
			this.dout = new DataOutputStream(socket.getOutputStream());
			this.din = new DataInputStream(socket.getInputStream());
		} catch (Exception e) {
			this.close();
			throw e;
		}
	}

	public Connection(DataOutputStream dout, DataInputStream din, Socket socket) {
		this.socket = socket;
		this.din = din;
		this.dout = dout;
	}

	public DataOutputStream getDataOutputStream() {
		return this.dout;
	}

	public DataInputStream getDataInputDtream() {
		return this.din;
	}

	public void close() {
		try {
			if (dout != null) {
				dout.close();
			}
		} catch (IOException e) {
		}
		try {
			if (din != null) {
				din.close();
			}
		} catch (IOException e) {
		}
		try {
			if (socket != null) {
				socket.close();
			}
		} catch (IOException e) {
		}
		this.dout = null;
		this.din = null;
		this.socket = null;
	}
}
