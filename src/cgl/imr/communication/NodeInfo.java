package cgl.imr.communication;

public class NodeInfo {
	private String ip;
	private int port;

	public NodeInfo(String ip, int port) {
		this.ip = ip;
		this.port = port;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getIp() {
		return ip;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getPort() {
		return port;
	}
}
