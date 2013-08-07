package cgl.imr.client;

public class DaemonInfo {
	private String ip;

	private int port;

	public DaemonInfo(String ip, int port) {
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
