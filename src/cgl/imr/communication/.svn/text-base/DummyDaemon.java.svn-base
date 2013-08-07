package cgl.imr.communication;

public class DummyDaemon {
	// main
	// get ID
	// start receiver

	public static void main(String args[]) throws Exception {
		int nodeID = Integer.parseInt(args[0]);
		Config.getInstance().setSelfID(nodeID);
		DataManager.getInstance();
		Receiver.startReceiver();
		System.out.println("Node " + nodeID + " starts. ");
		while (Receiver.isRunning()) {
		}
	}
}
