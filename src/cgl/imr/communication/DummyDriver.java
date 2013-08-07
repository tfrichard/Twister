package cgl.imr.communication;

import java.util.ArrayList;
import java.util.List;

public class DummyDriver {

	public static void main(String args[]) throws Exception {
		int totalData = Integer.parseInt(args[0]);
		int numChunk = Integer.parseInt(args[1]);
		int numLoop = Integer.parseInt(args[2]);
		int dataPerChunk = totalData / numChunk;
		// generate data
		// brocaster.start
		long a = System.currentTimeMillis();
		List<byte[]> bytes = new ArrayList<byte[]>();
		for (int i = 0; i < numChunk; i++) {
			byte[] byteArray = new byte[dataPerChunk];
			System.out.println(" The end of byteArray: "
					+ byteArray[byteArray.length - 1]);
			byteArray[byteArray.length - 1] = 1;
			System.out.println(" The end of byteArray: "
					+ byteArray[byteArray.length - 1]);
			bytes.add(byteArray);
		}
		System.out.println(" creation time: "
				+ (System.currentTimeMillis() - a));
		
		long jobID = 0;
		Config.getInstance().setSelfID(-1);
		Receiver.startReceiver();
		CollectiveComm bcaster = null;
		// new multichain broadcaster
		for (int i = 0; i < numLoop; i++) {
			DataManager.getInstance().put(jobID, bytes);
			long start = System.currentTimeMillis();
			if (bytes.size() > 1) {
				bcaster = new MultiChainBcaster(jobID, -1, bytes.size(), -1);
			} else {
				bcaster = new ChainBcaster(jobID, -1, -1);
				// bcaster = new NaiveBcaster(jobID, -1, -1);
			}
			boolean success = bcaster.start();
			// exception happens on IPoIB
			// boolean success = ((NaiveBcaster) bcaster).startWithMultiThreads();
			System.out.println("time: " + (System.currentTimeMillis() - start)
					+ " " + success);
			bytes.get(0)[bytes.get(0).length - 1] = (byte) (Math.random() * Byte.MAX_VALUE);
		}
		Receiver.stopReceiver();
		while (Receiver.isRunning()) {
		}
		System.out.println("broadcasting ends");
		System.exit(0);
	}
}
