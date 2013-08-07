package cgl.imr.communication;

import java.io.DataOutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class Util {

	static void closeReceiver(String ip, int port) {
		Connection conn = null;
		try {
			// close the receiver on this node
			conn = new Connection(ip, port, 0);
		} catch (Exception e) {
			e.printStackTrace();
			conn = null;
		}
		if (conn == null) {
			return;
		}
		try {
			DataOutputStream dout = conn.getDataOutputStream();
			dout.writeByte(CodeConstants.RECEIVER_QUIT_REQUEST);
			dout.flush();
			conn.close();
		} catch (Exception e) {
			conn.close();
		}
	}
	
	public static void closeExecutor(ExecutorService executor, String executorName) {
		// shutdown all receiving tasks
		executor.shutdown();
		try {
			if (!executor.awaitTermination(CodeConstants.TERMINATION_TIMEOUT_1,
					TimeUnit.SECONDS)) {
				System.out.println(executorName + " still works after "
						+ CodeConstants.TERMINATION_TIMEOUT_1 + " seconds...");
				executor.shutdownNow();
				if (!executor.awaitTermination(
						CodeConstants.TERMINATION_TIMEOUT_2, TimeUnit.SECONDS)) {
					System.out.println(executorName
							+ " did not terminate with "
							+ CodeConstants.TERMINATION_TIMEOUT_2 + " more.");
				}
			}
		} catch (InterruptedException e) {
			executor.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}
	
	public static void closeExecutor(ExecutorService executor, String executorName, long maxtime) {
		// shutdown all receiving tasks
		executor.shutdown();
		try {
			if (!executor.awaitTermination(maxtime, TimeUnit.SECONDS)) {
				System.out.println(executorName + " still works after "
						+ CodeConstants.TERMINATION_TIMEOUT_1 + " seconds...");
				executor.shutdownNow();
				if (!executor.awaitTermination(
						CodeConstants.TERMINATION_TIMEOUT_2, TimeUnit.SECONDS)) {
					System.out.println(executorName
							+ " did not terminate with "
							+ CodeConstants.TERMINATION_TIMEOUT_2 + " more.");
				}
			}
		} catch (InterruptedException e) {
			executor.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}
}
