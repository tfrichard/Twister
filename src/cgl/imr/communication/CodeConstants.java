package cgl.imr.communication;

public class CodeConstants {
	// code
	public static final byte RECEIVER_QUIT_REQUEST = 0;
	public static final byte FAULT_DETECTION = 1;
	public static final byte ACK = 2;
	public static final byte CHAIN_BCAST_START = 10;
	public static final byte CHAIN_ALTER_BCAST_START = 11;
	public static final byte NAIVE_BCAST_START = 20;

	public static final int CONNECT_MAX_WAIT_TIME = 60000;

	public static final long TERMINATION_TIMEOUT_1 = 180;
	public static final long TERMINATION_TIMEOUT_2 = 60;
	public static final int SENDRECV_UNIT = 8192;
	public static final int SLEEP_COUNT = 100;
	public static final int RETRY_COUNT = 10;
}
