package cgl.imr.message;

import java.util.HashMap;
import java.util.Map;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterMessage;

public class StartGatherMessage extends PubSubMessage {
	private String jobId;
	private String keyBase;
	private Map<Integer, Integer> reducerDaemonRankMap;
	private int root;
	private int left;
	private int right;
	private byte gatherMethod;
	private String responseTopic;

	private StartGatherMessage() {
		jobId = null;
		keyBase = null;
		reducerDaemonRankMap = new HashMap<Integer, Integer>();
		root = -1;
		left = -1;
		right = 0; // assume there is at least one daemon
		gatherMethod = TwisterConstants.GATHER_IN_DIRECT;
		responseTopic = null;
	}

	public StartGatherMessage(String jobId,
			Map<Integer, Integer> reducerDaemonRankMap, String responseTopic,
			byte gatherMethod) {
		this();
		this.jobId = jobId;
		this.keyBase = UUIDGenerator.getInstance().generateRandomBasedUUID()
				.toString();
		this.reducerDaemonRankMap = reducerDaemonRankMap;
		this.right = this.reducerDaemonRankMap.size() - 1;
		this.responseTopic = responseTopic;
		this.gatherMethod = gatherMethod;
	}

	public StartGatherMessage(TwisterMessage message)
			throws SerializationException {
		this();
		this.fromTwisterMessage(message);
	}

	public String getJobId() {
		return jobId;
	}
	
	public String getKeyBase() {
		return this.keyBase;
	}
	
	public String getResponseTopic() {
		return this.responseTopic;
	}
	
	public int getRoot() {
		return root;
	}

	public int getLeft() {
		return left;
	}

	public int getRight() {
		return right;
	}
	
	public byte getGatherMethod() {
		return gatherMethod;
	}
	
	public Map<Integer, Integer> getReduceDaemonRankMap() {
		return this.reducerDaemonRankMap;
	}

	@Override
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		// First byte is the message type, is read from onEvent
		// Read the refId if any and set the boolean flag.
		readRefIdIfAny(message);
		this.jobId = message.readString();
		this.keyBase = message.readString();
		this.responseTopic = message.readString();
		this.gatherMethod = message.readByte();
		this.root = message.readInt();
		this.left = message.readInt();
		this.right = message.readInt();
		int rankMapSize = message.readInt();
		for (int i = 0; i < rankMapSize; i++) {
			reducerDaemonRankMap.put(message.readInt(), message.readInt());
		}
	}

	@Override
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		// First byte is the message type.
		message.writeByte(START_GATHER);
		// Write the refID if any with the boolean flag.
		serializeRefId(message);
		message.writeString(jobId);
		message.writeString(this.keyBase);
		message.writeString(this.responseTopic);
		message.writeByte(gatherMethod);
		message.writeInt(root);
		message.writeInt(left);
		message.writeInt(right);
		message.writeInt(reducerDaemonRankMap.size());
		for (int rank : reducerDaemonRankMap.keySet()) {
			message.writeInt(rank);
			message.writeInt(reducerDaemonRankMap.get(rank));
		}
	}
}
