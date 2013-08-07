package cgl.imr.message;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;

public class StartShuffleMessage extends PubSubMessage {

	private String jobId;
	private String responseTopic;

	private StartShuffleMessage() {
	}

	public StartShuffleMessage(String jobId, String responseTopic) {
		this();
		this.jobId = jobId;
		this.responseTopic = responseTopic;
	}

	public StartShuffleMessage(TwisterMessage message)
			throws SerializationException {
		this();
		this.fromTwisterMessage(message);
	}

	public String getJobId() {
		return jobId;
	}
	
	public String getResponseTopic() {
		return this.responseTopic;
	}

	@Override
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		// First byte is the message type, is read from onEvent
		// Read the refId if any and set the boolean flag.
		readRefIdIfAny(message);
		this.jobId = message.readString();
		this.responseTopic = message.readString();
	}

	@Override
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		// First byte is the message type.
		message.writeByte(START_SHUFFLE);
		// Write the refID if any with the boolean flag.
		serializeRefId(message);
		message.writeString(this.jobId);
		message.writeString(this.responseTopic);
	}
}
