package cgl.imr.message;

import java.util.ArrayList;
import java.util.List;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;

public class MapTaskGroupRequest extends PubSubMessage {

	private List<MapTaskRequest> requests;

	public MapTaskGroupRequest() {
		this.requests = new ArrayList<MapTaskRequest>();
	}

	public MapTaskGroupRequest(TwisterMessage request)
			throws SerializationException {
		this();
		this.fromTwisterMessage(request);
	}

	@Override
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		// First byte is the message type, has already been in message
		// Read the refId if any and set the boolean flag.
		readRefIdIfAny(message);
		int size = message.readInt();
		for (int i = 0; i < size; i++) {
			message.readByte();
			MapTaskRequest request = new MapTaskRequest(message);
			this.requests.add(request);
		}
	}

	@Override
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		message.writeByte(MAP_TASK_REQUEST);
		// Write the refID if any with the boolean flag.
		serializeRefId(message);
		message.writeInt(requests.size());
		for (int i = 0; i < requests.size(); i++) {
			requests.get(i).toTwisterMessage(message);
		}
	}

	public List<MapTaskRequest> getMapRequests() {
		return this.requests;
	}
}
