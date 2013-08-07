package cgl.imr.types;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;

public class MemCacheAddress implements Value {

	private String memCacheKeyBase;
	private int start;
	private int range;

	public MemCacheAddress() {
		super();
	}

	public MemCacheAddress(String keyBase, int start, int range) {
		super();
		this.memCacheKeyBase = keyBase;
		this.start = start;
		this.range = range;
	}

	public String getMemCacheKeyBase() {
		return memCacheKeyBase;
	}

	public int getStart() {
		return start;
	}

	public int getRange() {
		return range;
	}

	@Override
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		this.memCacheKeyBase = message.readString();
		this.start = message.readInt();
		this.range = message.readInt();
	}

	@Override
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		message.writeString(this.memCacheKeyBase);
		message.writeInt(this.start);
		message.writeInt(this.range);
	}

	@Override
	public void mergeInShuffle(Value value) {
	}
}
