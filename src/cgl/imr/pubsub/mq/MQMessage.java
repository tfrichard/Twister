package cgl.imr.pubsub.mq;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Session;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;

class MQMessage implements TwisterMessage {

	private BytesMessage bmsg;
	private Session session;

	MQMessage(BytesMessage bytes, Session session) {
		bmsg = bytes;
		this.session = session;
	}
	
	// to get the message for sending
	BytesMessage getBytesMessage() {
		return this.bmsg;
	}
	
	Session getSession() {
		return this.session;
	}
	
	
	//note that after get bytes, the message can not be read of write any more.
	public byte[] getBytesAndDestroy() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.reset();
			byte[] message = new byte[(int) bmsg.getBodyLength()];
			bmsg.readBytes(message);
			
			this.bmsg = null;
			this.session.close();
			this.session = null;

			return message;
		} catch (Exception e) {
			throw new SerializationException(e);
		}
		
	}

	@Override
	public long getLength() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.reset();
			return bmsg.getBodyLength();
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}
	
	@Override
	public boolean readBoolean() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			return bmsg.readBoolean();
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeBoolean(boolean arg0) throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.writeBoolean(arg0);
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public byte readByte() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			return bmsg.readByte();
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeByte(byte arg0) throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.writeByte(arg0);
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public byte[] readBytes() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			int len = bmsg.readInt();
			byte bytes[] = new byte[len];
			bmsg.readBytes(bytes);
			return bytes;
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeBytes(byte[] bytes) throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.writeInt(bytes.length);
			bmsg.writeBytes(bytes);
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}
	@Override
	public void readBytesPartially(byte[] bytes, int off, int len)
			throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			byte tmpBytes[] = new byte[len];
			bmsg.readBytes(tmpBytes);
			for (int i = 0; i < len; i++) {
				bytes[off + i] = tmpBytes[i];
			}
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}
	
	@Override
	public void writeBytesPartially(byte[] bytes, int off, int len)
			throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.writeBytes(bytes, off, len);
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public char readChar() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			return bmsg.readChar();
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeChar(char arg0) throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.writeChar(arg0);
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public double readDouble() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			return bmsg.readDouble();
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeDouble(double arg0) throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.writeDouble(arg0);
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public float readFloat() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			return bmsg.readFloat();
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeFloat(float arg0) throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.writeFloat(arg0);
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public int readInt() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			return bmsg.readInt();
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeInt(int arg0) throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.writeInt(arg0);
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public long readLong() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			return bmsg.readLong();
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeLong(long arg0) throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.writeLong(arg0);
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public short readShort() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			return bmsg.readShort();
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeShort(short arg0) throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.writeShort(arg0);
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	/**
	 * In order to let the bytes got here is compatible with the bytes
	 * got through GenericBytesMessage, we need to keep the serialization
	 * implementation be same.
	 */
	public String readString() throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			return bmsg.readUTF();
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeString(String arg0) throws SerializationException {
		if (this.bmsg == null) {
			throw new SerializationException();
		}
		try {
			bmsg.writeUTF(arg0);
		} catch (JMSException e) {
			throw new SerializationException(e);
		}
	}

}
