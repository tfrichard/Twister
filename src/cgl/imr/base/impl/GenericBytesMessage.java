package cgl.imr.base.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;

public class GenericBytesMessage implements TwisterMessage {
	private byte[] bytes = null;
	private DataInputStream din = null;
	private ByteArrayOutputStream baOutputStream = null;
	private DataOutputStream dout = null;

	/**
	 * the trick here is that if bytes is not null, this message is only for
	 * reading
	 * 
	 * If bytes is null, this message is only for writing
	 * 
	 * @param bytes
	 */
	public GenericBytesMessage(byte[] bytes) {
		if (bytes != null) {
			this.bytes = bytes;
			din = new DataInputStream(new ByteArrayInputStream(this.bytes));
		} else {
			baOutputStream = new ByteArrayOutputStream();
			dout = new DataOutputStream(baOutputStream);
		}
	}

	/**
	 * A message for reading has bytes, it closes din and return bytes. A
	 * message for writing doesn't have bytes, it flush and return the bytes
	 * written
	 * 
	 * @return
	 * @throws IOException
	 */

	public byte[] getBytesAndDestroy() throws SerializationException {
		try {
			if (din != null) {
				din.close();
				din = null;
			}

			if (baOutputStream != null && dout != null) {
				dout.flush();
				this.bytes = baOutputStream.toByteArray();
				baOutputStream.close();
				dout.close();
				baOutputStream = null;
				dout = null;
			}
			byte[] bytes = this.bytes;
			this.bytes = null;
			return bytes;
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}
	
	@Override
	public long getLength() throws SerializationException {
		if (this.bytes != null) {
			return this.bytes.length;
		} else {
			return this.baOutputStream.size();
		}
	}

	private boolean isReadingInitialized() {
		if (this.bytes != null && this.din != null) {
			return true;
		}
		return false;
	}

	private boolean isWritingInitialized() {
		if (baOutputStream != null && dout != null) {
			return true;
		}
		return false;
	}

	@Override
	public boolean readBoolean() throws SerializationException {
		if (!isReadingInitialized()) {
			throw new SerializationException();
		}
		try {
			return din.readBoolean();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeBoolean(boolean arg0) throws SerializationException {
		if (!isWritingInitialized()) {
			throw new SerializationException();
		}
		try {
			dout.writeBoolean(arg0);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public byte readByte() throws SerializationException {
		if (!isReadingInitialized()) {
			throw new SerializationException();
		}
		try {
			return din.readByte();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeByte(byte arg0) throws SerializationException {
		if (!isWritingInitialized()) {
			throw new SerializationException();
		}
		try {
			dout.writeByte(arg0);
		} catch (IOException e) {
			throw new SerializationException(e);
		}

	}

	@Override
	public byte[] readBytes() throws SerializationException {
		if (!isReadingInitialized()) {
			throw new SerializationException();
		}
		try {
			int len = din.readInt();
			byte bytes[] = new byte[len];
			din.readFully(bytes);
			return bytes;
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeBytes(byte[] bytes) throws SerializationException {
		if (!isWritingInitialized()) {
			throw new SerializationException();
		}
		try {
			dout.writeInt(bytes.length);
			dout.write(bytes);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}
	
	@Override
	public void readBytesPartially(byte[] bytes, int off, int len)
			throws SerializationException {
		if (!isReadingInitialized()) {
			throw new SerializationException();
		}
		try {
			din.readFully(bytes, off, len);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeBytesPartially(byte[] bytes, int off, int len)
			throws SerializationException {
		if (!isWritingInitialized()) {
			throw new SerializationException();
		}
		try {
			dout.write(bytes, off, len);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public char readChar() throws SerializationException {
		if (!isReadingInitialized()) {
			throw new SerializationException();
		}
		try {
			return din.readChar();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeChar(char arg0) throws SerializationException {
		if (!isWritingInitialized()) {
			throw new SerializationException();
		}
		try {
			dout.writeChar(arg0);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public double readDouble() throws SerializationException {
		if (!isReadingInitialized()) {
			throw new SerializationException();
		}
		try {
			return din.readDouble();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeDouble(double arg0) throws SerializationException {
		if (!isWritingInitialized()) {
			throw new SerializationException();
		}
		try {
			dout.writeDouble(arg0);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public float readFloat() throws SerializationException {
		if (!isReadingInitialized()) {
			throw new SerializationException();
		}
		try {
			return din.readFloat();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeFloat(float arg0) throws SerializationException {
		if (!isWritingInitialized()) {
			throw new SerializationException();
		}
		try {
			dout.writeFloat(arg0);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public int readInt() throws SerializationException {
		if (!isReadingInitialized()) {
			throw new SerializationException();
		}
		try {
			return din.readInt();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeInt(int arg0) throws SerializationException {
		if (!isWritingInitialized()) {
			throw new SerializationException();
		}
		try {
			dout.writeInt(arg0);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public long readLong() throws SerializationException {
		if (!isReadingInitialized()) {
			throw new SerializationException();
		}
		try {
			return din.readLong();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeLong(long arg0) throws SerializationException {
		if (!isWritingInitialized()) {
			throw new SerializationException();
		}
		try {
			dout.writeLong(arg0);
		} catch (IOException e) {
			throw new SerializationException(e);
		}

	}

	@Override
	public short readShort() throws SerializationException {
		if (!isReadingInitialized()) {
			throw new SerializationException();
		}
		try {
			return din.readShort();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeShort(short arg0) throws SerializationException {
		if (!isWritingInitialized()) {
			throw new SerializationException();
		}
		try {
			dout.writeShort(arg0);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public String readString() throws SerializationException {
		if (!isReadingInitialized()) {
			throw new SerializationException();
		}
		try {
			return din.readUTF();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void writeString(String arg0) throws SerializationException {
		if (!isWritingInitialized()) {
			throw new SerializationException();
		}
		try {
			dout.writeUTF(arg0);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}
}
