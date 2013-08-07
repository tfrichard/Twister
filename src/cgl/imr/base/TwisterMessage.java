package cgl.imr.base;

public interface TwisterMessage {

	public byte[] getBytesAndDestroy() throws SerializationException;

	public long getLength() throws SerializationException;

	public boolean readBoolean() throws SerializationException;

	public void writeBoolean(boolean arg0) throws SerializationException;

	public byte readByte() throws SerializationException;

	public void writeByte(byte arg0) throws SerializationException;

	public byte[] readBytes() throws SerializationException;

	public void writeBytes(byte[] bytes) throws SerializationException;

	void readBytesPartially(byte[] bytes, int off, int len)
			throws SerializationException;

	public void writeBytesPartially(byte[] bytes, int off, int len)
			throws SerializationException;

	public char readChar() throws SerializationException;

	public void writeChar(char arg0) throws SerializationException;

	public double readDouble() throws SerializationException;

	public void writeDouble(double arg0) throws SerializationException;

	public float readFloat() throws SerializationException;

	public void writeFloat(float arg0) throws SerializationException;

	public int readInt() throws SerializationException;

	public void writeInt(int arg0) throws SerializationException;

	public long readLong() throws SerializationException;

	public void writeLong(long arg0) throws SerializationException;

	public short readShort() throws SerializationException;

	public void writeShort(short arg0) throws SerializationException;

	public String readString() throws SerializationException;

	public void writeString(String arg0) throws SerializationException;

}
