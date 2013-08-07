package cgl.imr.samples.kmeans.hydra.dcran.a;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;

public class BcastCentroidVectorData implements Value {
	private int numData;
	private int vecLen;
	private byte data[][];

	public BcastCentroidVectorData() {
		this.numData = 0;
		this.vecLen = 0;
		this.data = null;
	}

	public BcastCentroidVectorData(int numData, int vecLen, byte[][] data) {
		this.numData = numData;
		this.vecLen = vecLen;
		this.data = data;
	}

	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		this.numData = message.readInt();
		this.vecLen = message.readInt();
		if (this.numData > 0 && this.vecLen > 0) {
			this.data = new byte[this.numData][this.vecLen];
			for (int i = 0; i < this.numData; i++) {
				for (int j = 0; j < this.vecLen; j++) {
					this.data[i][j] = message.readByte();
				}
			}
		} else {
			this.data = null;
		}
	}

	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		message.writeInt(this.numData);
		message.writeInt(this.vecLen);
		if (this.numData > 0 && this.vecLen > 0) {
			for (int i = 0; i < this.numData; i++) {
				for (int j = 0; j < this.vecLen; j++) {
					message.writeByte(this.data[i][j]);
				}
			}
		}
	}

	@Override
	public void mergeInShuffle(Value value) {
	}

	public int getNumData() {
		return numData;
	}

	public int getVecLen() {
		return vecLen;
	}

	public byte[][] getData() {
		return this.data;
	}
}
