package cgl.imr.samples.kmeans.ivy.dcran.a;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;

public class StaticImageVectorData implements Value {
	private int numData;
	private int vecLen;
	private long id[][] ;
	private byte data[][];

	public StaticImageVectorData() {
		this.numData = 0;
		this.vecLen = 0;
		this.id = null;
		this.data = null;
	}

	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		this.numData = message.readInt();
		this.vecLen = message.readInt();
		// id row col
		if (this.numData > 0 && this.vecLen > 0) {
			this.id = new long[this.numData][3];
			for (int i = 0; i < this.numData; i++) {
				this.id[i][0] = message.readLong();
				this.id[i][1] = message.readLong();
				this.id[i][2] = message.readLong();
			}
			this.data = new byte[this.numData][this.vecLen];
			for (int i = 0; i < this.numData; i++) {
				for (int j = 0; j < this.vecLen; j++) {
					this.data[i][j] = message.readByte();
				}
			}
		} else {
			this.id = null;
			this.data = null;
		}
	}

	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		message.writeInt(this.numData);
		message.writeInt(this.vecLen);
		if (this.numData > 0 && this.vecLen > 0) {
			for (int i = 0; i < this.numData; i++) {
				message.writeLong(this.id[i][0]);
				message.writeLong(this.id[i][1]);
				message.writeLong(this.id[i][2]);
			}
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

	public long[][] getIDs() {
		return this.id;
	}

	public byte[][] getData() {
		return this.data;
	}

	/**
	 * Loads data from a text file. picID row col dim f1 f2 f3 f4 ... fdim
	 * 
	 */
	public void loadDataFromTextFile(String fileName) throws Exception {
		this.numData = KmeansClustering.countTotalLines(fileName);
		if (this.numData == 0) {
			throw new Exception("Fail in counting total lines number");
		}
		this.vecLen = KmeansClustering.countDimensionality(fileName);
		if (this.vecLen == 0) {
			throw new Exception("Fail in getting dimensionality");
		}
		this.id = new long[numData][3];
		this.data = new byte[numData][vecLen];
		// read real data
		BufferedReader reader = null;
		int tmpVecLen = this.vecLen;
		boolean exception = false;
		try {
			reader = new BufferedReader(new FileReader(new File(fileName)));
			for (int i = 0; i < this.numData; i++) {
				String inputLine = reader.readLine();
				String[] vectorValues = inputLine.split(" ");
				// to match with the new id e.g. 99000070_3
				this.id[i][0] = Long
						.parseLong(vectorValues[0].replace("_", ""));
				this.id[i][1] = Long.parseLong(vectorValues[1]);
				this.id[i][2] = Long.parseLong(vectorValues[2].split("\t")[0]);
				if (vectorValues.length < this.vecLen
						+ KmeansClustering.lineOffset) {
					tmpVecLen = vectorValues.length
							- KmeansClustering.lineOffset;
					System.out.println("Potential incorrect line: "
							+ vectorValues.length);
				} else {
					tmpVecLen = this.vecLen;
				}
				for (int j = 0; j < tmpVecLen; j++) {
					data[i][j] = (byte) (Integer.parseInt(vectorValues[j
							+ KmeansClustering.lineOffset]) - KmeansClustering.valueOffset);
				}
			}
			reader.close();
		} catch (Exception e) {
			exception = true;
		}
		// handle exception
		if (exception) {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
				}
			}
			throw new Exception("Fail in getting data");
		}
	}
}
