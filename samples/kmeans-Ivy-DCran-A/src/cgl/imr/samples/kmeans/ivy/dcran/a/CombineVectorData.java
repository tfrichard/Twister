package cgl.imr.samples.kmeans.ivy.dcran.a;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;

public class CombineVectorData implements Value {

	private BcastCentroidVectorData centroid;
	private double totalDistance;

	public CombineVectorData() {
		this.centroid = null;
		this.totalDistance = 0;
	}

	public CombineVectorData(BcastCentroidVectorData centroid,
			double totalDistance) {
		this.centroid = centroid;
		this.totalDistance = totalDistance;
	}

	@Override
	public void fromTwisterMessage(TwisterMessage msg)
			throws SerializationException {
		this.centroid = new BcastCentroidVectorData();
		this.centroid.fromTwisterMessage(msg);
		this.totalDistance = msg.readDouble();
	}

	@Override
	public void toTwisterMessage(TwisterMessage msg)
			throws SerializationException {
		this.centroid.toTwisterMessage(msg);
		msg.writeDouble(this.totalDistance);
	}

	@Override
	public void mergeInShuffle(Value arg0) {
	}

	public BcastCentroidVectorData getCentroid() {
		return this.centroid;
	}

	public double getTotalDistance() {
		return this.totalDistance;
	}
}
