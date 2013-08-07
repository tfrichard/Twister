package cgl.imr.samples.kmeans.hydra.a;

import cgl.imr.types.IntKey;

public class NewIntKey extends IntKey {
	public NewIntKey() {
		super();
	}

	public NewIntKey(int key) {
		super(key);
	}

	public boolean isMergeableInShuffle() {
		// System.out.println("isMergeableInShuffle");
		// return false;
		return true;
	}
}
