package cgl.imr.communication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataManager {
	private static DataManager dataManager;
	private Map<Long, List<byte[]>> dataHolder;
	private byte[] buffer;
	private int defaultBufferSize;
	private boolean isBufferInUse;

	private DataManager() {
		dataHolder = new HashMap<Long, List<byte[]>>();
		// defaultBufferSize = Integer.MAX_VALUE;
		// buffer = new byte[defaultBufferSize];
		defaultBufferSize = 0;
		buffer = null;
		isBufferInUse = false;
	}

	public static synchronized DataManager getInstance() {
		if (dataManager == null) {
			dataManager = new DataManager();
		}
		return dataManager;
	}
	
	/**
	 * return a buffer is it is available
	 * 
	 * @param jobID
	 * @param size
	 * @return
	 */
	public synchronized byte[] allocateBuffer(int size) {
		if (size > Integer.MAX_VALUE || this.isBufferInUse) {
			return null;
		}
		if (buffer == null || size > this.defaultBufferSize) {
			buffer = new byte[size];
			defaultBufferSize = size;
		}
		this.isBufferInUse = true;
		return this.buffer;
	}

	public synchronized boolean isBufferInUse() {
		return this.isBufferInUse;
	}

	public synchronized void freeBuffer() {
		this.isBufferInUse = false;
	}

	/**
	 * add on existing job or new job
	 * @param jobID
	 * @param bytes
	 */
	public synchronized void add(long jobID, byte[] bytes) {
		if (this.dataHolder.containsKey(jobID)) {
			this.dataHolder.get(jobID).add(bytes);
		} else {
			List<byte[]> bytesList = new ArrayList<byte[]>();
			bytesList.add(bytes);
			this.dataHolder.put(jobID, bytesList);
		}
	}

	/**
	 * replace the existing one
	 * 
	 * @param jobID
	 * @param bytes
	 */
	public synchronized void put(long jobID, byte[] bytes) {
		if (dataHolder.containsKey(jobID)) {
			dataHolder.get(jobID).clear();
			dataHolder.get(jobID).add(bytes);
		} else {
			List<byte[]> bytesList = new ArrayList<byte[]>();
			bytesList.add(bytes);
			dataHolder.put(jobID, bytesList);
		}
	}
	
	/**
	 * replace the existing one
	 * 
	 * @param jobID
	 * @param bytes
	 */
	public synchronized void put(long jobID, List<byte[]> bytes) {
		dataHolder.put(jobID, bytes);
	}

	public synchronized List<byte[]> get(long jobID) {
		return dataHolder.get(jobID);
	}

	public synchronized List<byte[]> remove(long jobID) {
		return dataHolder.remove(jobID);
	}
}
