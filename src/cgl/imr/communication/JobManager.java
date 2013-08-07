package cgl.imr.communication;

import java.util.HashMap;
import java.util.Map;

public class JobManager {
	private static JobManager jobManager;
	private Map<Long, CollectiveComm> jobHolder;

	private JobManager() {
		jobHolder = new HashMap<Long, CollectiveComm>();
	}

	public static synchronized JobManager getInstance() {
		if (jobManager == null) {
			jobManager = new JobManager();
		}
		return jobManager;
	}

	public synchronized boolean add(long jobID, CollectiveComm job) {
		if (this.jobHolder.get(jobID) == null) {
			this.jobHolder.put(jobID, job);
			return true;
		}
		return false;
	}

	public synchronized CollectiveComm get(long jobID) {
		return this.jobHolder.get(jobID);
	}

	public synchronized void remove(long jobID) {
		this.jobHolder.remove(jobID);
	}
}
