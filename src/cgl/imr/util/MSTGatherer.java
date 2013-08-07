package cgl.imr.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.client.DaemonInfo;

/**
 * this is the key class to do gather in MST
 * 
 * @author zhangbj
 * 
 */
public class MSTGatherer {

	private String keyBase;
	private int me;
	private Map<Integer, DaemonInfo> daemonInfo;

	/*
	 * rankID : daemonID
	 */
	private Map<Integer, Integer> daemonRankMap;
	private ConcurrentHashMap<String, DataHolder> dataCache;

	public MSTGatherer(String jobID, String keyBase, int me,
			Map<Integer, DaemonInfo> daemonInfo,
			Map<Integer, Integer> daemonRankMap,
			ConcurrentHashMap<String, DataHolder> dataCache,
			TwisterMessage message) throws SerializationException {

		// we use job ID and  iteration here
		this.keyBase =keyBase+"_";
		this.me = me;
		this.daemonInfo = daemonInfo;
		this.daemonRankMap = daemonRankMap;

		this.dataCache = dataCache;
		if (message != null) {
			this.dataCache.put(this.keyBase + me,
					new DataHolder(message.getBytesAndDestroy(), 1));
		}
	}

	public void gatherInMST(int root, int left, int right)
			throws TwisterException {
		if (left == right) {
			return;
		}

		int mid = (int) Math.floor((left + right) / (double) 2);
		int srce = left;
		if (root <= mid) {
			srce = right;
		}

		if (me <= mid && root <= mid) {
			gatherInMST(root, left, mid);
		} else if (me <= mid && root > mid) {
			gatherInMST(srce, left, mid);
		} else if (me > mid && root <= mid) {
			gatherInMST(srce, mid + 1, right);
		} else if (me > mid && root > mid) {
			gatherInMST(root, mid + 1, right);
		}

		if (root <= mid) {
			if (me == root) {
				int daemonID = this.daemonRankMap.get(srce);
				// System.out.println("srce " + srce + " daemonID " + daemonID
				// );
				DaemonInfo info = daemonInfo.get(daemonID);
				String keyListStr = "";
				List<String> keyList = new ArrayList<String>();
				for (int i = mid + 1; i <= right; i++) {
					keyList.add(this.keyBase + i);
					keyListStr = keyListStr + " " + i;
				}
				/*
				 * if (me == 79 || me == 75) { System.out.println(me +
				 * ": download from " + srce + " " + " mid+1: " + (mid + 1) +
				 * " right: " + right + ". " + keyListStr); }
				 */
				// get client name
				String clientName = "TwisterDriver";
				Integer daemonNo = this.daemonRankMap.get(me);
				if (daemonNo != null) {
					// clientName = this.daemonInfo.get(daemonNo).getIp();
					clientName = "Daemon No: " + daemonNo;
				}
				// receive
				Map<String, byte[]> remoteData = TwisterCommonUtil
						.getDataFromServer(info.getIp(), info.getPort(),
								keyList, clientName);
				for (String key : remoteData.keySet()) {
					this.dataCache.put(key, new DataHolder(remoteData.get(key),
							1));
				}
			}
		} else {
			if (me == root) {
				int daemonID = this.daemonRankMap.get(srce);
				// System.out.println("srce " + srce + " daemonID " + daemonID
				// );
				DaemonInfo info = daemonInfo.get(daemonID);
				String keyListStr = "";
				List<String> keyList = new ArrayList<String>();
				for (int i = left; i <= mid; i++) {
					keyList.add(this.keyBase + i);
					keyListStr = keyListStr + " " + i;
				}
				/*
				 * if (me == 79 || me == 75) { System.out.println(me +
				 * ": download from " + srce + " " + " left: " + left + " mid: "
				 * + mid + ". " + keyListStr); }
				 */
				// get client name
				String clientName = "TwisterDriver";
				Integer daemonNo = this.daemonRankMap.get(me);
				if (daemonNo != null) {
					clientName = "Daemon No: " + daemonNo;
				}
				// receive
				Map<String, byte[]> remoteData = TwisterCommonUtil
						.getDataFromServer(info.getIp(), info.getPort(),
								keyList, clientName);
				for (String key : remoteData.keySet()) {
					this.dataCache.put(key, new DataHolder(remoteData.get(key),
							1));
				}
			}
		}
	}
}