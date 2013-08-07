package cgl.imr.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import cgl.imr.base.Combiner;
import cgl.imr.base.Key;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;
import cgl.imr.base.impl.GenericBytesMessage;
import cgl.imr.base.impl.JobConf;
import cgl.imr.message.CombineInput;
import cgl.imr.message.StartGatherMessage;
import cgl.imr.types.StringValue;
import cgl.imr.util.DataHolder;
import cgl.imr.util.MSTGatherer;
import cgl.imr.util.TwisterCommonUtil;

public class Gatherer {
	private JobConf jobConf;
	private Map<Integer, DaemonInfo> daemonInfo;
	private Combiner currentCombiner;
	private TwisterMonitorBasic monitor;
	private static Logger logger = Logger.getLogger(Gatherer.class);
	private ExecutorService taskExecutor = null;
	private TwisterDriver driver;

	Gatherer(JobConf jobConf, Map<Integer, DaemonInfo> daemonInfo,
			Combiner combiner, TwisterMonitorBasic monitor, TwisterDriver driver) {
		this.jobConf = jobConf;
		this.daemonInfo = daemonInfo;
		this.currentCombiner = combiner;
		this.monitor = monitor;
		this.taskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime()
				.availableProcessors());
		this.driver = driver;
	}

	void handleCombineInput(TwisterMessage message) {
		HandleCombineInputThread handler = new HandleCombineInputThread(message);
		this.taskExecutor.execute(handler);
	}

	private class HandleCombineInputThread implements Runnable {
		private TwisterMessage message;

		HandleCombineInputThread(TwisterMessage msg) {
			message = msg;
		}

		@Override
		public void run() {
			CombineInput combineInput = new CombineInput();
			try {
				combineInput.fromTwisterMessage(message);
				if (currentCombiner != null) {
					if (!combineInput.isHasData()) {
						combineInput = getCombineInputFromRemoteHost(combineInput);
					}
					processCombineInput(combineInput);
				} else {
					logger.error("Combiner is not configured");
				}
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Combiner encountered errors.");
				monitor.combineInputFailed();
			}
		}
	}

	/**
	 * 8/20/2011, change combine operation to be synchronized, simplify the user
	 * defined combine operation
	 * 
	 * @throws TwisterException
	 */
	private void processCombineInput(CombineInput combineInput)
			throws TwisterException {
		synchronized (currentCombiner) {
			if (!driver.isCurrentIteration(combineInput.getIteration())) {
				return;
			}
			if (!combineInput.getKeyValues().isEmpty()) {
				currentCombiner.combine(combineInput.getKeyValues());
			} else {
				System.out.println("Empty CombinInput from Daemon "
						+ combineInput.getDaemonNo());
			}
			monitor.combinerInputReceived(combineInput);
		}
	}

	private CombineInput getCombineInputFromRemoteHost(
			CombineInput combineInputTmp) throws TwisterException,
			SerializationException {
		// Map<Key,List<Value>> tmpMap=reduceInputTmp.getOutputs();
		StringValue memKey = null;// (StringValue)(tmpMap.get("known_key").get(0));//There
									// is only one value here.

		Map<Key, Value> tmpMap = combineInputTmp.getKeyValues();
		for (Key key : tmpMap.keySet()) {
			memKey = (StringValue) tmpMap.get(key);
		}
		String[] parts = memKey.toString().split(":");
		String host = parts[0].trim();
		int port = Integer.parseInt(parts[1].trim());
		String key = parts[2].trim();

		List<String> keys = new ArrayList<String>();
		keys.add(key);
		Map<String, byte[]> remoteData = TwisterCommonUtil.getDataFromServer(
				host, port, keys, "TwisterDriver");
		byte[] data = remoteData.get(key);

		/*
		 * fromTwisterMessage won't read the msgType byte, in order to keep data
		 * stream consistent, we need to read it here.
		 */
		TwisterMessage message = new GenericBytesMessage(data);
		byte msgType = message.readByte();
		if (msgType != TwisterConstants.COMBINE_INPUT) {
			throw new TwisterException("This is not a CombineInput.");
		}

		CombineInput combInput = new CombineInput();
		combInput.fromTwisterMessage(message);
		return combInput;
	}

	/**
	 * Gather combineInput from daemons where reducers are reducerDaemons are
	 * sorted with a new ID mapping to original daemon ID driver is considered
	 * as -1.
	 * 
	 * We can also use a thread to execute it.
	 * 
	 * @param reducerDaemons
	 */
	void gatherCombineInputInMST(StartGatherMessage msg) {
		HandleMSTGatherCombineInputInMSTThread handler = new HandleMSTGatherCombineInputInMSTThread(
				msg);
		this.taskExecutor.execute(handler);
	}

	/**
	 * I guess exception can happen if a daemon in the whole MST fails, the
	 * problem here is that it is hard to stop it.
	 * 
	 * @author zhangbj
	 * 
	 */
	private class HandleMSTGatherCombineInputInMSTThread implements Runnable {

		private StartGatherMessage message;

		HandleMSTGatherCombineInputInMSTThread(StartGatherMessage msg) {
			message = msg;
		}

		@Override
		public void run() {
			try {
				ConcurrentHashMap<String, DataHolder> dataCache = new ConcurrentHashMap<String, DataHolder>();
				MSTGatherer gatherer = new MSTGatherer(jobConf.getJobId(),
						message.getKeyBase(), message.getRoot(), daemonInfo,
						message.getReduceDaemonRankMap(), dataCache, null);
				gatherer.gatherInMST(message.getRoot(), message.getLeft(),
						message.getRight());
				// results should be in datacache
				int combineSize = 0;
				for (DataHolder holder : dataCache.values()) {
					combineSize = combineSize + holder.getData().length;
					TwisterMessage message = new GenericBytesMessage(
							holder.getData());
					byte msgType = message.readByte();
					if (msgType != TwisterConstants.COMBINE_INPUT) {
						throw new TwisterException(
								"This is not a CombineInput.");
					}
					CombineInput combInput = new CombineInput();
					combInput.fromTwisterMessage(message);
					processCombineInput(combInput);
				}
				System.out.println(" Combine Data Size: " + combineSize);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
