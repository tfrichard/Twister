package cgl.imr.deployment;

import java.util.ArrayList;
import java.util.List;

class CMDOutput {
	private boolean status;
	private List<String> output;

	CMDOutput() {
		status = true;
		output = new ArrayList<String>();
	}

	void setExecutionStatus(boolean isSuccess) {
		status = isSuccess;
	}

	boolean getExecutionStatus() {
		return status;
	}

	List<String> getExecutionOutput() {
		return output;
	}
}