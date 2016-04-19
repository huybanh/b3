package com.betbrain.b3.pushclient;

import com.betbrain.sepc.connector.sdql.EntityChangeBatchProcessingMonitor;

public class BatchMonitor implements EntityChangeBatchProcessingMonitor {

	public long getLastAppliedEntityChangeBatchId() {
		//return 1078396949; //1.078.396.949 26.087.561.785
		//return 26087561785l; //26.087.605.918
		return 26087605918l; //26087648733, 26088032242, 26088174747
	}

}
