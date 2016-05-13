package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEventInfo;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;

public class EventInfoQuery {
	
	private static JsonMapper mapper = new JsonMapper();
	
	public static void main(String[] args) {
		
		//DynamoWorker.initBundleCurrent();
		DynamoWorker.initBundleByStatus("SPRINT2");
		System.out.println("Scores");
		B3KeyEventInfo scoreKey = new B3KeyEventInfo(217562668L, 1L, null);
		ArrayList<EventInfo> scores = scoreKey.listEntities(true, mapper);
		for (EventInfo one : scores) {
			System.out.println(one);
		}
		
		System.out.println("Match statuses");
		B3KeyEventInfo statusKey = new B3KeyEventInfo(217562668L, 92L, null);
		ArrayList<EventInfo> statuses = statusKey.listEntities(true, mapper);
		for (EventInfo one : statuses) {
			System.out.println(one);
		}
	}
}
