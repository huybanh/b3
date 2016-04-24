package com.betbrain.b3.pushclient;

import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

/**
 * Hello world!
 *
 */
public class App {
	
	public static void main(String[] args) {
		System.out.println("Hello B3!");
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(new InitialPushListener());
		pushConnector.setEntityChangeBatchProcessingMonitor(new BatchMonitor());
		pushConnector.start("OddsHistory");

	}
}
