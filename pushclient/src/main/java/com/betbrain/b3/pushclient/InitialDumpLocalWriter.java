package com.betbrain.b3.pushclient;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

public class InitialDumpLocalWriter implements SEPCConnectorListener {
	
	public static void main(String[] args) {
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(new InitialDumpLocalWriter());
		//pushConnector.setEntityChangeBatchProcessingMonitor(new BatchMonitor());
		pushConnector.start("OddsHistory");
	}

	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		
	}

	public void notifyInitialDump(final List<? extends Entity> entityList) {
		
		new Thread() {
			public void run() {
				try {
					FileOutputStream fos = new FileOutputStream("initial_dump.json", false);
					BufferedOutputStream out = new BufferedOutputStream(fos);
					OutputStreamWriter writer = new OutputStreamWriter(out);
					for (Entity e : entityList) {
						writer.write((JsonMapper.SerializeF(e) + "\n"));
					}
					System.out.println("Got all entities in file");
					writer.close();
					out.close();
					fos.close();
				} catch (FileNotFoundException e1) {
					throw new RuntimeException(e1);
				} catch (IOException e1) {
					throw new RuntimeException(e1);
				}
			}
		}.start();
	}

}
