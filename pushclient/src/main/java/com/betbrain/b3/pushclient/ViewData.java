package com.betbrain.b3.pushclient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sdql.SEPCConnector;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;
import com.betbrain.sepc.connector.sdql.SEPCPushConnector;

public class ViewData implements SEPCConnectorListener {
	
	final long LIMIT_RECORD = 500;
	public static void main(String[] args) {
		SEPCConnector pushConnector = new SEPCPushConnector("sept.betbrain.com", 7000);
		pushConnector.addConnectorListener(new ViewData());
		//pushConnector.setEntityChangeBatchProcessingMonitor(new BatchMonitor());
		pushConnector.start("OddsHistory");
	}

	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		
	}
	
	private HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<String, HashMap<Long,Entity>>();
	
	//eventPartId -> eventId
	private HashMap<Long, Long> eventPartToEventMap = new HashMap<Long, Long>();
	
	private JsonMapper jsonMapper = new JsonMapper();

	public void notifyInitialDump(List<? extends Entity> entityList) {
		int iEventPart = 0;
		int iProvider = 0;
		int iOutcome = 0;
		int iLocation = 0;
		int iEventAction = 0;
		//int iEventInfo = 0;
		
		for (Entity e : entityList) {
			HashMap<Long, Entity> subMap = masterMap.get(e.getClass().getName());
			if (subMap == null) {
				subMap = new HashMap<Long, Entity>();
				masterMap.put(e.getClass().getName(), subMap);
			}
			subMap.put(e.getId(), e);
			
			if (e instanceof Event) {
				Event event = (Event) e;
				eventPartToEventMap.put(event.getRootPartId(), event.getId());
			}
			
			if (e instanceof com.betbrain.sepc.connector.sportsmodel.EventPart) {
				iEventPart++;
				if(iEventPart > LIMIT_RECORD) continue;
				System.out.println("cautch EventPart Entity");
				ReadWriteTextFileWithEncoding io = new ReadWriteTextFileWithEncoding("EventPart.txt", "UTF-8");
				io.setsOutput(jsonMapper.serialize(e));
				try {
					io.write();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			
			else if (e instanceof com.betbrain.sepc.connector.sportsmodel.Provider) {
				iProvider++;
				if(iProvider > LIMIT_RECORD) continue;				
				System.out.println("cautch Provider Entity");
				ReadWriteTextFileWithEncoding io = new ReadWriteTextFileWithEncoding("Provider.txt", "UTF-8");
				io.setsOutput(jsonMapper.serialize(e));
				try {
					io.write();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			
			else if (e instanceof com.betbrain.sepc.connector.sportsmodel.Outcome) {
				iOutcome++;
				if(iOutcome > LIMIT_RECORD) continue;	
				System.out.println("cautch Outcome Entity");
				ReadWriteTextFileWithEncoding io = new ReadWriteTextFileWithEncoding("OutCome.txt", "UTF-8");
				io.setsOutput(jsonMapper.serialize(e));
				try {
					io.write();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			
			else if (e instanceof com.betbrain.sepc.connector.sportsmodel.Location) {
				iLocation++;
				if(iLocation > LIMIT_RECORD) continue;		
				System.out.println("cautch Location Entity");
				ReadWriteTextFileWithEncoding io = new ReadWriteTextFileWithEncoding("Location.txt", "UTF-8");
				io.setsOutput(jsonMapper.serialize(e));
				try {
					io.write();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			
			else if (e instanceof com.betbrain.sepc.connector.sportsmodel.EventAction) {
				iEventAction++;
				if(iEventAction > LIMIT_RECORD) continue;		
				System.out.println("cautch Location Entity");
				ReadWriteTextFileWithEncoding io = new ReadWriteTextFileWithEncoding("EventAction.txt", "UTF-8");
				io.setsOutput(jsonMapper.serialize(e));
				try {
					io.write();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			
			else if (e instanceof com.betbrain.sepc.connector.sportsmodel.EventInfo) {
				//iEventInfo++;
				if(iEventAction > LIMIT_RECORD) continue;		
				System.out.println("cautch EventInfo Entity");
				ReadWriteTextFileWithEncoding io = new ReadWriteTextFileWithEncoding("EventInfo.txt", "UTF-8");
				io.setsOutput(jsonMapper.serialize(e));
				try {
					io.write();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			
			else if (e instanceof com.betbrain.sepc.connector.sportsmodel.EventInfoType) {	
				System.out.println("cautch EventInfoType Entity");
				ReadWriteTextFileWithEncoding io = new ReadWriteTextFileWithEncoding("EventInfoType.txt", "UTF-8");
				io.setsOutput(jsonMapper.serialize(e));
				try {
					io.write();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
}
