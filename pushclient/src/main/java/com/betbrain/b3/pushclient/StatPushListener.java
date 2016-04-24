package com.betbrain.b3.pushclient;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;
import com.betbrain.sepc.connector.sdql.SEPCConnectorListener;

public class StatPushListener implements SEPCConnectorListener {
	
	private static boolean first = true;

	public void notifyEntityUpdates(EntityChangeBatch changeBatch) {
		for (EntityChange change: changeBatch.getEntityChanges()) {
			
			if (first) {
				System.out.println("First changed entity: " + change);
			}
			
			String entClass = change.getEntityClass().getName();
			/*if (entClass.contains(".Betting") || entClass.startsWith(".Event") ||
					entClass.startsWith(".Outcome")) {
				
				if (!entClass.contains("Participant")) {
					System.out.println("Changed: " + change);
				}
			}*/
			if (entClass.contains(".Source")) {
				//System.out.println("Changed source: " + change);
				if (change instanceof EntityUpdate) {
					long id = ((EntityUpdate) change).getEntityId();
					Integer count = sourceMap.get(id);
					if (count == null) {
						count = 0;
					}
					sourceMap.put(id, ++count);
				}
			}
			
			Integer i = map.get(entClass);
			if (i == null) {
				i = 0;
			}
			map.put(entClass, i + 1);
			
			if (System.currentTimeMillis() - lastConsole > 10000) {
				System.out.println("========= " + new Date() + 
						" / after (sec): " + (System.currentTimeMillis() - firstConsole)/1000);
				LinkedList<String> keys = new LinkedList<String>(map.keySet());
				Collections.sort(keys);
				for (String s : keys) {
					System.out.println(s + ":" + map.get(s));
				}
				
				System.out.println("Source update frequencies:");
				LinkedList<Long> sources = new LinkedList<Long>(sourceMap.keySet());
				Collections.sort(sources);
				int c = 0;
				for (Long id : sources) {
					System.out.println(id + ":" + sourceMap.get(id));
					if (c++ > 10) {
						break;
					}
				}
				lastConsole = System.currentTimeMillis();
				
				/*========= Thu Apr 21 13:36:58 ICT 2016 / after (sec): 597
						com.betbrain.sepc.connector.sportsmodel.BettingOffer:1123
						com.betbrain.sepc.connector.sportsmodel.Event:526
						com.betbrain.sepc.connector.sportsmodel.EventInfo:332
						com.betbrain.sepc.connector.sportsmodel.EventParticipantRelation:572
						com.betbrain.sepc.connector.sportsmodel.Outcome:18053
						com.betbrain.sepc.connector.sportsmodel.Participant:2
						com.betbrain.sepc.connector.sportsmodel.ParticipantRelation:100405
						com.betbrain.sepc.connector.sportsmodel.ParticipantUsage:2
						com.betbrain.sepc.connector.sportsmodel.ProviderEventRelation:161
						com.betbrain.sepc.connector.sportsmodel.Source:16287
						Source update frequencies:
						82577:10
						82598:10
						82599:9
						82600:9
						82601:9
						82602:9
						82603:9
						82605:9
						82606:9
						82607:9
						82608:9
						82609:9
						*/
			}
		}
		first = false;
	}
	
	private static long firstConsole = System.currentTimeMillis();
	private static long lastConsole;
	private static HashMap<String, Integer> map = new HashMap<String, Integer>();
	private static HashMap<Long, Integer> sourceMap = new HashMap<Long, Integer>();

	public void notifyInitialDump(List<? extends Entity> entityList) {
		System.out.println("notifyInitialDump: " + entityList.size());
		//run1: Read batch [1] containing 81 entities. [153 batches left]
		//run2: Read batch [1] containing 81 entities. [153 batches left]
		//next day
		//run3: Read batch [1] containing 81 entities. [159 batches left]
		//run3: notifyInitialDump: 2514123
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for (Entity e : entityList) {
			Integer c = map.get(e.getClass().getName());
			if (c == null) {
				c = 0;
				System.out.println(map.size() + ": First of " + e.getClass().getName());
			}
			map.put(e.getClass().getName(), ++c);
			//System.out.println(e);
		}
		
		for (Entry<String, Integer> entry : map.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
		/*
		notifyInitialDump: 2466496
		0: First of com.betbrain.sepc.connector.sportsmodel.Sport
		1: First of com.betbrain.sepc.connector.sportsmodel.Currency
		2: First of com.betbrain.sepc.connector.sportsmodel.LocationType
		3: First of com.betbrain.sepc.connector.sportsmodel.Location
		4: First of com.betbrain.sepc.connector.sportsmodel.LocationRelationType
		5: First of com.betbrain.sepc.connector.sportsmodel.LocationRelation
		6: First of com.betbrain.sepc.connector.sportsmodel.Provider
		7: First of com.betbrain.sepc.connector.sportsmodel.BettingType
		8: First of com.betbrain.sepc.connector.sportsmodel.ParticipantType
		9: First of com.betbrain.sepc.connector.sportsmodel.ParticipantRole
		10: First of com.betbrain.sepc.connector.sportsmodel.Participant
		11: First of com.betbrain.sepc.connector.sportsmodel.ParticipantUsage
		12: First of com.betbrain.sepc.connector.sportsmodel.EventType
		13: First of com.betbrain.sepc.connector.sportsmodel.EventPart
		14: First of com.betbrain.sepc.connector.sportsmodel.EventStatus
		15: First of com.betbrain.sepc.connector.sportsmodel.EventTemplate
		16: First of com.betbrain.sepc.connector.sportsmodel.Event
		17: First of com.betbrain.sepc.connector.sportsmodel.EventPartDefaultUsage
		18: First of com.betbrain.sepc.connector.sportsmodel.EntityType
		19: First of com.betbrain.sepc.connector.sportsmodel.EntityPropertyType
		20: First of com.betbrain.sepc.connector.sportsmodel.EntityProperty
		21: First of com.betbrain.sepc.connector.sportsmodel.EventParticipantRelation
		22: First of com.betbrain.sepc.connector.sportsmodel.Source
		23: First of com.betbrain.sepc.connector.sportsmodel.ParticipantRelationType
		24: First of com.betbrain.sepc.connector.sportsmodel.ParticipantRelation
		25: First of com.betbrain.sepc.connector.sportsmodel.ProviderEventRelation
		26: First of com.betbrain.sepc.connector.sportsmodel.OutcomeStatus
		27: First of com.betbrain.sepc.connector.sportsmodel.OutcomeType
		28: First of com.betbrain.sepc.connector.sportsmodel.OutcomeTypeBettingTypeRelation
		29: First of com.betbrain.sepc.connector.sportsmodel.BettingOfferStatus
		30: First of com.betbrain.sepc.connector.sportsmodel.Outcome
		31: First of com.betbrain.sepc.connector.sportsmodel.BettingOffer
		32: First of com.betbrain.sepc.connector.sportsmodel.EventActionType
		33: First of com.betbrain.sepc.connector.sportsmodel.EventActionDetailType
		34: First of com.betbrain.sepc.connector.sportsmodel.EventInfoType
		35: First of com.betbrain.sepc.connector.sportsmodel.EventParticipantInfoType
		36: First of com.betbrain.sepc.connector.sportsmodel.EventParticipantInfoDetailType
		37: First of com.betbrain.sepc.connector.sportsmodel.EventInfo
		com.betbrain.sepc.connector.sportsmodel.Currency: 5
		com.betbrain.sepc.connector.sportsmodel.Event: 28412
		com.betbrain.sepc.connector.sportsmodel.LocationRelationType: 1
		com.betbrain.sepc.connector.sportsmodel.LocationRelation: 253
		com.betbrain.sepc.connector.sportsmodel.ProviderEventRelation: 1995
		com.betbrain.sepc.connector.sportsmodel.Source: 62568
		com.betbrain.sepc.connector.sportsmodel.EventParticipantInfoType: 2
		com.betbrain.sepc.connector.sportsmodel.EventStatus: 8
		com.betbrain.sepc.connector.sportsmodel.OutcomeStatus: 5
		com.betbrain.sepc.connector.sportsmodel.BettingOffer: 159178
		com.betbrain.sepc.connector.sportsmodel.OutcomeType: 314
		com.betbrain.sepc.connector.sportsmodel.ParticipantRole: 5
		com.betbrain.sepc.connector.sportsmodel.EventActionType: 38
		com.betbrain.sepc.connector.sportsmodel.ParticipantType: 5
		com.betbrain.sepc.connector.sportsmodel.Participant: 484106
		com.betbrain.sepc.connector.sportsmodel.EventTemplate: 17085
		com.betbrain.sepc.connector.sportsmodel.ParticipantRelationType: 1
		com.betbrain.sepc.connector.sportsmodel.OutcomeTypeBettingTypeRelation: 314
		com.betbrain.sepc.connector.sportsmodel.EventType: 2
		com.betbrain.sepc.connector.sportsmodel.Location: 262
		com.betbrain.sepc.connector.sportsmodel.EventPartDefaultUsage: 124
		com.betbrain.sepc.connector.sportsmodel.EntityType: 7
		com.betbrain.sepc.connector.sportsmodel.EventParticipantInfoDetailType: 4
		com.betbrain.sepc.connector.sportsmodel.EventInfo: 103
		com.betbrain.sepc.connector.sportsmodel.EntityProperty: 2
		com.betbrain.sepc.connector.sportsmodel.BettingOfferStatus: 6
		com.betbrain.sepc.connector.sportsmodel.EventInfoType: 99
		com.betbrain.sepc.connector.sportsmodel.Outcome: 953635
		com.betbrain.sepc.connector.sportsmodel.EventPart: 455
		com.betbrain.sepc.connector.sportsmodel.Sport: 81
		com.betbrain.sepc.connector.sportsmodel.LocationType: 4
		com.betbrain.sepc.connector.sportsmodel.ParticipantRelation: 119764
		com.betbrain.sepc.connector.sportsmodel.EventParticipantRelation: 152052
		com.betbrain.sepc.connector.sportsmodel.EventActionDetailType: 11
		com.betbrain.sepc.connector.sportsmodel.EntityPropertyType: 7
		com.betbrain.sepc.connector.sportsmodel.Provider: 745
		com.betbrain.sepc.connector.sportsmodel.BettingType: 309
		com.betbrain.sepc.connector.sportsmodel.ParticipantUsage: 484529
		*/
	}

}
