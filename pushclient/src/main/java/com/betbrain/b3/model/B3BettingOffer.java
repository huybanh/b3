package com.betbrain.b3.model;

import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.data.B3KeyOffer;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.BettingOfferStatus;
import com.betbrain.sepc.connector.sportsmodel.BettingType;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Outcome;
import com.betbrain.sepc.connector.sportsmodel.Provider;

public class B3BettingOffer extends B3Entity<BettingOffer/*, B3KeyOffer*/> {
	
	public B3Provider provider;
	
	//public B3Source source;
	
	public B3Outcome outcome;
	
	public B3BettingType bettingType;
	
	public B3BettingOfferStatus status;
	
	@Override
	public EntitySpec2 getSpec() {
		return EntitySpec2.BettingOffer;
	}

	@Override
	public void load(Item item, String cellName, JsonMapper mapper) {
		super.load(item, cellName, mapper);
		
		String baseCellName;
		if (cellName == null) {
			baseCellName = "";
		} else {
			baseCellName = cellName + B3Table.CELL_LOCATOR_SEP;
		}
		outcome = new B3Outcome();
		outcome.load(item, baseCellName + BettingOffer.PROPERTY_NAME_outcomeId, mapper);
		
		provider = new B3Provider();
		provider.load(item, baseCellName + BettingOffer.PROPERTY_NAME_providerId, mapper);
		
		bettingType = new B3BettingType();
		bettingType.load(item, baseCellName + BettingOffer.PROPERTY_NAME_bettingTypeId, mapper);
		
		status = new B3BettingOfferStatus();
		status.load(item, baseCellName + BettingOffer.PROPERTY_NAME_statusId, mapper);
	}

	@Override
	public void getDownlinkedEntitiesInternal() {
		
		//unfollowed links
		//addDownlinkUnfollowed(BettingOffer.PROPERTY_NAME_outcomeId, Outcome.class/*, entity.getOutcomeId()*/);
		
		addDownlink(BettingOffer.PROPERTY_NAME_outcomeId, Outcome.class, outcome);
		addDownlink(BettingOffer.PROPERTY_NAME_providerId, Provider.class, provider);
		//addDownlink(BettingOffer.PROPERTY_NAME_sourceId, Source.class, source);
		addDownlink(BettingOffer.PROPERTY_NAME_bettingTypeId, BettingType.class, bettingType);
		addDownlink(BettingOffer.PROPERTY_NAME_statusId, BettingOfferStatus.class, status);
	}
	
	@Override
	public void buildDownlinks(boolean forMainKeyOnly, HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {

		this.outcome = build(forMainKeyOnly, entity.getOutcomeId(), new B3Outcome(), 
				Outcome.class, masterMap, mapper);
		if (forMainKeyOnly) {
			return;
		}
		this.provider = build(forMainKeyOnly, entity.getProviderId(), new B3Provider(), 
				Provider.class, masterMap, mapper);
		//this.source = build(forMainKeyOnly, entity.getSourceId(), new B3Source(), 
		//		Source.class, masterMap, mapper);
		this.bettingType = build(forMainKeyOnly, entity.getBettingTypeId(), 
				new B3BettingType(), BettingType.class, masterMap, mapper);
		this.status = build(forMainKeyOnly, entity.getStatusId(), 
				new B3BettingOfferStatus(), BettingOfferStatus.class, masterMap, mapper);
	}
	
	@Override
	String canCreateMainKey() {
		if (entity == null) {
			return "Null entity";
		}
		if (outcome == null) {
			return "Missing outcome " + entity.getOutcomeId();
		}
		return null;
	}

	@Override
	public B3KeyOffer createMainKey() {
		if (entity == null || outcome == null) {
			return null;
		}
		return new B3KeyOffer(/*outcome.event.entity.getSportId(),
				outcome.event.entity.getTypeId(),*/
				outcome.event.entity.getId(),
				this.entity.getBettingTypeId(),
				outcome.entity.getEventPartId(),
				outcome.entity.getTypeId(),
				outcome.entity.getId(),
				this.entity.getId());
		
	}
	
	@Override
	String getRevisionId() {
		return String.valueOf(this.entity.getLastChangedTime().getTime());
	}
}
