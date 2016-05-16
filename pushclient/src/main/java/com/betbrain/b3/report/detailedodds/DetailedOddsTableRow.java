package com.betbrain.b3.report.detailedodds;

import java.util.HashMap;

import com.betbrain.sepc.connector.util.beans.BeanUtil;

public class DetailedOddsTableRow {

	private long time;
	
	private HashMap<Long, String> odds = new HashMap<>();
	private HashMap<Long, String> scores = new HashMap<>();
	private HashMap<Long, String> statuses = new HashMap<>();
	
	void addOdds(Long providerId, String value) {
		odds.put(providerId, value);
	}
	
	void addScore(Long providerId, String value) {
		scores.put(providerId, value);
	}
	
	void addStatus(Long providerId, String value) {
		statuses.put(providerId, value);
	}
	
	public long getTime() {
		return time;
	}
	
	public void setTime(long time) {
		this.time = time;
	}
	
	public HashMap<Long, String> getOdds() {
		return odds;
	}
	
	public void setOdds(HashMap<Long, String> odds) {
		this.odds = odds;
	}
	
	public HashMap<Long, String> getScores() {
		return scores;
	}
	
	public void setScores(HashMap<Long, String> scores) {
		this.scores = scores;
	}
	
	public HashMap<Long, String> getStatuses() {
		return statuses;
	}
	
	public void setStatuses(HashMap<Long, String> statuses) {
		this.statuses = statuses;
	}
	
	@Override
	public String toString() {
		return BeanUtil.toString(this);
	}
	
}
