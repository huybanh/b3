package com.betbrain.b3.report.detailedodds;

import java.util.HashMap;

import com.betbrain.b3.api.DetailedOddsTableRowTrait;
import com.betbrain.sepc.connector.util.beans.BeanUtil;

public class DetailedOddsTableRow implements DetailedOddsTableRowTrait {

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
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.report.detailedodds.DetailedOddsTableRowTrait#getTime()
	 */
	@Override
	public long getTime() {
		return time;
	}
	
	public void setTime(long time) {
		this.time = time;
	}
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.report.detailedodds.DetailedOddsTableRowTrait#getOdds(long)
	 */
	@Override
	public String getOdds(long providerId) {
		return odds.get(providerId);
	}
	
	public HashMap<Long, String> getOddsMap() {
		return odds;
	}
	
	public void setOddsMap(HashMap<Long, String> odds) {
		this.odds = odds;
	}
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.report.detailedodds.DetailedOddsTableRowTrait#getScore(long)
	 */
	@Override
	public String getScore(long providerId) {
		return scores.get(providerId);
	}
	
	public HashMap<Long, String> getScores() {
		return scores;
	}
	
	public void setScores(HashMap<Long, String> scores) {
		this.scores = scores;
	}
	
	/* (non-Javadoc)
	 * @see com.betbrain.b3.report.detailedodds.DetailedOddsTableRowTrait#getStatus(long)
	 */
	@Override
	public String getStatus(long providerId) {
		return statuses.get(providerId);
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
