package com.betbrain.b3.api;

public interface DetailedOddsTableRowTrait {

	long getTime();

	String getOdds(long providerId);

	String getScore(long providerId);

	String getStatus(long providerId);

}
