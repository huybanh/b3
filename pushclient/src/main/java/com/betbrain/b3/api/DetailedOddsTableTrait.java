package com.betbrain.b3.api;

import java.util.ArrayList;
import java.util.HashMap;

public interface DetailedOddsTableTrait {

	String getCaption();

	HashMap<Long, String> getOddsProviderNames();

	HashMap<Long, String> getScoreProviderNames();

	HashMap<Long, String> getStatusProviderNames();

	ArrayList<DetailedOddsTableRowTrait> getRows();

}
