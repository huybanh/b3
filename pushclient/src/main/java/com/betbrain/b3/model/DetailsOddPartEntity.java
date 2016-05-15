package com.betbrain.b3.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.betbrain.sepc.connector.sportsmodel.Entity;

public class DetailsOddPartEntity {
	
	private String caption;

	private List<HashMap<String, ArrayList<ItemProvider>>> rowData;
	
	public String getCaption() {
		return caption;
	}

	public void setCaption(String caption) {
		this.caption = caption;
	}

	public List<HashMap<String, ArrayList<ItemProvider>>> getRowData() {
		return rowData;
	}

	public void setRowData(List<HashMap<String, ArrayList<ItemProvider>>> rowData) {
		this.rowData = rowData;
	}
	
	public DetailsOddPartEntity() {
		rowData = new ArrayList<HashMap<String, ArrayList<ItemProvider>>>();
		caption = "Default";
	}
	
	public DetailsOddPartEntity(String caption) {
		rowData = new ArrayList<HashMap<String, ArrayList<ItemProvider>>>();
		this.caption = caption;
		
	}

}
