package com.betbrain.b3.data;

public abstract class B3Key {
	
	private String revisionId;
	
	abstract boolean isDetermined();
	
	abstract String getRangeKeyInternal();
	
	abstract String getHashKeyInternal();
	
	final String getRangeKey() {
		if (revisionId != null) {
			return getRangeKeyInternal() + B3Table.KEY_SEP + revisionId;
		} else {
			return getRangeKeyInternal();
		}
	}
	
	public final String getHashKey() {
		if (revisionId != null) {
			return getHashKeyInternal() + B3Table.KEY_SUFFIX_REVISION;
		} else {
			return getHashKeyInternal();
		}
	}
	
	/*protected int module(int l, int m) {
		return Math.abs(l % m);
	}*/
	
	public void setRevisionId(String revisionId) {
		this.revisionId = revisionId;
	}
}
