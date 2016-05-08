package com.betbrain.b3.data;

public abstract class B3Key {
	
	private String revisionId;
	
	abstract boolean isDetermined();
	
	abstract String getRangeKeyInternal();
	
	abstract String getHashKeyInternal();
	
	public final String getRangeKey() {
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
	
	private static final String ZEROS = "0000000000000000000000000000000000000000000000000000000000";
	
	public static String zeroPadding(int length, long number) {
		String s = String.valueOf(number);
		if (s.length() > length) {
			throw new RuntimeException("Number has more than " + length + " digits: " + number);
		}
		return ZEROS.substring(0, length - s.length()) + s;
	}
	
	public static void main(String[] args) {
		System.out.println(zeroPadding(1, 1));
		System.out.println(zeroPadding(5, 1));
		System.out.println(zeroPadding(5, 23));
	}
}
