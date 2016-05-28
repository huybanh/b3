package com.betbrain.b3.api;

public class OutcomeParameter {
	
	public final String name;

	public final String value;

	public String valueName;
	
	public boolean primaryParam = true;
	
	public OutcomeParameter(String name, String value) {
		super();
		this.name = name;
		this.value = value;
	}
	
	@Override
	public String toString() {
		return "name=" + name + ", value=" + value;
	}
    
    /* ----------------------------------------------------------------------------------------- */

	@Override
    public int hashCode() {
        
    	int n = value.hashCode();
        return name.hashCode() ^ (n ^ (n >>> 32));
    }

    /* ----------------------------------------------------------------------------------------- */

	@Override
    public boolean equals(Object object) {

        if (object == null)
            return false;
        
        if (!getClass().equals(object.getClass()))
            return false;
        
        //name is not null
        OutcomeParameter other = (OutcomeParameter) object;
        return name.equals(other.name) && value.equals(other.value);
    }

}
