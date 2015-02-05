package mapreduce.guardedfragment.structure.booleanexpressions;

import java.util.HashMap;
import java.util.Map;

public class BEvaluationContext {
	
	
	Map<Integer, Boolean> valuemap;

	public BEvaluationContext() {
		valuemap = new HashMap<Integer, Boolean>();
	}
	/**
	 * Lookup the truth value of a variable. When it is not found, an exception is thrown.
	 * @param v the variable
	 * @return
	 * @throws VariableNotFoundException when the requested variable is not found
	 */
	public boolean lookupValue(BVariable v) throws VariableNotFoundException {
		return lookupValue(v.id);
	}
	
	
	/**
	 * 
	 * @param id the id of the variable to look up
	 * @return the boolean value of the variable, or false if it's not present.
	 * @throws VariableNotFoundException
	 */
	protected boolean lookupValue(int id) throws VariableNotFoundException {
		
		if(!valuemap.containsKey(id)) {
			return false;
//			throw new VariableNotFoundException("Variable with id "+id+" was not found in the value map!");
		}
		return valuemap.get(id);
	}


	/**
	 * Sets the truth value of the given variable.
	 * @param v
	 * @param b
	 */
	public void setValue(BVariable v, boolean b) {
		setValue(v.id, b);
	}
	
	protected void setValue(int id, boolean b) {
		valuemap.put(id, b);
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return valuemap.toString();
	}

}
