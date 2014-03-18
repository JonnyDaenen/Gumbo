package guardedfragment.booleanstructure;

import java.util.HashMap;
import java.util.Map;

public class BEvaluationContext {
	
	
	Map<Integer, Boolean> valuemap;

	public BEvaluationContext() {
		valuemap = new HashMap<Integer, Boolean>();
	}

	public boolean lookupValue(BVariable v) throws VariableNotFoundException {
		return lookupValue(v.id);
	}
	
	
	public boolean lookupValue(int id) throws VariableNotFoundException {
		
		if(!valuemap.containsKey(id))
			throw new VariableNotFoundException("Variable with id "+id+" was not found in the value map!");
		
		return valuemap.get(id);
	}

	public void setValue(BVariable v, boolean b) {
		setValue(v.id, b);
	}
	
	public void setValue(int id, boolean b) {
		valuemap.put(id, b);
	}

}
