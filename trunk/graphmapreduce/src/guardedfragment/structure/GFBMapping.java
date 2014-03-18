package guardedfragment.structure;

import java.util.HashMap;
import java.util.Map;

import guardedfragment.booleanstructure.BVariable;


/**
 * Representation of a mapping between atomic values of GFExpressions to variables of BExpressions.
 * 
 * @author Jonny Daenen
 *
 */
public class GFBMapping {
	
	int nextId;
	Map<GFAtomicExpression, BVariable> mapping;
	
	public GFBMapping() {
		mapping = new HashMap<GFAtomicExpression, BVariable>();
		nextId = 0;
	}

	public BVariable getVariable(GFAtomicExpression aex) {
		
		BVariable v;
		
		// create new var if necessary  
		if(!mapping.containsKey(aex)) {
			v = new BVariable(nextId);
			mapping.put(aex, v);
			// update id counter
			nextId++;
		} else {
			v = mapping.get(aex);
		}
			
		
		return v;
	}
	
	@Override
	public String toString() {
		String s = "";
		
		for(GFAtomicExpression aex : mapping.keySet()) {
			BVariable v = mapping.get(aex);
			s += aex.generateString() + " -> " + v.generateString() + "\n";
		}
		return s;
	}

}
