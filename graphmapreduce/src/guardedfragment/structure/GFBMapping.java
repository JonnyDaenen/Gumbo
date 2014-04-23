package guardedfragment.structure;

import guardedfragment.booleanstructure.BVariable;
import guardedfragment.structure.expressions.GFAtomicExpression;
import guardedfragment.structure.expressions.GFExistentialExpression;
import guardedfragment.structure.expressions.GFExpression;

import java.util.HashMap;
import java.util.Map;


/**
 * Representation of a mapping between atomic values of GFExpressions to variables of BExpressions.
 * 
 * @author Jonny Daenen
 *
 */
public class GFBMapping {
	
	int nextId;
	Map<GFExpression, BVariable> mapping;
	
	public GFBMapping() {
		mapping = new HashMap<GFExpression, BVariable>();
		nextId = 0;
	}
	
	public void insertElement(GFExistentialExpression gf){
		BVariable v;
		
		if (!mapping.containsKey(gf)) {
			v = new BVariable(nextId);
			mapping.put(gf, v);
			// update id counter
			nextId++;	
		}
	}
	
	public void insertElement(GFAtomicExpression gf){
		BVariable v;
		
		if (!mapping.containsKey(gf)) {
			v = new BVariable(nextId);
			mapping.put(gf, v);
			// update id counter
			nextId++;	
		}
	}
	
	
	public BVariable getVariable(GFExistentialExpression aex) {
		
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
		
		for(GFExpression aex : mapping.keySet()) {
			BVariable v = mapping.get(aex);
			s += aex.generateString() + " -> " + v.generateString() + "\n";
		}
		return s;
	}

}
