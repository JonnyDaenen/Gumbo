package mapreduce.guardedfragment.structure.conversion;

import java.util.HashMap;
import java.util.Map;

import mapreduce.guardedfragment.structure.booleanexpressions.BVariable;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;


/**
 * Representation of a mapping between atomic values of GFExpressions to variables of BExpressions.
 * 
 * @author Jonny Daenen
 *
 */
public class GFBooleanMapping {
	
	int nextId;
	Map<GFExpression, BVariable> mapping;
	
	public GFBooleanMapping() {
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