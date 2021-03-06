package gumbo.structures.conversion;

import java.util.HashMap;
import java.util.Map;

import gumbo.structures.booleanexpressions.BVariable;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;


/**
 * Representation of a mapping between atomic values of GFExpressions to variables of BExpressions.
 * 
 * @author Jonny Daenen
 *
 */
public class GFBooleanMapping {

	public class AtomNotFoundException extends Exception {

		public AtomNotFoundException(String msg) {
			super(msg);
		}

	}

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

	public BVariable getVariableIfExists(GFAtomicExpression aex) throws AtomNotFoundException {

		BVariable v;

		// create new var if necessary  
		if(!mapping.containsKey(aex)) {
			throw new AtomNotFoundException("Atom " + aex + " not found in boolean mapping.");
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

	/**
	 * Reverse lookup of boolean variable
	 * @param e
	 * @return the first match found
	 * 
	 * @pre the variable appears in the mapping
	 */
	public GFExpression getAtomic(BVariable e) {
		for (GFExpression key : mapping.keySet()) {
			BVariable val = mapping.get(key);
			if(val.equals(e))
				return key;
		}

		return null;
	}

}
