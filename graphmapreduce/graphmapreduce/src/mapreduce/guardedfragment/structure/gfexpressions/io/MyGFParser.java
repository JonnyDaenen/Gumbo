package mapreduce.guardedfragment.structure.gfexpressions.io;

import mapreduce.guardedfragment.structure.gfexpressions.GFAndExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFNotExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFOrExpression;

/**
 * 
 * @author Tony Tan
 *
 */
@Deprecated
public class MyGFParser {
	
	String formula;
	int start;
	
	public MyGFParser(String s) {
		formula = s;
		start = 0;
	}
	
	public GFExpression deserialize() throws DeserializeException {
		if(start >= formula.length()) {
			throw new DeserializeException("The string has been parsed");
		}
		
		
		// negation
		if(formula.charAt(start) == '!') {
			start = start+1;
			GFExpression g = deserialize();
			return new GFNotExpression(g);
		}
		
		// or
		if(formula.charAt(start) == '|') {
			start = start+1;
			GFExpression g1 = deserialize();
			GFExpression g2 = deserialize();
			return new GFOrExpression(g1,g2);
		}
		
		// and
		if(formula.charAt(start) == '&') {
			start = start+1;
			GFExpression g1 = deserialize();
			GFExpression g2 = deserialize();
			return new GFAndExpression(g1,g2);
		}
		if(formula.charAt(start) == '=') {
			start = start+1;
			GFAtomicExpression gout = deserializeGFAtom();
			if(formula.charAt(start) != '^') {
				throw new DeserializeException("Wrong syntax for existentialGFExpression at position "+start);
			}
			start = start+1;
			GFAtomicExpression guard = deserializeGFAtom();
			GFExpression guarded = deserialize();
			return new GFExistentialExpression(guard,guarded,gout);
		}
		
		return deserializeGFAtom();
	}
	
	
	public GFAtomicExpression deserializeGFAtom() throws DeserializeException {
		int k1 = formula.indexOf('(', start);
		if(k1 == -1) 
			throw new DeserializeException("Expecting ( after position " + start);
		
		int k2 = formula.indexOf(')',k1);
		if(k2 == -1)
			throw new DeserializeException("Expecting ) after position " + k1);
		
		String name = formula.substring(start, k1);
		if(!name.matches("[a-zA-Z0-9]+")) {
			throw new DeserializeException("Expecting only alphanumeric symbols for relation names");
		}
		String rest = formula.substring(k1 + 1, k2);
		String[] vars = rest.split(",");

		start = k2+1;

		return new GFAtomicExpression(name, vars);
	}
	



}
