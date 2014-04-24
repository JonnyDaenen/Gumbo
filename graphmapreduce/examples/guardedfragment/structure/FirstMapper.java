package guardedfragment.structure;

import guardedfragment.structure.gfexpressions.GFAtomicExpression;
import guardedfragment.structure.gfexpressions.GFExpression;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import mapreduce.data.KeyValuePair;
import mapreduce.data.Tuple;


public class FirstMapper {
	
	GFAtomicExpression guard;
	GFExpression child;
	
	private GFAtomicExpression[] arrayAllAtoms;
	
	public FirstMapper(GFAtomicExpression g, GFExpression f) {
		guard = g;
		child = f;
		Set<GFAtomicExpression> setAllAtoms = child.getAtomic();
		arrayAllAtoms = setAllAtoms.toArray(new GFAtomicExpression[0]);
	}
	
	public Set<KeyValuePair> getKeyValuePair(String s) {
		Tuple t = new Tuple(s);
		
		// check if tuple satisfies equality type
		if (guard.matches(t)) {
			return getKeyValuePairByGuard(t);
		}
	
		// if not, then check which guared relations it matches
		// and output the tuple
		Set<KeyValuePair> p = new HashSet<KeyValuePair>();		
		for (int i =0; i < arrayAllAtoms.length;i++) {
			if (t.getName().equals(arrayAllAtoms[i].getName())) {
				p.add(new KeyValuePair(s,s));
			}
		}
		return p;
	}
	
	private Set<KeyValuePair> getKeyValuePairByGuard(Tuple t) {
		Set<KeyValuePair> p = new HashSet<KeyValuePair>();		
		
		HashMap<Integer,Integer> f = new HashMap<Integer,Integer>();
		String tkey = new String();
		
		// get mapping from guard relation to each guarded relation
		// and project it
		for (int i =0; i < arrayAllAtoms.length;i++) {
			
			// get mapping
			f = getVariableMapping(guard,arrayAllAtoms[i]);
			
			// project
			tkey = t.getData(arrayAllAtoms[i].getName(), f);
			
			// add to output
			p.add(new KeyValuePair(tkey,t.generateString()));
		}
		return p;
	}
	
	private HashMap<Integer,Integer> getVariableMapping(GFAtomicExpression gf1, GFAtomicExpression gf2){
		
		HashMap<Integer,Integer> f = new HashMap<Integer,Integer>(gf1.getNumVariables());
		
		String[] vars1 = gf1.getVars();
		String[] vars2 = gf2.getVars();
		
		for(int i=0; i<vars2.length; i++){
			for(int j=0; j<vars1.length;j++){
				if (vars2[i].equals(vars1[j])){
					f.put(i,j); // TODO this is not a set, so is this the correct way?
				}
			}
			
       }
		
		return f;
	}


}
