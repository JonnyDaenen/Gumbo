package guardedfragment.structure;

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
		
		if (t.belongsTo(guard)) {
			return getKeyValuePairByGuard(t);
		}
	
		Set<KeyValuePair> p = new HashSet<KeyValuePair>();		
		for (int i =0; i < arrayAllAtoms.length;i++) {
			if (t.getName().equals(arrayAllAtoms[i].relation)) {
				p.add(new KeyValuePair(s,s));
			}
		}
		return p;
	}
	
	private Set<KeyValuePair> getKeyValuePairByGuard(Tuple t) {
		Set<KeyValuePair> p = new HashSet<KeyValuePair>();		
		
		HashMap<Integer,Integer> f = new HashMap<Integer,Integer>();
		String tkey = new String();
		for (int i =0; i < arrayAllAtoms.length;i++) {
			f = getVariableMapping(guard,arrayAllAtoms[i]);
			tkey = t.getData(arrayAllAtoms[i].relation, f);
			p.add(new KeyValuePair(tkey,t.generateString()));
		}
		return p;
	}
	
	private HashMap<Integer,Integer> getVariableMapping(GFAtomicExpression gf1, GFAtomicExpression gf2){
		HashMap<Integer,Integer> f = new HashMap<Integer,Integer>(gf1.noVariables());
		
		String[] vars1 = gf1.variables;
		String[] vars2 = gf2.variables;
		
		for(int i=0; i<vars2.length; i++){
			for(int j=0; j<vars1.length;j++){
				if (vars2[i].equals(vars1[j])){
					f.put(i,j);
				}
			}
			
       }
		
		return f;
	}


}
