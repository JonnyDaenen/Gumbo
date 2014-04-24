package guardedfragment.structure;

import guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.data.Tuple;

public class FirstReducer {
	
	Tuple key;
	GFAtomicExpression guard;
	
	public FirstReducer(String s, GFAtomicExpression gf) {
		key = new Tuple(s);
		guard = gf;
	}

	public void readValue(String s) {
//		Tuple value = new Tuple(s);
		// TODO
		
	}
	
	
}
