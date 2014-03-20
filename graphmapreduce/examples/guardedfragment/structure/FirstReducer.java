package guardedfragment.structure;

public class FirstReducer {
	
	MyTuple key;
	GFAtomicExpression guard;
	
	public FirstReducer(String s, GFAtomicExpression gf) {
		key = new MyTuple(s);
		guard = gf;
	}

	public void readValue(String s) {
		MyTuple value = new MyTuple(s);
		// TODO
		
	}
	
	
}
