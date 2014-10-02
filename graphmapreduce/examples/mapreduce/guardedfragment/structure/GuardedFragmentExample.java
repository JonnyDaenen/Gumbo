package mapreduce.guardedfragment.structure;

import java.util.Set;

import mapreduce.guardedfragment.planner.structures.data.KeyValuePair;
import mapreduce.guardedfragment.structure.gfexpressions.GFAndExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFNotExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFOrExpression;

public class GuardedFragmentExample {

	public static void main(String[] args) {
		
		testing2();	
		
	}
	
	
	private static void testing2() {
		GFAtomicExpression R = new GFAtomicExpression("Relation","x1","x2","y1","y2","y3");
		GFAtomicExpression S = new GFAtomicExpression("S","x1","y1");
		GFAtomicExpression T = new GFAtomicExpression("T","y1","y2");
		GFAtomicExpression U = new GFAtomicExpression("U","y2","y3");
		GFAtomicExpression V = new GFAtomicExpression("V","y3","x2");
		GFAtomicExpression W = new GFAtomicExpression("S","x1","y1");
		

		GFNotExpression NotT = new GFNotExpression(T);
		GFNotExpression NotV = new GFNotExpression(V);
		GFNotExpression NotW = new GFNotExpression(W);
		
		GFAndExpression AndST = new GFAndExpression(S,NotT);
		GFOrExpression OrUV = new GFOrExpression(U,NotV);

		GFAndExpression AndSTUV = new GFAndExpression(AndST,OrUV);
		GFAndExpression STUVW = new GFAndExpression(AndSTUV,NotW);
		
		FirstMapper mymapper = new FirstMapper(R,STUVW);
		
		Set<KeyValuePair> p = mymapper.getKeyValuePair(new String("Relation(1,4,oe,950,938)"));
		KeyValuePair[] arrayp = p.toArray(new KeyValuePair[0]);
		
		for(int i=0;i< arrayp.length;i++) {
			arrayp[i].printPair();
		}
		
	}
	

	
/*	private static void testing1() {

		GFAtomicExpression R = new GFAtomicExpression("R","x1","x2","y1","y2","y3");
		GFAtomicExpression S = new GFAtomicExpression("S","x1","y1");
		GFAtomicExpression T = new GFAtomicExpression("T","y1","y2");
		GFAtomicExpression U = new GFAtomicExpression("U","y2","y3");
		GFAtomicExpression V = new GFAtomicExpression("V","y3","x2");
		GFAtomicExpression W = new GFAtomicExpression("W","x2","y1");
		
		System.out.println(R.noVariables());
		System.out.println(S.noVariables());
		System.out.println(T.noVariables());
		System.out.println(U.noVariables());
		System.out.println(V.noVariables());
		
		GFNotExpression NotT = new GFNotExpression(T);
		GFNotExpression NotV = new GFNotExpression(V);
		GFNotExpression NotW = new GFNotExpression(W);
		
		GFAndExpression AndST = new GFAndExpression(S,NotT);
		GFOrExpression OrUV = new GFOrExpression(U,NotV);

		GFAndExpression AndSTUV = new GFAndExpression(AndST,OrUV);
		GFAndExpression STUVW = new GFAndExpression(AndSTUV,NotW);
		
		GFExistentialExpression gf = new GFExistentialExpression(R,STUVW,"y1","y2","y3");
		System.out.println(gf.generateString());
		
		try {
			BExpression prop = convert(STUVW);
			System.out.println(prop.generateString());			
		} catch (GFConversionException e) {
			e.printStackTrace();
		}
		
	}
	
	
	private static BExpression convert(GFExpression gfe2) throws GFConversionException {
		GFBMapping m = new GFBMapping();
		BExpression bex = gfe2.convertToBExpression(m);
		return bex;
	} 
*/	
	

}

