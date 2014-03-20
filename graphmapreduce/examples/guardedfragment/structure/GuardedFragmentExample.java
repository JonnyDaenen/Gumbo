package guardedfragment.structure;

import guardedfragment.booleanstructure.BExpression;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class GuardedFragmentExample {

	public static void main(String[] args) {
		
		testing2();
		
		//
			
		//GFAtomicExpression R = new GFAtomicExpression("relation","x1","x2","x1","y1","y2","y3");
		
		//System.out.println(t.belongsTo(R));
		
		
		
		
	}
	
	
	private static void testing2() {
		GFAtomicExpression R = new GFAtomicExpression("Relation","x1","x2","y1","y2","y3");
		GFAtomicExpression S = new GFAtomicExpression("S","x1","y1");
		GFAtomicExpression T = new GFAtomicExpression("T","y1","y2");
		GFAtomicExpression U = new GFAtomicExpression("U","y2","y3");
		GFAtomicExpression V = new GFAtomicExpression("V","y3","x2");
		GFAtomicExpression W = new GFAtomicExpression("W","x2","y1");
		
		//System.out.println(R.noVariables());
		//System.out.println(S.noVariables());
		//System.out.println(T.noVariables());
		//System.out.println(U.noVariables());
		//System.out.println(V.noVariables());
		
		GFNotExpression NotT = new GFNotExpression(T);
		GFNotExpression NotV = new GFNotExpression(V);
		GFNotExpression NotW = new GFNotExpression(W);
		
		GFAndExpression AndST = new GFAndExpression(S,NotT);
		GFOrExpression OrUV = new GFOrExpression(U,NotV);

		GFAndExpression AndSTUV = new GFAndExpression(AndST,OrUV);
		GFAndExpression STUVW = new GFAndExpression(AndSTUV,NotW);
		
		MyMapper mymapper = new MyMapper(R,STUVW);
		
		Set<KeyValuePair> p = mymapper.getKeyValuePair(new String("S(1,4)"));
		KeyValuePair[] arrayp = p.toArray(new KeyValuePair[0]);
		
		for(int i=0;i< arrayp.length;i++) {
			System.out.println(arrayp[i].getKey()+ " : " +arrayp[i].getValue());
		}
		
	}
	

	
	private static void testing1() {

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
	
	

}

