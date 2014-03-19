package guardedfragment.structure;

import guardedfragment.booleanstructure.BExpression;

public class GuardedFragmentExample {

	public static void main(String[] args) {
		
		testing();
		
	}

	
	private static void testing() {

		GFAtomicExpression R = new GFAtomicExpression("R","x1","x2","y1","y2","y3");
		GFAtomicExpression S = new GFAtomicExpression("S","x1","y1");
		GFAtomicExpression T = new GFAtomicExpression("T","y1","y2");
		GFAtomicExpression U = new GFAtomicExpression("U","y2","y3");
		GFAtomicExpression V = new GFAtomicExpression("V","y3","x2");
		GFAtomicExpression W = new GFAtomicExpression("W","x2","y2");
		
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
