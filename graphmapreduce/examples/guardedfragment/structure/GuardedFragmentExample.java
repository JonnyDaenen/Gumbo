package guardedfragment.structure;

import guardedfragment.booleanstructure.BExpression;

import java.util.HashMap;
import java.util.HashSet;

public class GuardedFragmentExample {

	public static void main(String[] args) {
		
		//testing2();
		
		String s = new String("relation(1,4,6,for,asb,edt)");
		
		MyTuple t = new MyTuple(s);
			
		GFAtomicExpression R = new GFAtomicExpression("relation","x1","x2","x1","y1","y2","y3");
		
		System.out.println(t.belongsTo(R));
		
		
		
	}
	
	private static void testing2() {
		GFAtomicExpression R = new GFAtomicExpression("R","x1","x2","y1","y2","y3");
		GFAtomicExpression S = new GFAtomicExpression("S","x1","y1");
		GFAtomicExpression T = new GFAtomicExpression("T","y1","y2");
		GFAtomicExpression U = new GFAtomicExpression("U","y2","y3");
		GFAtomicExpression V = new GFAtomicExpression("V","y3","x2");
		GFAtomicExpression W = new GFAtomicExpression("W","x2","y1");
		
		HashMap<Integer,Integer> f1 = getVariableMapping(R,S);
		HashMap<Integer,Integer> f2 = getVariableMapping(R,T);
		HashMap<Integer,Integer> f3 = getVariableMapping(R,U);
		HashMap<Integer,Integer> f4 = getVariableMapping(R,V);
		HashMap<Integer,Integer> f5 = getVariableMapping(R,W);
		
		System.out.println(f1.toString());
		System.out.println(f2.toString());
		System.out.println(f3.toString());
		System.out.println(f4.toString());
		System.out.println(f5.toString());
		

		
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
	
	
	private static HashMap<Integer,Integer> getVariableMapping(GFAtomicExpression gf1, GFAtomicExpression gf2){
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

