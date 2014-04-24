package guardedfragment.structure;

import guardedfragment.structure.booleanexpressions.BExpression;
import guardedfragment.structure.conversion.GFBooleanMapping;
import guardedfragment.structure.conversion.GFtoBooleanConversionException;
import guardedfragment.structure.conversion.GFtoBooleanConvertor;
import guardedfragment.structure.gfexpressions.GFAndExpression;
import guardedfragment.structure.gfexpressions.GFAtomicExpression;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.GFExpression;

public class GuardedFragmentExample2 {

	public static void main(String[] args) {

		GFAtomicExpression rel1 = new GFAtomicExpression("A", "x", "y");
		GFAtomicExpression rel2 = new GFAtomicExpression("B", "y");

		GFExpression gfe = new GFExistentialExpression(rel1, rel2, "y");
		

		System.out.println(gfe.generateString());
		System.out.println(gfe.getFreeVariables());
		System.out.println(gfe.isGuarded());
		System.out.println(gfe.isAtomicBooleanCombination());
		convert(gfe);
		
		GFExpression gfe2 = new GFAndExpression(rel1, rel2);
		System.out.println(gfe2.generateString());
		System.out.println(gfe2.getFreeVariables());
		System.out.println(gfe2.isGuarded());
		System.out.println(gfe2.isAtomicBooleanCombination());
		convert(gfe2);
		
		

		GFAtomicExpression rel3 = new GFAtomicExpression("B", "y");
		GFExpression gfe3 = new GFAndExpression(rel2, rel3);
		System.out.println(gfe3.generateString());
		System.out.println(gfe3.getFreeVariables());
		System.out.println(gfe3.isGuarded());
		System.out.println(gfe3.isAtomicBooleanCombination());
		convert(gfe3);
		
		
		
		

	}

	private static void convert(GFExpression gfe2) {
		try {
			
			GFtoBooleanConvertor convertor = new GFtoBooleanConvertor();
			BExpression bex = convertor.convert(gfe2);
			GFBooleanMapping m = convertor.getMapping();
			
			System.out.println(bex);
			System.out.println(m);
		} catch (GFtoBooleanConversionException e) {
			e.printStackTrace();
		}
	}

}
