package guardedfragment.structure;

import guardedfragment.booleanstructure.BExpression;

public class GuardedFragmentExample {

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
			GFBMapping m = new GFBMapping();
			BExpression bex = gfe2.convertToBExpression(m);
			System.out.println(bex.generateString());
			System.out.println(m);
		} catch (GFConversionException e) {
			e.printStackTrace();
		}
	}

}
