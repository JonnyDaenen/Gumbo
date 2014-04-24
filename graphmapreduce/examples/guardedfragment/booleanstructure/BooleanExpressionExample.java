package guardedfragment.booleanstructure;

import guardedfragment.structure.booleanexpressions.BAndExpression;
import guardedfragment.structure.booleanexpressions.BEvaluationContext;
import guardedfragment.structure.booleanexpressions.BExpression;
import guardedfragment.structure.booleanexpressions.BNotExpression;
import guardedfragment.structure.booleanexpressions.BOrExpression;
import guardedfragment.structure.booleanexpressions.BVariable;
import guardedfragment.structure.booleanexpressions.VariableNotFoundException;

public class BooleanExpressionExample {
	
	public static void main(String[] args) {
		
		BVariable p1 = new BVariable(1);
		BVariable p2 = new BVariable(2);
		BVariable p3 = new BVariable(3);
		
		BExpression be = new BOrExpression(new BAndExpression(p1,p2), new BNotExpression(p3));
		
		BEvaluationContext c = new BEvaluationContext();
		
		System.out.println(be.generateString());
		
		try {
			
			c.setValue(p1,true);
			c.setValue(p2,true);
			c.setValue(p3,true);
			System.out.println(be.evaluate(c )); // true
			
			c.setValue(p1,false);
			c.setValue(p2,false);
			c.setValue(p3,false);
			System.out.println(be.evaluate(c )); // true
			
			c.setValue(p1,true);
			c.setValue(p2,false);
			c.setValue(p3,false);
			System.out.println(be.evaluate(c )); // true
			
			
			c.setValue(p1,false);
			c.setValue(p2,false);
			c.setValue(p3,true);
			System.out.println(be.evaluate(c )); // false
			
			c.setValue(p1,true);
			c.setValue(p2,false);
			c.setValue(p3,true);
			System.out.println(be.evaluate(c )); // false
			
			
		} catch (VariableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
