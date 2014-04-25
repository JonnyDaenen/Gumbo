package guardedfragment.structure.booleanexpressions;

public class BVariable extends BExpression {
	
	int id;
	
	
	public BVariable(int id) {
		this.id = id;
	}



	@Override
	public boolean equals(Object obj) {
		
		if (obj instanceof BVariable) {
			BVariable var2 = (BVariable) obj;
			
			return var2.id == this.id;
			
		} else
			return false;
	}



	@Override
	public boolean evaluate(BEvaluationContext c) throws VariableNotFoundException {
		return c.lookupValue(id);
	}
	
	
	public String generateString() {
		return "v"+id;
	}



}
