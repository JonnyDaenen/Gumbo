package guardedfragment.structure;

import guardedfragment.booleanstructure.BAndExpression;
import guardedfragment.booleanstructure.BExpression;
import guardedfragment.booleanstructure.BVariable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GFAtomicExpression implements GFExpression {

	String relation;
	String[] variables;

	public GFAtomicExpression(String relationName, String... variables) {
		this.relation = relationName;
		this.variables = variables;

	}

	@Override
	/**
	 * Forms a tuple using the context information and checks if this tuple exists.
	 * 
	 * @return true iff the tuple derived from the context exists
	 */
	public boolean evaluate(GFEvaluationContext c) {
		// get the required variables
		String[] values = new String[variables.length];

		for (int i = 0; i < values.length; i++) {
			values[i] = c.lookupValue(variables[i]);

		}

		return c.lookupTuple(relation, values);
	}

	@Override
	/**
	 * @return the set of variables appearing in the formula
	 */
	public Set<String> getFreeVariables() {
		List<String> varlist = Arrays.asList(variables);
		Set<String> freevars = new HashSet<String>(varlist);

		return freevars;
	}

	@Override
	public String generateString() {
		// TODO Auto-generated method stub
		return relation + "(" + generateVarString() + ")";
	}

	private String generateVarString() {
		String list = "";
		for (String v : variables)
			list += "," + v;

		return list.substring(1);
	}

	@Override
	public boolean isGuarded() {
		return true;
	}

	@Override
	public boolean isAtomicBooleanCombination() {
		return true;
	}

	@Override
	public BExpression convertToBExpression(GFBMapping m) {
		BVariable v = m.getVariable(this);
		return v;
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return relation.hashCode() + variables.length;
	}

	@Override
	public boolean equals(Object obj) {

		if (obj instanceof GFAtomicExpression) {
			GFAtomicExpression aex = (GFAtomicExpression) obj;
			if (aex.variables.length != variables.length
					|| !aex.relation.equals(relation))
				return false;

			for (int i = 0; i < variables.length; i++) {
				if (!variables[i].equals(aex.variables[i]))
					return false;
			}
			return true;

		}
		return super.equals(obj);
	}
}
