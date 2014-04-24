package guardedfragment.structure.gfexpressions;


import guardedfragment.structure.booleanexpressions.BExpression;
import guardedfragment.structure.booleanexpressions.BVariable;
import guardedfragment.structure.conversion.GFBooleanMapping;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mapreduce.data.RelationSchema;
import mapreduce.data.Tuple;

public class GFAtomicExpression extends GFExpression {

	String relation;
	String[] variables;
	int rank;

	public GFAtomicExpression(String relationName, String... variables) {
		this.relation = relationName;
		this.variables = variables;
		this.rank = 0;
	}


	public String getName() {
		return relation;
	}

	public String[] getVars() {
		return variables;
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

	public Set<GFAtomicExpression> getAtomic() {
		Set<GFAtomicExpression> atom = new HashSet<GFAtomicExpression>();
		atom.add(this);
		return atom;
	}

	@Override
	public String generateString() {
		return relation + "(" + generateVarString() + ")";
	}

	@Override
	public String prefixString() {
		return relation + "(" + generateVarString() + ")";
	}

	public int getNumVariables() {
		return variables.length;
	}

	private String generateVarString() {
		String list = "";
		for (String v : variables)
			list += "," + v;

		if (list.length() > 0)
			return list.substring(1);
		else
			return "";
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
	public BExpression convertToBExpression(GFBooleanMapping m) {
		BVariable v = m.getVariable(this);
		return v;
	}

	@Override
	public int hashCode() {
		return relation.hashCode() + variables.length;
	}

	@Override
	public boolean equals(Object obj) {

		if (obj instanceof GFAtomicExpression) {
			GFAtomicExpression aex = (GFAtomicExpression) obj;
			if (aex.variables.length != variables.length || !aex.relation.equals(relation))
				return false;

			for (int i = 0; i < variables.length; i++) {
				if (!variables[i].equals(aex.variables[i]))
					return false;
			}
			return true;

		}
		return super.equals(obj);
	}

	/**
	 * Generates and returns a RelationSchema of this expression.
	 * 
	 * @return the relationschema of this relation
	 */
	public RelationSchema extractRelationSchema() {
		return new RelationSchema(relation, variables.length);
	}

	/**
	 * @return the number of fields of the relation
	 */
	public int getNumFields() {
		return variables.length;
	}

	public boolean matches(Tuple t) {

		// name must be equal
		if (!relation.equals(t.getName())) {
			return false;
		}

		// number of fields must be equal
		if (size() != t.size()) {
			return false;
		}

		// compare field names
		for (int i = 0; i < size(); i++) {
			// next fields
			for (int j = i + 1; j < size(); j++) {
				// atom equality implies value equality
				if (variables[i].equals(variables[j]) && !t.get(i).equals(t.get(j))) {
					return false;
				}

			}
		}
		return true;
	}

	/**
	 * 
	 * @return the number of fields
	 */
	public int size() {
		return variables.length;
	}

	@Override
	public int getRank() {
		return this.rank;
	}

	@Override
	public Set<GFExistentialExpression> getSubExistentialExpression(int k) {
		Set<GFExistentialExpression> set = new HashSet<GFExistentialExpression>();
		return set;
	}

	@Override
	public <R> R accept(GFVisitor<R> v) {
		return v.visit(this);
	}
}
