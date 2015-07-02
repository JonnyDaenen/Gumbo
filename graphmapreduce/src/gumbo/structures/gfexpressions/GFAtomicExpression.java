package gumbo.structures.gfexpressions;

import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.io.Pair;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class GFAtomicExpression extends GFExpression implements Comparable<Object>{

	private static final long serialVersionUID = 1L;

	String relation;
	String[] variables;
	String[] constants;

	RelationSchema schemaCache = null;

	boolean cached;
	List<Pair<Integer, Integer>> checks;

	public GFAtomicExpression(String relationName, String... variables) {
		this.relation = relationName;
		this.variables = variables;
		this.constants = new String[variables.length];
		
		// CONSTANTCODE begin
		for (int i = 0; i < variables.length; i++) {
			String var = variables[i];
			if (var.contains("=")) {
				this.variables[i] = var.split("=")[0];
				this.constants[i] = var.split("=")[1];
			}
		}
		// CONSTANTCODE end

		cached = false;
		checks = new LinkedList<Pair<Integer, Integer>>();
	}

	/**
	 * Makes a copy of the object.
	 * 
	 * @param aexp
	 */
	public GFAtomicExpression(GFAtomicExpression aexp) {
		this.relation = aexp.relation;
		this.variables = aexp.variables.clone();
		this.constants = aexp.constants.clone();
		cached = false;
		checks = new LinkedList<Pair<Integer, Integer>>();
	}

	public String getName() {
		return relation;
	}

	public String[] getVars() {
		return variables;
	}
	
	public String[] getConstants() {
		return constants;
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

	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#addAtomic(java.util.Collection)
	 */
	@Override
	public void addAtomic(Collection<GFAtomicExpression> current) {
		current.add(this);
	}

	@Override
	public String generateString() {
		// OPTIMIZE cache?
		return relation + "(" + generateVarString() + ")";
	}

	public int getNumVariables() {
		return variables.length;
	}

	private String generateVarString() {
		String list = "";
		int i = 0;
		for (String v : variables) {
			list += "," + v;
			if (constants[i] != null)
				list += "=" + constants[i];
			i++;
		}

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
	public int hashCode() {
		return relation.hashCode() ^ variables.length;
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

		// number of fields must be equal
		if (size() != t.size()) {
			//			System.out.println("size: " + toString() + " " + t.toString());
			return false;
		}

		// name must be equal
		if (!relation.equals(t.getName())) {

			//			System.out.println("name: " + toString() + " " + t.toString());
			return false;
		}
		
		// CONSTANTCODE begin
		// check constants
		for (int i = 0; i < size(); i++) {
			if (constants[i] != null && !t.get(i).equals(constants[i])) {
				return false;
			}
		}
		// CONSTANTCODE end

		if (!cached) {

			boolean success = true;

			// compare field names
			for (int i = 0; i < size(); i++) {
				// next fields
				for (int j = i + 1; j < size(); j++) {
					// atom equality implies value equality

					boolean performCheck = false;
					if (variables[i].equals(variables[j])) {
						performCheck = true;
						checks.add(new Pair<Integer, Integer>(i, j));
					}

					if (performCheck && !t.get(i).equals(t.get(j))) {
						success = false;
					}

				}
			}
			
			cached = true;
			return success;
		} else {

			for (Pair<Integer, Integer> p : checks) {
				if (!t.get(p.fst).equals(t.get(p.snd))) {
					return false;
				}
			}
			
			return true;
		}
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
		return 0;
	}

	@Override
	public Set<GFExistentialExpression> getSubExistentialExpression(int k) {
		Set<GFExistentialExpression> set = new HashSet<GFExistentialExpression>();
		return set;
	}

	@Override
	public <R> R accept(GFVisitor<R> v) throws GFVisitorException {
		return v.visit(this);
	}

	/**
	 * @return the relation schema of this atom (column names are arbitrary).
	 */
	public RelationSchema getRelationSchema() {
		if (schemaCache == null)
			schemaCache = new RelationSchema(this.relation, this.variables.length);
		return schemaCache;

	}

	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#containsAnd()
	 */
	@Override
	public boolean containsAnd() {
		return false;
	}

	/**
	 * Returns true, as an atom is trivially in DNF.
	 * 
	 * @return true
	 * 
	 * @see gumbo.structures.gfexpressions.GFExpression#isInDNF()
	 */
	@Override
	public boolean isInDNF() {
		return true;
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#containsOr()
	 */
	@Override
	public boolean containsOr() {
		return false;
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#countOccurences(gumbo.structures.gfexpressions.GFExpression)
	 */
	@Override
	public int countOccurences(GFExpression ge) {
		if (this == ge)
			return 1;
		return 0;
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#getParent(gumbo.structures.gfexpressions.GFExpression)
	 */
	@Override
	public GFExpression getParent(GFExpression e) {

		return null;
	}

	/**
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Object o) {

		if (o == null) 
			throw new NullPointerException();

		if (! (o instanceof GFAtomicExpression))
			throw new ClassCastException();

		GFAtomicExpression other = (GFAtomicExpression) o;

		String s1 = this.generateString();
		String s2 = other.generateString();


		return s1.compareTo(s2);
	}

}
