package gumbo.structures.gfexpressions;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import gumbo.structures.data.RelationSchema;

public class GFExistentialExpression extends GFExpression {
	
	private static final long serialVersionUID = 1L;
	GFAtomicExpression guard;
	GFExpression child;
	int rank;
	GFAtomicExpression output;
	
	
	
	/**
	 * Creates an existential expression, consisting of a guard (atomic expression) and a child expression.
	 * @param guard an atomic expression
	 * @param child a child expression
	 * @param name the name of the output
	 * @param variables the PROJECTED variables
	 */
	public GFExistentialExpression(GFAtomicExpression guard, GFExpression child, String name, String ... variables) {
		super();
		this.guard = guard;
		this.child = child;
		this.output = new GFAtomicExpression(name,variables);
		this.rank = child.getRank()+1;
	}

	public GFExistentialExpression(GFAtomicExpression guard, GFExpression child, GFAtomicExpression out) {
		super();
		this.guard = guard;
		this.child = child;
		this.output = out;
		this.rank = child.getRank()+1;
	}
		

	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#addAtomic(java.util.Collection)
	 */
	@Override
	public void addAtomic(Collection<GFAtomicExpression> current) {
		child.addAtomic(current);
		current.add(guard);		
	}

	
	/**
	 * Constructs a set containing the relations schemas this expression depends on.
	 * @return the set of relation schemas this expressions depends on.
	 */
	public Set<RelationSchema> getRelationDependencies() {
		Collection<GFAtomicExpression> allAtoms = getAtomic();
		Set<RelationSchema> schemas = new HashSet<RelationSchema>();
		
		for (GFAtomicExpression atom : allAtoms) {
			RelationSchema schema = atom.getRelationSchema();
			schemas.add(schema);
		}
		
		return schemas;
	}
	
	public GFAtomicExpression getOutputRelation() {
		return output;
	}
	
	public RelationSchema getOutputSchema() {
		return output.getRelationSchema();
	}


	@Override
	public Set<String> getFreeVariables() {
		//Set<String> freeVars = guard.getFreeVariables();
		//freeVars.addAll(child.getFreeVariables());	
		//List<String> varlist = Arrays.asList(variables);
		//freeVars.removeAll(varlist);
		
		return output.getFreeVariables();
	}

/*	private Set<String> getQuantifiedVariables() {
		Set<String> guardVars = guard.getFreeVariables();
		Set<String> freevars = output.getFreeVariables();
		
		guardVars.removeAll(freevars);
		return guardVars;
	}
*/
	
	@Override
	public String generateString() {
		return "(" + output.generateString() + " : " + guard.generateString() + " & " + child.generateString() + ")";
	}
	

	
	
/*
	private String generateQuantifiedVarString() {
		Set<String> set = getQuantifiedVariables();
		String[] array = set.toArray(new String[0]);
		
		String list = "";
		for(int i=0;i< array.length;i++) {
			list += "," +array[i];
		}
		
		return list.substring(1);
	}
*/	
	
    @Override
	public boolean isGuarded() {
		Set<String> guardvars = guard.getFreeVariables();
		Set<String> phivars = child.getFreeVariables();
		
		Set<String> varlist = new HashSet<String>(Arrays.asList(output.getVars()));
		
		phivars.removeAll(guardvars);
		varlist.removeAll(guardvars);
		
		return phivars.isEmpty() && varlist.isEmpty(); // OPTIMIZE this can be optimized
	}


	@Override
	public boolean isAtomicBooleanCombination() {
		return false;
	}
	
	@Override
	public boolean isBasicGF() {
		return child.isAtomicBooleanCombination();
	}
	
	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#isNonConjunctiveBasicGF()
	 */
	@Override
	public boolean isNonConjunctiveBasicGF() {
		return child.isAtomicBooleanCombination() && child.containsAnd();
	}

	
	public GFExpression getChild() {
		return child;
	}
	
	public GFAtomicExpression getGuard() {
		return	guard;
	}
	


	/**
	 * Creates a set of atomics on the "right" side.
	 * @return the set of guarded relations
	 */
	public Collection<GFAtomicExpression> getGuardedAtoms() {
		return child.getAtomic();
	}


	@Override
	public int getRank() {
		return this.rank;
	}
	
	
	// return all sub ExistentialExpression of rank k
	public Set<GFExistentialExpression> getSubExistentialExpression(int k) {
		Set<GFExistentialExpression> set = new HashSet<GFExistentialExpression>();
		
		if (k > this.rank) {
			return set;
		}
		if (k == this.rank) {
			set.add(this);
			return set;
		}
		
		return child.getSubExistentialExpression(k);	
	}
	
	@Override
	public <R> R accept(GFVisitor<R> v) throws GFVisitorException {
		return v.visit(this);
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#containsAnd()
	 */
	@Override
	public boolean containsAnd() {
		return true;
	}
	
	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#containsOr()
	 */
	@Override
	public boolean containsOr() {
		return child.containsOr();
	}


	/**
	 * Existential is not in DNF
	 * @see gumbo.structures.gfexpressions.GFExpression#isInDNF()
	 */
	@Override
	public boolean isInDNF() {
		return false;
	}
	
	/**
	 * Checks whether the guarded relations are in DNF.
	 * @return true if the guarded relations are in DNF	
	 */
	public boolean isGuardedDNF(){
		return child.isInDNF();
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#countOccurences(gumbo.structures.gfexpressions.GFExpression)
	 */
	@Override
	public int countOccurences(GFExpression ge) {
		int thisok = 0;
		if(this == ge) 
			thisok = 1;
		
		return thisok + guard.countOccurences(ge) + child.countOccurences(ge) + output.countOccurences(ge);
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#getParent(gumbo.structures.gfexpressions.GFExpression)
	 */
	@Override
	public GFExpression getParent(GFExpression e) {
		if(guard == e || child == e || output == e)
			return this;
		
		GFExpression child1result = child.getParent(e);
		if(child1result != null)
			return child1result;
		
		
		return null;
	}


	
	
	
}
