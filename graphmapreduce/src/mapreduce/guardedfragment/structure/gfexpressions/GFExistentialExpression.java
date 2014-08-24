package mapreduce.guardedfragment.structure.gfexpressions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.data.RelationSchema;

public class GFExistentialExpression extends GFExpression {
	
	GFAtomicExpression guard;
	GFExpression child;
	int rank;
	GFAtomicExpression output;
	
	char quantifierSymbol = '#';
	
	
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
		

	public Set<GFAtomicExpression> getAtomic() {
		Set<GFAtomicExpression> allAtoms = child.getAtomic();
		allAtoms.add(guard);
		return allAtoms;
	}
	
	/**
	 * 
	 * @return the set of relation schemas this expressions depends on.
	 */
	public Set<RelationSchema> getRelationDependencies() {
		Set<GFAtomicExpression> allAtoms = getAtomic();
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
		return "(" + output.generateString() + " # " + guard.generateString() + " & " + child.generateString() + ")";
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
	 * @see mapreduce.guardedfragment.structure.gfexpressions.GFExpression#isNonConjunctiveBasicGF()
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
	
	public GFAtomicExpression getOutput() {
		return output;
	}


	/**
	 * Creates a set of atomics on the "right" side.
	 * @return the set of guarded relations
	 */
	public Set<GFAtomicExpression> getGuardedRelations() {
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
	 * @see mapreduce.guardedfragment.structure.gfexpressions.GFExpression#containsAnd()
	 */
	@Override
	public boolean containsAnd() {
		return true;
	}

	
	
}
