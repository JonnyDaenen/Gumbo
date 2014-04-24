package guardedfragment.structure.gfexpressions;

import guardedfragment.structure.booleanexpressions.BExpression;
import guardedfragment.structure.conversion.GFBooleanMapping;
import guardedfragment.structure.conversion.GFtoBooleanConversionException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class GFExistentialExpression extends GFExpression {
	
	GFAtomicExpression guard;
	GFExpression child;
	int rank;
	GFAtomicExpression output;
	
	char quantifierSymbol = 'E';
	
	
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
	
	public GFAtomicExpression getOutputSchema() {
		return output;
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
		return "(" + output.generateString() + " = " + guard.generateString() + " ^ " + child.generateString() + ")";
	}
	

	public String prefixString() {
		return "=" + output.prefixString() + "^" + guard.prefixString() +  child.prefixString();
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
		
//		System.out.println(phivars);
//		System.out.println(varlist);
//		System.out.println(guardvars);
		
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
	
}