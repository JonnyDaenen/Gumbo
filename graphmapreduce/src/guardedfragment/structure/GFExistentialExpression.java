package guardedfragment.structure;

import guardedfragment.booleanstructure.BExpression;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GFExistentialExpression extends GFExpression {
	
	String [] variables;
	GFAtomicExpression guard;
	GFExpression child;
	
	char quantifierSymbol = 'E';
	
	
	/**
	 * Creates an existential expression, consisting of a guard (atomic expression) and a child expression.
	 * @param guard an atomic expression
	 * @param child a child expression
	 * @param variables the PROJECTED variables
	 */
	public GFExistentialExpression(GFAtomicExpression guard, GFExpression child, String ... variables) {
		super();
		this.guard = guard;
		this.child = child;
		this.variables = variables;
	}


	public Set<GFAtomicExpression> getAtomic() {
		Set<GFAtomicExpression> allAtoms = child.getAtomic();
		allAtoms.add(guard);
		return allAtoms;
	}

	@Override
	/**
	 * Checks if there exist values for the supplied variables for which the formula evaluates to true.
	 * 
	 * @return true iff there exist values for the variables that make the formula true
	 */
	public boolean evaluate(GFEvaluationContext c) {
		
		// TODO assign values to variables? (add to context)
		
		return guard.evaluate(c) && child.evaluate(c);
	}

	@Override
	public Set<String> getFreeVariables() {
		Set<String> freeVars = guard.getFreeVariables();
		freeVars.addAll(child.getFreeVariables());
		
		List<String> varlist = Arrays.asList(variables);
		freeVars.removeAll(varlist);
		
		return freeVars;
	}
	
	@Override
	public String generateString() {
		return "(" + quantifierSymbol + " " + generateVarString()+ " " + guard.generateString() + " & " + child.generateString() + ")";
	}



	private String generateVarString() {
		String list = "";
		for(String v : variables)
			list += "," + v;
		
		return list.substring(1);
	}
	
	
	@Override
	public boolean isGuarded() {
		Set<String> guardvars = guard.getFreeVariables();
		Set<String> phivars = child.getFreeVariables();
		
		Set<String> varlist = new HashSet<String>(Arrays.asList(variables));
		
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
	public BExpression convertToBExpression(GFBMapping m) throws GFConversionException {
		throw new GFConversionException("It's not possible to convert formulas that are not a boolean combination of atomic formula's.");
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
	public Set<GFAtomicExpression> getGuardedRelations() {
		return child.getAtomic();
	}
	
}
