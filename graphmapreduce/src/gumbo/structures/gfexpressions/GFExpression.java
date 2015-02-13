package gumbo.structures.gfexpressions;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Warning: no duplicate objects should be used in GFExpressions.
 * @author Jonny Daenen
 *
 */
public abstract class GFExpression implements Serializable {

	private static final long serialVersionUID = 1L;

	public abstract Set<String> getFreeVariables();

	public abstract int getRank();

	public abstract String generateString();
	
	

	public abstract Set<GFExistentialExpression> getSubExistentialExpression(int k);


	/**
	 * 
	 * @return a set of all atoms
	 */
	public Collection<GFAtomicExpression> getAtomic() {
		Set<GFAtomicExpression> atoms = new HashSet<GFAtomicExpression>();
		addAtomic(atoms);
		return atoms;
	}
	
	/**
	 * Calculates all atomic descendants and adds them to an existing collection.
	 * The guards are also included as atoms.
	 * @param current an existing collection to expand into
	 */
	public abstract void addAtomic(Collection<GFAtomicExpression> current);

	/**
	 * Checks whether the formula is guarded. A Formula is guarded when it is in
	 * the following inductively defined set: 1. an atomic relation expression
	 * is a GF-expression 2. any boolean combination of GF-expressions is a
	 * GF-expression 3a. Ex R(y) & phi(y') is a GF-expression when: - x is a
	 * subset of y - y' is a subset of y 3b. Vx R(y) -> phi(y') is a
	 * GF-expression when: - x is a subset of y - y' is a subset of y
	 * 
	 * 
	 * @return true if the formula is a GF-expression, false otherwise
	 */
	public abstract boolean isGuarded();

	/**
	 * Checks whether the subformula is a boolean combination of atomic
	 * expressions.
	 * 
	 * @return true if the formula is a boolean combination of atomic
	 *         expressions, false otherwise
	 */
	public abstract boolean isAtomicBooleanCombination();
	
	/**
	 * Checks whether the GFExpression is *basic*. A Basic expression has the following form:
	 * exists x: guard and boolean combination of atomics.
	 * 
	 * @return true when the expression is a *basic* GF expression
	 */
	public boolean isBasicGF() {
		return false;
	}
	
	/**
	 * Checks whether the GFExpression is *non-conjunctive* and *basic*.
	 * This kind of expression in a basic existential expression where the guarded part
	 * solely consists of non-conjunctions. Hence, it has the following form:
	 * exists x: guard and (or/not combination of atomics).
	 * 
	 * @return true when the expression is a *non-conjunctive* and *basic* GF expression
	 */
	public boolean isNonConjunctiveBasicGF() {
		return false;
	}

	@Override
	public String toString() {
		return generateString();
	}

	public <R> R accept(GFVisitor<R> v) throws GFVisitorException {
		return v.visit(this);
	}

	/**
	 * Checks whether the expression contains a conjunction operation (including the guards).
	 * @return true when the expression contains a conjunction (AND)
	 */
	abstract public boolean containsAnd();
	
	/**
	 * Checks whether the expression contains a disjunction operation.
	 * @return true when the expression contains a disjunction (OR)
	 */
	abstract public boolean containsOr();

	/**
	 * Checks if the formula is in DNF, see concrete implementations for more detail.
	 * @return true if the formula is in DNF, false otherwise.
	 */
	public abstract boolean isInDNF();

	/**
	 * Counts the number of times the <b>object</b> is used in the expression. Should be 1.
	 * Output atomics of existential expressions also counted.
	 * @param ge the object to look for
	 * @return the number of times the object appears in the expression.
	 */
	public abstract int countOccurences(GFExpression ge);
	
	/**
	 * Locates the parent of a given expression (object, equals is not used).
	 * @param e the expression to look for
	 * @return the parent of the expression, null if not found
	 */
	public abstract GFExpression getParent(GFExpression e);

}
