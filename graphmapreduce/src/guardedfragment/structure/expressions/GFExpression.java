package guardedfragment.structure.expressions;

import guardedfragment.booleanstructure.BExpression;
import guardedfragment.structure.GFBMapping;
import guardedfragment.structure.GFConversionException;
import guardedfragment.structure.GFEvaluationContext;

import java.util.Set;

public abstract class GFExpression {

	public abstract boolean evaluate(GFEvaluationContext c);

	public abstract Set<String> getFreeVariables();

	public abstract int getRank();

	public abstract String generateString();

	public abstract Set<GFExistentialExpression> getSubExistentialExpression(int k);

	@Deprecated
	public abstract String prefixString();

	/**
	 * @return the set of all atomic formulae
	 */
	public abstract Set<GFAtomicExpression> getAtomic();

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
	 * Convert the GFExpression to a boolean expression. A mapping from atomic
	 * values to boolean variables is created if necessary. Note that identical
	 * relations are mapped to the same variable. E.g., B(x) & B(x) is mapped
	 * onto v0 & v0. This is the case even when the GFAtomicExpressions are
	 * different.
	 * 
	 * @param m
	 *            a mapping from atomic values to variables; missing values are
	 *            added
	 * @return a boolean expression
	 * @throws GFConversionException
	 *             when it's not a boolean combination of atomic expressions
	 */
	public abstract BExpression convertToBExpression(GFBMapping m) throws GFConversionException;

	@Override
	public String toString() {
		return generateString();
	}

	public <R> R accept(GFVisitor<R> v) {
		return v.visit(this);
	}

}
