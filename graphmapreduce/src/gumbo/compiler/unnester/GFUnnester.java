/**
 * Created: 28 Apr 2014
 */
package gumbo.compiler.unnester;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.structures.conversion.DNFConverter;
import gumbo.structures.conversion.DNFConverter.DNFConversionException;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;
import gumbo.structures.gfexpressions.GFVisitor;
import gumbo.structures.gfexpressions.GFVisitorException;
import gumbo.structures.gfexpressions.io.Pair;



/**
 * Converts each formula to DNF and breaks up 
 * the conjunctions into separate formulas.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFUnnester implements GFVisitor<Set<GFExpression>> {


	private static final Log LOG = LogFactory.getLog(GFUnnester.class);

	DNFConverter dnfconverter;
	public GFUnnester() {
		dnfconverter = new DNFConverter();
	}

	public Set<GFExpression> unnest(GFExpression e) throws GFDecomposerException {

		// TODO reject non-existentials


		Set<GFExpression> result = null;
		try {

			GFExpression newe = dnfconverter.convert(e);
			result = newe.accept(this);

		} catch (GFDecomposerException e1) {
			throw e1;

		} catch (GFVisitorException | DNFConversionException e1) {
			LOG.error("Unknown Exception");
			e1.printStackTrace();
		} 

		return result;

	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFExpression)
	 */
	@Override
	public Set<GFExpression> visit(GFExpression e) throws GFVisitorException {
		throw new GFDecomposerException("Unknown GFExpression type");
	}

	/**
	 * Should not be called
	 */
	@Override
	public Set<GFExpression> visit(GFAtomicExpression e) throws GFVisitorException {

		throw new GFDecomposerException("Atomic expression unnest request occured.");
	}

	/**
	 * Extracts the positive and negative atoms from a conjunction.
	 */
	@Override
	public Set<GFExpression> visit(GFAndExpression e) throws GFVisitorException {


		Set<GFExpression> result = new HashSet<>();


		GFExpression child1 = e.getChild1();
		GFExpression child2 = e.getChild2();

		if (child1.containsAnd()) {
			result.addAll(child1.accept(this));
		} else {
			result.add(child1);
		}

		if (child2.containsAnd()) {
			result.addAll(child2.accept(this));
		} else {
			result.add(child2);
		}
		
		return result;

	}

	/**
	 * Extracts conjunctions from the DNF query.
	 */
	@Override
	public Set<GFExpression> visit(GFOrExpression e) throws GFVisitorException {


		Set<GFExpression> result = new HashSet<>();


		GFExpression child1 = e.getChild1();
		GFExpression child2 = e.getChild2();

		if (child1.containsOr()) {
			result.addAll(child1.accept(this));
		} else {
			result.add(child1);
		}

		if (child2.containsOr()) {
			result.addAll(child2.accept(this));
		} else {
			result.add(child2);
		}
		
		return result;
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFNotExpression)
	 */
	@Override
	public Set<GFExpression> visit(GFNotExpression e) throws GFVisitorException {

		throw new GFDecomposerException("Negation expression unnest request occured.");
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFExistentialExpression)
	 */
	@Override
	public Set<GFExpression> visit(GFExistentialExpression e) throws GFVisitorException {



		Set<GFExpression> result = new HashSet<>();
		GFAtomicExpression guard = e.getGuard();
		GFExpression newChild = e.getChild();

		// if there is a disjunction, we break it up
		Set<GFExpression> subs;
		if (e.containsOr()) {
			subs = e.getChild().accept(this);
		} else {
			subs = new HashSet<>();
			subs.add(e.getChild());
		}

		GFAtomicExpression prevConjOutput = null;
		GFOrExpression prevDisj = null;
		GFExistentialExpression conjExp = null;

		// for each conjunction
		for (GFExpression gf : subs) {

			// unravel into normal and negated atoms
			Set<GFExpression> atoms = gf.accept(this);

			// make linear AND tree of the atoms
			GFAtomicExpression currentGuard = guard;
			for (GFExpression atom : atoms) {

				// create a new expression with new name
				GFAtomicExpression out = getNewOutputAtom(e.getOutputRelation());
				conjExp = new GFExistentialExpression(currentGuard, atom, out);

				// add to resulting expression set
				result.add(conjExp);

				// shift guard
				currentGuard = out;
			}


			// add to top-level expression for disjunctions
			if (prevDisj != null)
				prevDisj = new GFOrExpression(prevDisj, currentGuard);

			else if (prevConjOutput != null)
				prevDisj = new GFOrExpression(prevConjOutput, currentGuard);

			prevConjOutput = currentGuard;

		}

		if (prevDisj != null) {
			result.add(new GFExistentialExpression(guard, prevDisj, e.getOutputRelation()));
		} else {
			// if there is only one conjunction, we modify the output name
			// as the guard was read in the deepest expression 
			// and should not be re-read after the last expression

			result.remove(conjExp);
			result.add(new GFExistentialExpression(conjExp.getGuard(), conjExp.getChild(), e.getOutputRelation()));

		}

		return result;
	}

	private final String helpPrefix = "gumbohelp";
	private int helpcounter = 0;
	private GFAtomicExpression getNewOutputAtom(GFAtomicExpression original) {
		
		return new GFAtomicExpression(helpPrefix + original.getName()+ "" + helpcounter++, original.getVars());
		
	}

	public Set<GFExistentialExpression> unnest(Collection<? extends GFExpression> expressions) throws GFDecomposerException {
		HashSet<GFExpression> firstresult = new HashSet<GFExpression>();
		HashSet<GFExistentialExpression> result = new HashSet<GFExistentialExpression>();
		for (GFExpression e : expressions) {
			firstresult.addAll(unnest(e));
		}
		
		for (GFExpression e : firstresult) {
			result.add((GFExistentialExpression) e);
		}

		System.out.println(result);
		return result;
	}


}
