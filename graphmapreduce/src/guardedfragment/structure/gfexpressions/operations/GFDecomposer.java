/**
 * Created: 28 Apr 2014
 */
package guardedfragment.structure.gfexpressions.operations;

import guardedfragment.structure.gfexpressions.GFAndExpression;
import guardedfragment.structure.gfexpressions.GFAtomicExpression;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.GFExpression;
import guardedfragment.structure.gfexpressions.GFNotExpression;
import guardedfragment.structure.gfexpressions.GFOrExpression;
import guardedfragment.structure.gfexpressions.GFUniversalExpression;
import guardedfragment.structure.gfexpressions.GFVisitor;
import guardedfragment.structure.gfexpressions.GFVisitorException;
import guardedfragment.structure.gfexpressions.io.Pair;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



/**
 * Decomposes GFExpressions into sets of basicexpression.
 * A visitor pattern is used to let each expression resutn the set of basic subexpressions and their clean version.
 * Also, a GFexpression is used fot non-existential expressions to pass intermediate structures; existential expressions
 * just return their output relation.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFDecomposer implements GFVisitor<Pair<GFExpression,Set<GFExistentialExpression>>> {
	

	private static final Log LOG = LogFactory.getLog(GFDecomposer.class);

	public Set<GFExistentialExpression> decompose(GFExpression e) throws GFDecomposerException {
		
		// TODO reject non-existentials
		
		Pair<GFExpression, Set<GFExistentialExpression>> result = null;
		try {
			result = e.accept(this);
			
		} catch (GFDecomposerException e1) {
			throw e1;
			
		} catch (GFVisitorException e1) {
			LOG.error("Unknown Exception");
			e1.printStackTrace();
		}
		
		return result.snd;
		
	}

	/**
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFExpression)
	 */
	@Override
	public Pair<GFExpression,Set<GFExistentialExpression>> visit(GFExpression e) throws GFVisitorException {
		throw new GFDecomposerException("Unknown GFExpression type");
	}

	/**
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFAtomicExpression)
	 */
	@Override
	public Pair<GFExpression,Set<GFExistentialExpression>> visit(GFAtomicExpression e) throws GFVisitorException {
		
		Set<GFExistentialExpression> subs = new HashSet<GFExistentialExpression>();
		GFAtomicExpression cleanedExp = new GFAtomicExpression(e);
		
		return new Pair<GFExpression, Set<GFExistentialExpression>>(cleanedExp, subs);
	}

	/**
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFAndExpression)
	 */
	@Override
	public Pair<GFExpression,Set<GFExistentialExpression>> visit(GFAndExpression e) throws GFVisitorException {
		
		Pair<GFExpression, Set<GFExistentialExpression>> first = e.getChild1().accept(this);
		Pair<GFExpression, Set<GFExistentialExpression>> second = e.getChild2().accept(this);
		
		Set<GFExistentialExpression> subs = new HashSet<GFExistentialExpression>();
		subs.addAll(first.snd);
		subs.addAll(second.snd);
		
		GFAndExpression cleanedExp = new GFAndExpression(first.fst, second.fst);
		
		return new Pair<GFExpression, Set<GFExistentialExpression>>(cleanedExp, subs);
		
	}

	/**
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFOrExpression)
	 */
	@Override
	public Pair<GFExpression,Set<GFExistentialExpression>> visit(GFOrExpression e) throws GFVisitorException {
		Pair<GFExpression, Set<GFExistentialExpression>> first = e.getChild1().accept(this);
		Pair<GFExpression, Set<GFExistentialExpression>> second = e.getChild2().accept(this);
		
		Set<GFExistentialExpression> subs = new HashSet<GFExistentialExpression>();
		subs.addAll(first.snd);
		subs.addAll(second.snd);
		
		GFOrExpression cleanedExp = new GFOrExpression(first.fst, second.fst);
		
		return new Pair<GFExpression, Set<GFExistentialExpression>>(cleanedExp, subs);
	}

	/**
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFNotExpression)
	 */
	@Override
	public Pair<GFExpression,Set<GFExistentialExpression>> visit(GFNotExpression e) throws GFVisitorException {
		Pair<GFExpression, Set<GFExistentialExpression>> first = e.getChild().accept(this);
		
		Set<GFExistentialExpression> subs = new HashSet<GFExistentialExpression>();
		subs.addAll(first.snd);
		
		GFNotExpression cleanedExp = new GFNotExpression(first.fst);
		
		return new Pair<GFExpression, Set<GFExistentialExpression>>(cleanedExp, subs);
	}

	/**
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFExistentialExpression)
	 */
	@Override
	public Pair<GFExpression,Set<GFExistentialExpression>> visit(GFExistentialExpression e) throws GFVisitorException {


		
		Set<GFExistentialExpression> subexps;
		GFExpression newChild;
		
		// on atomic level, we can stop
		if(e.isAtomicBooleanCombination()) {
			newChild = e.getChild(); // FIXME clone?
			subexps = new HashSet<GFExistentialExpression>();
			
		} else {
			Pair<GFExpression, Set<GFExistentialExpression>> subs = e.getChild().accept(this);
			newChild = subs.fst;
			subexps = subs.snd;
		}
		
		
		// create new expression and add it to this
		GFAtomicExpression newGuard = new GFAtomicExpression(e.getGuard());
		GFAtomicExpression newOutput = new GFAtomicExpression(e.getOutput());
		
		GFExistentialExpression ee = new GFExistentialExpression(newGuard,newChild,newOutput);
		subexps.add(ee);
	
		
		return new Pair<GFExpression, Set<GFExistentialExpression>>(newOutput, subexps);
	}

	/**
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFUniversalExpression)
	 */
	@Override
	public Pair<GFExpression,Set<GFExistentialExpression>> visit(GFUniversalExpression e) throws GFVisitorException {
		throw new GFDecomposerException("GFExistentialExpression is not supported");
	}

}
