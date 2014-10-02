/**
 * Created: 25 Sep 2014
 */
package mapreduce.guardedfragment.planner.compiler.mappers;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

/**
 * @author Jonny Daenen
 * 
 */
public class GFMapper1AtomBasedIterator implements Iterable<Pair<String, String>>, Iterator<Pair<String, String>> {

	Iterator<GFAtomicExpression> guards;
	Iterator<GFAtomicExpression> guards2;
	Iterator<GFAtomicExpression> guardeds;
	Iterator<Pair<GFAtomicExpression, GFAtomicExpression>> gAndG;
	Tuple t;

	Pair<String, String> next;

	int phase;

	private static final Log LOG = LogFactory.getLog(GFMapper1AtomBasedIterator.class);

	/**
	 * 
	 */
	public GFMapper1AtomBasedIterator(Set<GFAtomicExpression> guards, Set<GFAtomicExpression> guardeds,
			Set<Pair<GFAtomicExpression, GFAtomicExpression>> gAndG, String value) {
		phase = 1;
		next = null;
		t = new Tuple(value.toString());

		this.guards = guards.iterator();
		this.guards2 = guards.iterator();
		this.guardeds = guardeds.iterator();
		this.gAndG = gAndG.iterator();

	}

	@Override
	public Iterator<Pair<String, String>> iterator() {
		return this;
	}

	/**
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		calculateNext();

		return next != null;
	}

	/**
	 * Adds a new element to the queue if there is none.
	 */
	void calculateNext() {

		// LOG.trace("calculateNext:" + t);

		// check if there is a next element, if so, do nothing
		if (next != null)
			return;

		// check guards
		while (guards.hasNext()) {
			GFAtomicExpression guard = guards.next();
			if (guard.matches(t)) {
				next = new Pair<String, String>(t.toString(), t.toString());
				return;
			}
		}

		// check guards + atom
		while (guards2.hasNext()) {
			GFAtomicExpression guard = guards2.next();
			if (guard.matches(t)) {
				next = new Pair<String, String>(t.toString(), t.toString()+";"+guard.toString());
				return;
			}
		}

		// check guardeds
		while (guardeds.hasNext()) {
			GFAtomicExpression guarded = guardeds.next();
			if (guarded.matches(t)) {
				next = new Pair<String, String>(t.toString(), t.toString());
				return;
			}
		}

		// check pairs
		// OPTIMIZE group by guard
		while (gAndG.hasNext()) {

			Pair<GFAtomicExpression, GFAtomicExpression> gpair = gAndG.next();

			GFAtomicExpression guard = gpair.fst;
			GFAtomicExpression guarded = gpair.snd;

			if (guard.matches(t)) {
				GFAtomProjection gp = new GFAtomProjection(guard, guarded);
				Tuple tprime;
				try {

					tprime = gp.project(t);
					if (guarded.matches(tprime)) {
						next = new Pair<String, String>(tprime.toString(), t.toString() + ";" + guarded);
						return;
					}
				} catch (NonMatchingTupleException e) {
					// should not happen!
					LOG.error(e.getMessage());
					e.printStackTrace();
				}
			}

		}
		return;

	}

	/**
	 * @see java.util.Iterator#next()
	 */
	@Override
	public Pair<String, String> next() {
		calculateNext();
		Pair<String, String> current = next;
		next = null;
		// if(current.fst.contains(",10,") || current.snd.contains(",10,"))
		// LOG.warn(current.fst + " -  " + current.snd );

		return current;
	}

	/**
	 * Does nothing.
	 * 
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {

	}

}