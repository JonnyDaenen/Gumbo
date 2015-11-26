/**
 * Created: 25 Sep 2014
 */
package gumbo.compiler.resolver.mappers;

import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;
import gumbo.structures.gfexpressions.operations.NonMatchingTupleException;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * @author Jonny Daenen
 * 
 */
public class GFMapper1AtomBasedIterator implements Iterable<Pair<Text, Text>>, Iterator<Pair<Text, Text>> {

	Iterator<GFAtomicExpression> guards;
	Iterator<GFAtomicExpression> guardeds;
	Iterator<Pair<GFAtomicExpression, GFAtomicExpression>> gAndG;
	Tuple t;
	Text txt;

	Pair<Text, Text> next;

	private boolean outputGuard;
	Set<GFAtomicExpression> actualGuards;

	private static final Log LOG = LogFactory.getLog(GFMapper1AtomBasedIterator.class);

	/**
	 * 
	 */
	public GFMapper1AtomBasedIterator(Set<GFAtomicExpression> guards, Set<GFAtomicExpression> guardeds,
			Set<Pair<GFAtomicExpression, GFAtomicExpression>> gAndG, Text value) {
		next = null;
		t = new Tuple(value.toString());
		txt = value;

		actualGuards = new HashSet<>();

		for (GFAtomicExpression guard : guards) {
			if (guard.matches(t))
				actualGuards.add(guard);
		}

		if (actualGuards.size() != 0)
			outputGuard = true;

		this.guards = actualGuards.iterator();
		this.guardeds = guardeds.iterator();
		this.gAndG = gAndG.iterator();

	}

	@Override
	public Iterator<Pair<Text, Text>> iterator() {
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

		// guard output
		if (outputGuard) {
			next = new Pair<Text, Text>(txt, txt);
			outputGuard = false;
			return;
		}

		// check guards + atom (keep-alive)
		while (guards.hasNext()) {
			GFAtomicExpression guard = guards.next();
			// guard match check has been done already
				next = new Pair<Text, Text>(txt, new Text(t.toString() + ";" + guard.toString()));
				return;
		}

		// check guardeds
		while (guardeds.hasNext()) {
			GFAtomicExpression guarded = guardeds.next();
			if (guarded.matches(t)) {
				next = new Pair<Text, Text>(txt, txt);
				return;
			}
		}

		// check pairs
		// OPTIMIZE group by guard
		while (gAndG.hasNext()) {

			Pair<GFAtomicExpression, GFAtomicExpression> gpair = gAndG.next();

			GFAtomicExpression guard = gpair.fst;
			GFAtomicExpression guarded = gpair.snd;

			// precalculated
			if (actualGuards.contains(guard)) {
				GFAtomProjection gp = new GFAtomProjection(guard, guarded);
				Tuple tprime;
				try {

					tprime = gp.project(t);
					if (guarded.matches(tprime)) {
						next = new Pair<Text, Text>(new Text(tprime.toString()), new Text(t.toString() + ";" + guarded));
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
	public Pair<Text, Text> next() {
		calculateNext();
		Pair<Text, Text> current = next;
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