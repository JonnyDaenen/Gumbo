package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;


/**
 * Tuple equality representation.
 * 
 * @author Jonny Daenen
 *
 */
public class EqualityType {

	int size;
	int [] equality;



	public EqualityType(GFAtomicExpression guard) {
		init(guard);
	}

	public EqualityType(QuickWrappedTuple guard) {
		init(guard);
	}


	public boolean matches (EqualityType e) {
		if (e.size != size)
			return false;

		for (int i = 0; i < size; i++) {
			if (equality[i] != e.equality[i])
				return false;
		}
		return true;
	}

	private void init(GFAtomicExpression guard) {
		size = guard.getNumFields();
		equality = new int[size];

		//init
		for (int i = 0; i < size; i++) {
			equality[i] = i;
		}

		String[] vars = guard.getVars();

		// for each var
		for (int i = 0; i < vars.length; i++) {
			String first = vars[i];

			// find first match, next matches will be solved by next i
			for (int j = i; j < vars.length; j++) {
				String second = vars[i];
				if (first.equals(second)) {
					equality[j] = equality[i];
					break;
				}
			}

		}

		// FUTURE support for constants!


	}

	private void init(QuickWrappedTuple qt) {
		size = qt.size();
		equality = new int[size];

		//init
		for (int i = 0; i < size; i++) {
			equality[i] = i;
		}

		// for each var
		for (int i = 0; i < size; i++) {

			// find first match, next matches will be solved by next i
			for (int j = i; j < size; j++) {
				if (qt.equals(i, j)) {
					equality[j] = equality[i];
					break;
				}
			}

		}

		// FUTURE support for constants!


	}

}
