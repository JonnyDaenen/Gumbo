package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.Arrays;

import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;


/**
 * Tuple equality representation.
 * 
 * @author Jonny Daenen
 *
 */
public class EqualityType {

	int [] equality;



	public EqualityType(GFAtomicExpression atom) {
		init(atom);
	}

	public EqualityType(QuickWrappedTuple tuple) {
		init(tuple);
	}


	public EqualityType(int[] convert) {
		init(convert);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(equality);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof EqualityType){
			EqualityType et = (EqualityType) obj;
			return Arrays.equals(equality, et.equality);
		}
		return false;
	}


	public boolean matches (EqualityType e) {
		if (e.equality.length != equality.length)
			return false;

		for (int i = 0; i < equality.length; i++) {
			if (equality[i] != e.equality[i])
				return false;
		}
		return true;
	}

	private void init(GFAtomicExpression guard) {
		equality = new int[guard.getNumFields()];

		//init
		for (int i = 0; i < equality.length; i++) {
			equality[i] = i;
		}

		String[] vars = guard.getVars();

		// for each var
		for (int i = 0; i < vars.length; i++) {
			String first = vars[i];

			// find first match, next matches will be solved by next i
			for (int j = i+1; j < vars.length; j++) {
				String second = vars[j];
				if (first.equals(second)) {
					equality[j] = equality[i];
					break;
				}
			}

		}

		// FUTURE support for constants!


	}
	
	private void init(int [] values) {
		equality = new int[values.length];

		//init
		for (int i = 0; i < equality.length; i++) {
			equality[i] = i;
		}

		// for each var
		for (int i = 0; i < equality.length; i++) {

			// find first match, next matches will be solved by next i
			for (int j = i+1; j < equality.length; j++) {
				if (values[i] == values[j]) {
					equality[j] = equality[i];
					break;
				}
			}

		}

		// FUTURE support for constants!


	}

	private void init(QuickWrappedTuple qt) {
		equality = new int[qt.size()];

		//init
		for (int i = 0; i < equality.length; i++) {
			equality[i] = i;
		}

		// for each var
		for (int i = 0; i < equality.length; i++) {

			// find first match, next matches will be solved by next i
			for (int j = i+1; j < equality.length; j++) {
				if (qt.equals(i, j)) {
					equality[j] = equality[i];
					break;
				}
			}

		}

		// FUTURE support for constants!


	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(equality.length*2);
		int i;
		for (i = 0; i < equality.length; i++)
			sb.append(equality[i] + ",");
		if (i > 0)
			sb.deleteCharAt(sb.length()-1);

		return sb.toString();
	}

}
