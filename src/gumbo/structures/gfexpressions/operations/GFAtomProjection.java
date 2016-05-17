package gumbo.structures.gfexpressions.operations;

import org.apache.directory.api.util.ByteBuffer;

import gumbo.structures.data.QuickTuple;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;

/**
 * Represents a projection from a atom to another.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFAtomProjection {

	GFAtomicExpression source;
	GFAtomicExpression target;


	int [] arrayMapping;
	private ByteBuffer bb;
	byte [] nameBytes;

	/**
	 * Creates a projection to transform a guard tuple to a guarded tuple.
	 * The guard tuple must *match* the guard expression (this has to be checked by the user).
	 * 
	 * @param source the guard expression
	 * @param target the guarded expression on which to project
	 */
	public GFAtomProjection(GFAtomicExpression source,
			GFAtomicExpression target) {

		this.source = source;
		this.target = target;
		bb = new ByteBuffer(100);

		// initialize mapping
		arrayMapping = new int[target.getNumVariables()];
		initialize();
	}


	/**
	 * Constructs a mapping between the atoms.
	 * For each position in the target atom, an index in the source tuple is kept.
	 * This index indicates where to get the value to fill in on this position.
	 * Example:
	 * source: T(x,y,x,z)
	 * target: S(x,z)
	 * mapping:
	 * 	- 0 -> 0
	 * 	- 1 -> 3
	 * 
	 * There is no guarantee on which position is chosen when there are several options
	 * 
	 */
	private void initialize() {


		String[] sourceVars = source.getVars();
		String[] targetVars = target.getVars();

		// TODO what if mapping is not possible? -> test using projection of the variable tuple??



		// check for equal variable identifiers
		// for each position of the target
		for (int i = 0; i < targetVars.length; i++) {
			// check where it occurs in the source
			for (int j = 0; j < sourceVars.length; j++) {

				// if we find a match
				if (targetVars[i].equals(sourceVars[j])) {

					// add it
					arrayMapping[i] = j;

					// other matches will have the same value
					// when they belong to the source GF-expression
					break;
				}
			}

		}
		nameBytes = target.getName().getBytes();
	}


	/**
	 * Converts a tuple from the source relation to a tuple in the target
	 * relation, according to the mapping.
	 * It is not checked if the tuple belongs to the guard expression.
	 * This is because of performance reasons.
	 * 
	 * @return a projection of a given source-tuple to the target relation
	 * @throws NonMatchingTupleException not thrown for now
	 */
	public Tuple project(Tuple t) throws NonMatchingTupleException {

		// TODO throw exception if non-matching

		int fields = target.getNumFields();

		String[] s = new String[fields];

		// copy fields one by one
		for (int i = 0; i < fields; i++)
			s[i] = t.get(arrayMapping[i]);

		// create a new tuple from the generated String
		return new Tuple(target.getName(), s);

	}

	/**
	 * Projects a tuple onto another according to this transformation.
	 * When CSV mode is turned on, no relation name nor surrounding brackets will be present
	 * in the output.
	 * 
	 * @param t a tuple
	 * @param csv when turned on, csv format will be returned
	 * 
	 * @return the projected tuple
	 * 
	 * @throws NonMatchingTupleException
	 */
	public String projectString(Tuple t, boolean csv) throws NonMatchingTupleException {

		bb.clear();

		// head
		if (!csv) {
			bb.append(nameBytes);
			bb.append('(');
		}

		// middle
		int i;
		int fields = target.getNumFields()-1;
		for (i = 0; i < fields; i++) {
			bb.append(t.get(arrayMapping[i]).getBytes());
			bb.append(',');
		}
		bb.append(t.get(arrayMapping[i]).getBytes());

		// tail
		if (!csv) {
			bb.append(')');
		}
		String s = new String(bb.copyOfUsedBytes());
//		System.out.println(s);
		return s;

	}
	
	public String projectString(QuickTuple t, boolean csv) throws NonMatchingTupleException {

		
		

		int i;
		int fields = target.getNumFields()-1;
		int totalBytes = 0;
		for (i = 0; i < fields+1; i++) {
			int index = arrayMapping[i];
			totalBytes += t.getLength(index) + 1;
		}
		totalBytes -= 1;
		
		byte [] result = new byte[totalBytes];

		// middle
		int pos = 0;
		for (i = 0; i < fields; i++) {
			int index = arrayMapping[i];
			int length = t.getLength(index);
			int start = t.getStart(index);
			System.arraycopy(t.getData(), start, result, pos, length);
			result[pos + length] = ',';
			pos += length + 1;
		}
		int index = arrayMapping[i];
		int length = t.getLength(index);
		int start = t.getStart(index);
		System.arraycopy(t.getData(), start, result, pos, length);
		pos += length;

		// csv wrapping
		if (!csv) {
			bb.clear();
			bb.append(nameBytes);
			bb.append('(');
			bb.append(result);
			bb.append(')');
			result = bb.copyOfUsedBytes();
		}
		String s = new String(result);
//		System.out.println(s);
		return s;

	}


	/**
	 * @return relation schema of the output relation
	 */
	public RelationSchema getOutputSchema() {
		return target.getRelationSchema();
	}

}
