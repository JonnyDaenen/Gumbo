package gumbo.structures.gfexpressions.operations;

import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;

import java.util.HashMap;
import java.util.Map;

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
	
	public String projectString(Tuple t) throws NonMatchingTupleException {
		
		StringBuilder sb = new StringBuilder(target.getName().length()+t.generateString().length());

		
		// head
		sb.append(target.getName());
		sb.append('(');
		
		// middle
		int i;
		int fields = target.getNumFields()-1;
		for (i = 0; i < fields; i++) {
			sb.append(t.get(arrayMapping[i]));
			sb.append(',');
		}
		sb.append(t.get(arrayMapping[i]));
		
		// tail
		sb.append(')');
		
		return sb.toString();

	}


	/**
	 * @return relation schema of the output relation
	 */
	public RelationSchema getOutputSchema() {
		return target.getRelationSchema();
	}

}
