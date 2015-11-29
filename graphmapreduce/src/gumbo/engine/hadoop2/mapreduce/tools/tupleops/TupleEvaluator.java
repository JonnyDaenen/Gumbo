package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.List;

import org.apache.hadoop.io.Text;

import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.structures.booleanexpressions.BEvaluationContext;
import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.data.QuickTuple;
import gumbo.structures.gfexpressions.GFExistentialExpression;


/**
 * Guard tuple checker for reducer of evaluation operation.
 * It can evaluate the boolean expression and project the guard onto
 * an output schema. It also keeps track of a target file location.
 * 
 * @author Jonny Daenen
 *
 */
public class TupleEvaluator {

	BExpression be;
	BEvaluationContext context;
	List<Integer> fields;
	String filename;
	
	public TupleEvaluator(GFExistentialExpression e, String filename) {
		// TODO implement
		this.filename = filename;
	}

	/**
	 * Check if the guarded part is satisfied.
	 * 
	 * @param atomids truth values for all atoms
	 * @return true iff expression evaluates to true
	 */
	public boolean eval(boolean[] atomids) {

		// set correct atoms ids to true
		for (int i = 0; i < atomids.length; i++) {
			context.setValue(i, atomids[i]);
		}
		return be.evaluate(context);
	}

	
	/**
	 * Projects the guard tuple to the correct fields 
	 * and puts them in the output writable.
	 * 
	 * @param data guarded data
	 * @param output
	 */
	public void project(QuickWrappedTuple qt, Text output) {
		qt.project(fields, output);
	}
	

	/**
	 * Returns the location the output of this projection should go to.
	 * @return output location
	 */
	public String getFilename() {
		return filename;
	}

}
