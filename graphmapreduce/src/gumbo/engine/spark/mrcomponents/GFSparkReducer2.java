/**
 * Created on: 19 Feb 2015
 */
package gumbo.engine.spark.mrcomponents;

import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.booleanexpressions.BEvaluationContext;
import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.conversion.GFBooleanMapping;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;

import java.util.HashSet;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

/**
 * @author Jonny Daenen
 *
 */
public class GFSparkReducer2 extends GFSparkComponent implements FlatMapFunction<Tuple2<String, Iterable<String>>, Tuple2<String,String>> {


	private static final long serialVersionUID = 1L;


	public GFSparkReducer2(ExpressionSetOperations eso, AbstractExecutorSettings settings) {
		super(eso, settings);
	}


	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> kvset) throws Exception {

		// extract  valueset
		Iterable<String> values = kvset._2;

		// create boolean value mapping
		GFBooleanMapping mapGFtoB = eso.getBooleanMapping();
		BEvaluationContext booleanContext = new BEvaluationContext();

		HashSet<Tuple2<String,String>> output = new HashSet<>();
		Tuple keyTuple = null;

		for (String value : values) {

			// we need to find the actual tuple between the values.
			if ( isTuple(value) ) {

				// extract the tuple
				keyTuple = getTuple(value);

			} else {
				// non-tuples are id's
				int id = Integer.parseInt(value);

				// adjust context
				GFAtomicExpression atom = eso.getAtom(id); 
				booleanContext.setValue(mapGFtoB.getVariable(atom), true);
			}
		}

		// TODO exception if tuple is not found


		// evaluate all formulas
		for (GFExistentialExpression formula : eso.getExpressionSet()) {

			// only if applicable
			if (formula.getGuard().matches(keyTuple)) {

				// get associated boolean expression
				BExpression booleanChildExpression = eso.getBooleanChildExpression(formula);

				if (booleanChildExpression.evaluate(booleanContext)) {

					// determine output
					GFAtomProjection p = eso.getOutputProjection(formula);
					String outputRelation = p.getOutputSchema().toString(); // TODO #spark check this!
					String outputTuple = p.project(keyTuple).generateString();

					// add relation identicator + real tuple to the output
					output.add(new Tuple2<>(outputRelation,outputTuple));
				} 
			}

		}

		return output;
	}


	/**
	 * Extracts the guard tuple from a string. The String must have the form "#R(1,2,...,n)" (without quotes).
	 * @param val
	 * @return
	 */
	private Tuple getTuple(String val) {
		return new Tuple(val.substring(1));
	}

	/**
	 * Checks id the strings contains a guard tuple
	 * 
	 * FIXME #spark use regex!
	 * @param val
	 * @return
	 */
	private boolean isTuple(String val) {
		if (val.length() > 0)
			return val.charAt(0) == '#'; // TODO put this in settings
		return false;
	}


}
