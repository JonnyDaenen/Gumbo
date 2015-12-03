/**
 * Created on: 19 Feb 2015
 */
package gumbo.engine.spark.mrcomponents;

import java.util.HashSet;

import org.apache.spark.api.java.function.FlatMapFunction;

import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.structures.booleanexpressions.BEvaluationContext;
import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.conversion.GFBooleanMapping;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;
import scala.Tuple2;

/**
 * @author Jonny Daenen
 *
 */
public class GFSparkReducer2 extends GFSparkComponent implements FlatMapFunction<Tuple2<String, Iterable<String>>, Tuple2<String,String>> {


	private static final long serialVersionUID = 1L;


	public GFSparkReducer2() {
		// TODO Auto-generated constructor stub
	}

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
boolean print = false;
		for (String value : values) {

			// we need to find the actual tuple between the values.
			if ( isTuple(value) ) {

				// extract the tuple
				keyTuple = getTuple(value);

			} else {
				// non-tuples are id's
				int id = Integer.parseInt(value);
//				System.out.println(id);
				print = true;

				// adjust context
				GFAtomicExpression atom = eso.getAtom(id); 
				booleanContext.setValue(mapGFtoB.getVariable(atom), true);
			}
		}

		if (print) {
			System.out.println(keyTuple);
			System.out.println(mapGFtoB);
			System.out.println(booleanContext);
		}
		// TODO exception if tuple is not found


		// evaluate all formulas
		for (GFExistentialExpression formula : eso.getExpressionSet()) {

//			System.out.println("Considering formula " + formula);
			
			GFAtomicExpression guard = formula.getGuard();
			if (print) {
			System.out.println("Guard:" + guard);
			System.out.println("Keytuple: " + keyTuple);
			}

			// only if applicable
			if (guard.matches(keyTuple)) {
				if (print) {
				System.out.println("formula ok");
				}
				


				// get associated boolean expression
				BExpression booleanChildExpression = eso.getBooleanChildExpression(formula);
				

//				System.out.println(booleanChildExpression);
//				System.out.println(booleanContext);

				if (booleanChildExpression.evaluate(booleanContext)) {
					if (print) 
					System.out.println("evaluation ok");

					// determine output
					GFAtomProjection p = eso.getOutputProjection(formula);
					String outputRelation = p.getOutputSchema().toString(); // TODO #spark check this!
					String outputTuple = p.project(keyTuple).toString();

					if (print) 
					System.out.println(outputRelation);
					if (print) 
					System.out.println(outputTuple);
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
		return new Tuple(val);
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
			return val.contains("(");// FIXME regex and only when non-tuple mode on
//			return val.charAt(0) == '#'; // FIXME put this in settings
		return false;
	}


}
