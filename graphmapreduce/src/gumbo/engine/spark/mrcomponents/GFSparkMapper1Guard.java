/**
 * Created on: 19 Feb 2015
 */
package gumbo.engine.spark.mrcomponents;

import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;

import java.util.HashSet;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * TODO add eso to serialization?
 * @author Jonny Daenen
 *
 */
public class GFSparkMapper1Guard extends GFSparkComponent implements  PairFlatMapFunction<Tuple2<String,String>, String, String> {



	
	public GFSparkMapper1Guard() {
		super();
	}
	
	public GFSparkMapper1Guard(ExpressionSetOperations eso, AbstractExecutorSettings settings) {
		super(eso, settings);
	}




	/**
	 * Maps a key-value pair onto a new set of key-value pairs.
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<Tuple2<String, String>> call(Tuple2<String, String> kvpair) throws Exception {
		
		HashSet<Tuple2<String, String>> result = new HashSet<>();
		
		String tupleID = kvpair._1;
		Tuple t = new Tuple(kvpair._2);
		boolean guardIsGuarded = false;
		
		
		// for each guard atom ...
		for (GFAtomicExpression guard : eso.getGuardsAll()) {
			
			// ... that matches the current tuple
			if (guard.matches(t)) {
				
				// for each guarded atom
				for (GFAtomicExpression guarded : eso.getGuardeds(guard)) {
					
					// if guarded is same relation, output proof of existence afterwards
					if (guarded.getRelationSchema().equals(guard.getRelationSchema())) {
						guardIsGuarded = true;
					}
					
					// project onto guarded
					GFAtomProjection p = eso.getProjections(guard, guarded);
					Tuple tprime = p.project(t);
					
					// output request
					int guardedID = eso.getAtomId(guarded);
					
					String outKey = tprime.toString();
					String outVal = tupleID + ";" + guardedID;
					result.add(new Tuple2<String, String>(outKey, outVal));
					
				}
				
			}
			
		}
		if (guardIsGuarded) {
//			LOG.error("guard output POE");
			String outKey = t.toString();
			String outVal = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);
			result.add(new Tuple2<String, String>(outKey, outVal));
			
		}
		
		return result;
	}

}
