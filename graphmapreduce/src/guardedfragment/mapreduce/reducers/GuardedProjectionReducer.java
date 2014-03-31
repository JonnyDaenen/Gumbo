package guardedfragment.mapreduce.reducers;

import guardedfragment.structure.*;
import guardedfragment.booleanstructure.*;

import java.io.IOException;
import java.util.Set;

import mapreduce.data.Tuple;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Phase: Basic Guarded - Phase 2 Reducer
 * 
 * Input: Si(a,b) : set of tuples
 * Output: Si(a,b);R(a',b') (note the semicolon!)
 * 
 * Configuration: Guarding relation R, Guarded relations Si
 * 
 * This reducer checks for each data tuple Si(a,b) whether:
 * - it appears in R
 * - it appears in Si
 * 
 * When this is the case, both existing tuples are output.
 * 
 * 
 * @author Jonny Daenen
 *
 */
public class GuardedProjectionReducer extends Reducer<Text, Text, Text, Text>{
	
	GFAtomicExpression guard;
	GFExpression child;
	GFBMapping mapGFtoB;
	BExpression Bchild;

	public GuardedProjectionReducer(GFAtomicExpression g, GFExpression c) {
		super();
		this.guard=g;
		this.child=c;
		
		mapGFtoB = new GFBMapping();
		Set<GFAtomicExpression> allAtom = child.getAtomic();
		for (GFAtomicExpression atom : allAtom) {
			mapGFtoB.insertElement(atom);
		}

		try {
			Bchild = child.convertToBExpression(mapGFtoB);
		} catch (GFConversionException e) {
			// should not happen!
			e.printStackTrace();
		}
		
	}
	

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String s = key.toString();
		if(guard.matches(new Tuple(s))) {
			
			BEvaluationContext BchildEval = new BEvaluationContext();
			Set<GFAtomicExpression> allAtom = child.getAtomic();
			
			for (GFAtomicExpression atom : allAtom) {
				BchildEval.setValue(mapGFtoB.getVariable(atom), false);
			}
			
			Tuple t;
			GFAtomicExpression dummy;
			for (Text value : values) {
				t = new Tuple(value.toString());
				dummy = new GFAtomicExpression(t.getName(),t.getAllData());
				
				BchildEval.setValue(mapGFtoB.getVariable(dummy), true);
				
			}
			
			try
			{
				if (Bchild.evaluate(BchildEval)) {
					context.write(new Text(new String()), key);
				}
			} catch (VariableNotFoundException e) {
				// should not happen
				e.printStackTrace();
			}
				
		}
		
	}
	
/*
	protected void oldreduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Set<RelationSchema> foundRelations = new HashSet<RelationSchema>();
		Set<Tuple> outputTuples = new HashSet<Tuple>();
		
		// boolean allRelationsFound = false;
		
		// Record presence of all required relations
		for (Text value : values) {
			Tuple t = new Tuple(value.toString());
			
			if (t.satisfiesSchema(outputrelation))
				outputTuples.add(t);
			
			RelationSchema s = t.extractSchema();
			
			if(relations.contains(s))
				foundRelations.add(s);
			
		}
		
		// if all relations are found 
		if(foundRelations.size() == relations.size())
			for (Tuple t : outputTuples) {
				String value = t.generateString();
				context.write(key, new Text(value));
			}
		
	}
*/


}
