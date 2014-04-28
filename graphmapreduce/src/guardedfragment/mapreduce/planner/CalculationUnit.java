/**
 * Created: 28 Apr 2014
 */
package guardedfragment.mapreduce.planner;

import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.data.RelationSchema;

/**
 * Represents 1 calculation unit.
 * 
 * @author Jonny Daenen
 *
 */
public abstract class CalculationUnit {
	

	
	
	/**
	 * @return the number of MR-rounds this calculation takes.
	 */
	abstract int getNumRounds();
	
	/**
	 * @return set of dependent calculations
	 */
	abstract Set<CalculationUnit> getDependencies();
	
	/**
	 * @return the output schema
	 */
	abstract RelationSchema getOutputSchema();
	
	/**
	 * 
	 * @param round the round for which to look up the mapper
	 * @return the mapper for the specified round
	 */
	abstract Mapper<LongWritable, Text, Text, Text> getMapper(int round);
	
	/**
	 * 
	 * @param round the round for which to look up the reducer
	 * @return the reducer for the specified round
	 */
	abstract Reducer<LongWritable, Text, Text, Text> getReducer(int round);

}
