/**
 * Created: 28 Apr 2014
 */
package guardedfragment.mapreduce.planner;

import java.util.Set;

import mapreduce.data.RelationSchema;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Jonny Daenen
 *
 */
public class BasicGFCalculationUnit extends CalculationUnit {

	/**
	 * 
	 */
	public BasicGFCalculationUnit() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see guardedfragment.mapreduce.planner.CalculationUnit#getNumRounds()
	 */
	@Override
	int getNumRounds() {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * @see guardedfragment.mapreduce.planner.CalculationUnit#getDependencies()
	 */
	@Override
	Set<CalculationUnit> getDependencies() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see guardedfragment.mapreduce.planner.CalculationUnit#getOutputSchema()
	 */
	@Override
	RelationSchema getOutputSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see guardedfragment.mapreduce.planner.CalculationUnit#getMapper(int)
	 */
	@Override
	Mapper<LongWritable, Text, Text, Text> getMapper(int round) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see guardedfragment.mapreduce.planner.CalculationUnit#getReducer(int)
	 */
	@Override
	Reducer<LongWritable, Text, Text, Text> getReducer(int round) {
		// TODO Auto-generated method stub
		return null;
	}

}
