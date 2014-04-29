/**
 * Created: 28 Apr 2014
 */
package guardedfragment.mapreduce.planner.calculations;

import guardedfragment.structure.gfexpressions.GFExistentialExpression;

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
	
	GFExistentialExpression basicExpression;
	
	public BasicGFCalculationUnit(GFExistentialExpression basicExpression ) throws CalculationUnitException {
		
		if(!basicExpression.isBasicGF())
			throw new CalculationUnitException("Supplied expression is not basic");
		
		this.basicExpression = basicExpression;
		
	}

	/**
	 * 
	 */
	public BasicGFCalculationUnit() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see guardedfragment.mapreduce.planner.calculations.CalculationUnit#getNumRounds()
	 */
	@Override
	int getNumRounds() {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * @see guardedfragment.mapreduce.planner.calculations.CalculationUnit#getDependencies()
	 */
	@Override
	public
	Set<CalculationUnit> getDependencies() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see guardedfragment.mapreduce.planner.calculations.CalculationUnit#getOutputSchema()
	 */
	@Override
	RelationSchema getOutputSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see guardedfragment.mapreduce.planner.calculations.CalculationUnit#getMapper(int)
	 */
	@Override
	Mapper<LongWritable, Text, Text, Text> getMapper(int round) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see guardedfragment.mapreduce.planner.calculations.CalculationUnit#getReducer(int)
	 */
	@Override
	Reducer<LongWritable, Text, Text, Text> getReducer(int round) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	/**
	 * @return the basicExpression
	 */
	public GFExistentialExpression getBasicExpression() {
		return basicExpression;
	}

}
