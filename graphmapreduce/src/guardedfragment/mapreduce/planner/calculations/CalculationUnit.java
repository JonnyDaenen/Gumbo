/**
 * Created: 28 Apr 2014
 */
package guardedfragment.mapreduce.planner.calculations;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
	
	Map<RelationSchema,CalculationUnit> directDependencies;
	Set<RelationSchema> inputRelations;
	
	

	public CalculationUnit() {
		directDependencies = new HashMap<RelationSchema,CalculationUnit>();
		inputRelations = new HashSet<RelationSchema>();
	}

	
	
	/**
	 * @return the number of MR-rounds this calculation takes.
	 */
	abstract int getNumRounds();
	
	/**
	 * @return set of direct dependent
	 */
	public Collection<CalculationUnit> getDependencies() {
		return directDependencies.values();
	}
	
	/**
	 * @param cu a CU on which this CU depends
	 * @param rs the relation of the CU
	 */
	public void setDependency(RelationSchema rs,CalculationUnit cu) {
		directDependencies.put(rs, cu);
	}
	
	
	public void addInputRelation(RelationSchema rs) {
		inputRelations.add(rs);
	}
	
	public Set<RelationSchema> getInputRelations() {
		return inputRelations;
	}
	

	
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
