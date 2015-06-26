package gumbo.engine.hadoop.converter;

import java.util.Set;

import org.apache.hadoop.fs.Path;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.utils.estimation.RandomTupleEstimator;

/**
 * Tuple and byte size estimator for map 1 input.
 * 
 * @author Jonny Daenen
 *
 */
public class InputEstimator {

	RelationFileMapping mapping;


	/**
	 * Calculates an estimate for the number of tuples
	 * in a given relation. 
	 * 
	 * @param rs
	 * @return
	 */
	public long estNumTuples(RelationSchema rs) {
		long total = 0;
		
		RandomTupleEstimator rte = new RandomTupleEstimator();
		Set<Path> paths = mapping.getPaths(rs);
		for (Path path : paths) {
			total += rte.estimateNumTuples(path);
		}
		return total;
	}

	/**
	 * Calculates the number of bytes in all the files of a relation.
	 * @param rs
	 * @return The number of bytes in the relation.
	 */
	public long getNumBytes(RelationSchema rs) {
		return mapping.getRelationSize(rs);
	}

	/**
	 * Get total bytes of all the relations in the group.
	 * Guards and guardeds are considered in a distinct way.
	 * @param group
	 * @return
	 */
	public long getTotalBytes(CalculationGroup group) {
		long total = 0;
		for ( GFAtomicExpression exp : group.getGuardDistinctList()) {
			total += getNumBytes(exp.getRelationSchema());
		}

		for ( GFAtomicExpression exp : group.getGuardedDistinctList()) {
			total += getNumBytes(exp.getRelationSchema());
		}
		return total;
	}

}
