/**
 * Created: 14 Jan 2015
 */
package mapreduce.guardedfragment.executor.hadoop;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import mapreduce.guardedfragment.planner.compiler.DirManager;
import mapreduce.guardedfragment.planner.structures.MRJob;
import mapreduce.guardedfragment.planner.structures.RelationFileMapping;
import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;

/**
 * Estimates the number of reducers needed. 
 * TODO move to a specific mapper?
 * @author jonny
 *
 */
public class ReduceJobEstimator {
	


	private static final Log LOG = LogFactory.getLog(ReduceJobEstimator.class);
	
	
	/**
	 * @param job
	 * @param dirManager
	 * @return
	 */
	public int getNumReducers(MRJob job, DirManager dirManager) {
		int num;
		long bytesize = 0;
		
		RelationFileMapping rfm = dirManager.getFileMapping();
		Collection<GFExistentialExpression> exps = job.getGFExpressions();
		
		for (GFExistentialExpression exp: exps) {
			RelationSchema guard = exp.getGuard().getRelationSchema();
			Collection<GFAtomicExpression> guardedExps = exp.getGuardedRelations();
			HashSet<RelationSchema> guardeds = new HashSet<RelationSchema>();
			for (GFAtomicExpression guardedExp : guardedExps) {
				guardeds.add(guardedExp.getRelationSchema());
			}
			bytesize += estimateSize(guard, guardeds, rfm);
		}
		// TODO compensate for multi-query
		
		
		num = (int) Math.ceil(bytesize/1000000000.0);
		LOG.info("Reducer estimate " + num);
		
		return Math.max(1,num);
	}


	/**
	 * Estimates the output of a keep-alive mapper without id's
	 * @param guard the guard schema
	 * @param guardeds the guarded schemas
	 * @param rfm a file mapping
	 * @return a map output size estimate
	 */
	private long estimateSize(RelationSchema guard, HashSet<RelationSchema> guardeds, RelationFileMapping rfm) {
		long alpha = rfm.getRelationSize(guard);
		long beta = 0;
		double TGAR = 0; // total guarded arity
		for (RelationSchema guarded : guardeds) {
			beta += rfm.getRelationSize(guarded);
			TGAR += guarded.getNumFields();
		}
		TGAR /= guard.getNumFields();

		LOG.info("alpha " + alpha);
		LOG.info("beta " + beta);
		LOG.info("TGAR " + TGAR);
		
		long guarded = 2 * beta;
		long guardx = (long) ((2 * TGAR + 1) * alpha);
		long keepalive = 5 * alpha;
		
		LOG.info("guarded " + guarded);
		LOG.info("guard " + guardx);
		LOG.info("keepalive " + keepalive);
		LOG.info("map1 output estimate: " + (guarded + guardx + keepalive) / 1000000.0);
		
		return guarded + guardx + keepalive; // guarded + guard + Keep-alive
	}

}
