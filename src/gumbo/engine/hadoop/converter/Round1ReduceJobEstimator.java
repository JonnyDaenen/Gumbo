/**
 * Created: 14 Jan 2015
 */
package gumbo.engine.hadoop.converter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.utils.estimation.RandomTupleEstimator;
import gumbo.utils.estimation.TupleEstimator;

/**
 * Estimates the number of reducers needed. 
 * features:
 * - newline correction
 * - csv correction
 * @author Jonny Daenen
 *
 */
public class Round1ReduceJobEstimator {


	private HadoopExecutorSettings settings; // TODO #core fix settings passing
	private TupleEstimator tupleEstimator;


	/**
	 * 
	 */
	public Round1ReduceJobEstimator( HadoopExecutorSettings settings, TupleEstimator tupleEstimator) {
		this.settings = settings;
		this.tupleEstimator = tupleEstimator;
	}

	/**
	 * 
	 */
	public Round1ReduceJobEstimator(HadoopExecutorSettings settings) {
		this(settings, new RandomTupleEstimator(1024,10));
	}



	private static final Log LOG = LogFactory.getLog(Round1ReduceJobEstimator.class);


	/**
	 * @param job
	 * @param dirManager
	 * @return
	 */
	public int getNumReducers(Collection<GFExistentialExpression> exps, RelationFileMapping mapping) {
		int num;
		long bytesize = 0;


		for (GFExistentialExpression e: exps) {

			bytesize += estimateSize(e, mapping);
		}
		// TODO #multiquery compensate for multi-query
		// this can be done by ... 


		LOG.info("Output estimate " + bytesize);
		// 1 GB per reducer
		num = (int) Math.ceil(bytesize/1000000000.0);
		LOG.info("Reducer estimate " + num);

		return Math.max(1,num);
	}


	/**
	 * 
	 * @param e a guarded expression
	 * @param rfm relation-file mapping
	 * @return the estimated map output for the given expression
	 */
	private long estimateSize(GFExistentialExpression e, RelationFileMapping rfm) {
		RelationSchema guard = e.getGuard().getRelationSchema();
		Collection<GFAtomicExpression> guardedAtoms = e.getGuardedAtoms();


		long totalBytes = 0;
		totalBytes += estimateRequests(guard, guardedAtoms, rfm);
		totalBytes += estimateKeepAlives(guard, guardedAtoms, rfm);
		totalBytes += estimateProofOfExistance(guard, guardedAtoms, rfm);

		return totalBytes;

	}


	/**
	 * Estimates the size of the Proof of Existance messages
	 * @param guard the guard
	 * @param guardeds the guarded relations
	 * @param rfm relation-file mapping
	 * @return
	 */
	private long estimateProofOfExistance(RelationSchema guard, Collection<GFAtomicExpression> guardeds, RelationFileMapping rfm) {


		// make sure each schema is processed only once
		Set<RelationSchema> schemas = new HashSet<>();
		for (GFAtomicExpression guarded : guardeds) {
			schemas.add(guarded.getRelationSchema());
		}

		// keys
		long size = 0;
		for (RelationSchema rs : schemas) {
			size += rfm.getRelationSize(rs);

			// newlines take up bytes that are not transmitted
			long numTuples = rfm.visitAllPaths(rs,tupleEstimator);
			size -= numTuples;

			// compensation for csv to rel conversion
			if (rfm.getFormat(rs) == InputFormat.CSV) {
				// add wrap size for each tuple: parenthesis + relation name
				size += numTuples * (2 + rs.getName().length());
			}

		}

		LOG.info("keys: " + size);		


		// values
		if (settings.getBooleanProperty(HadoopExecutorSettings.assertConstantOptimizationOn)) {
			// value is now a constant
			for (RelationSchema rs : schemas) {

				long numTuples = rfm.visitAllPaths(rs,tupleEstimator);
				size += numTuples; // 1 byte for a tuple certificate
			}
		} else {
			// key and value are the same
			size *= 2;
		}

		long est = size;
		LOG.info("Estimated # proof of existance: " + est);		
		return est;
	}


	/**
	 * Estimates the number of keep alive messages sent by the guard.
	 * @param guard the guard
	 * @param guardeds the guarded relations
	 * @param rfm relation-file mapping
	 * @return an estimate for the number of keep-alive messages sent by the guard
	 */
	private long estimateKeepAlives(RelationSchema guard, Collection<GFAtomicExpression> guardeds, RelationFileMapping rfm) {

		// if optimization is on, no keep-alives are sent
		if (settings.getBooleanProperty(HadoopExecutorSettings.guardKeepAliveOptimizationOn)) {
			return 0;
		}

		long guardSize = rfm.getRelationSize(guard);
		long numTuples = rfm.visitAllPaths(guard,tupleEstimator);
		int atomSize = guard.toString().length();
		

		// newlines take up bytes that are not transmitted
		guardSize -= numTuples;

		// optimization correction
		if (settings.getBooleanProperty(HadoopExecutorSettings.assertConstantOptimizationOn)) {
			// 2 bytes for an atom id
			atomSize = 2;
		}

		// csv correction
		if (rfm.getFormat(guard) == InputFormat.CSV) {
			// add wrap size for each tuple: parenthesis + relation name
			guardSize += numTuples * (2 + guard.getName().length());
		}
		
		// TODO pointer optimization


		long est =  4 * guardSize + numTuples * (atomSize + 1); // 1 = separator 	
		LOG.info("Estimated # keep-alives: " + est);		
		return est;
	}


	/**
	 * Estimates the number of requests sent out by the guard.
	 * 
	 * @param guard the guard
	 * @param guardedAtoms the guarded relations
	 * @param rfm relation-file mapping
	 * 
	 * @return an estimate for the number of request messages sent by the guard
	 */
	private long estimateRequests(RelationSchema guard, Collection<GFAtomicExpression> guardedAtoms, RelationFileMapping rfm) {

		// guard R metrics
		long guardSize = rfm.getRelationSize(guard);
		long guardSize2 = guardSize;
		long numTuples = rfm.visitAllPaths(guard,tupleEstimator);
		int arityR = guard.getNumFields();

		// guarded S metrics
		int totalGuardedArity = 0;
		int totalGuardedAtomSize = 0;
		for (GFAtomicExpression guarded : guardedAtoms) {
			totalGuardedArity += guarded.getNumFields();
			totalGuardedAtomSize += guarded.toString().length();
		}


		// optimization corrections

		if (settings.getBooleanProperty(HadoopExecutorSettings.guardReferenceOptimizationOn)) {
			// use numtuples * 16 as the guardsize
			// 16 is approx for 10byte 64-bit long encoding and 6 ascii digits for file id 
			guardSize2 = numTuples * 16;
			LOG.info("compression rate: " + (1 - ((double)guardSize2/guardSize)));	
		}


		if (settings.getBooleanProperty(HadoopExecutorSettings.requestAtomIdOptimizationOn)) {
			// guardeds.size() must be replaced with 2, as an approximation for id bytes.
			totalGuardedAtomSize = guardedAtoms.size() * 2;

		}

		LOG.info("guardSize: " + guardSize);	
		LOG.info("arityR: " + arityR);	
		LOG.info("Num R tuples: " + numTuples);		
		LOG.info("guardSize2: " + guardSize2);
		LOG.info("totalGuardedArity: " + totalGuardedArity);	
		LOG.info("totalGuardedAtomSize: " + totalGuardedAtomSize);	

		long est = 0;
		est += (long)(guardSize * totalGuardedArity/(double)arityR); // keys
		est += guardSize2 * guardedAtoms.size(); // values: address part
		est += numTuples * totalGuardedAtomSize; // values: action part
		LOG.info("Estimated # requests: " + est);

		return est;

	}


	/**
	 * Estimates the output of a keep-alive mapper without id's.
	 * (OLD version)
	 * 
	 * @param guard the guard schema
	 * @param guardeds the guarded schemas
	 * @param rfm a file mapping
	 * @return a map output size estimate
	 */
	@SuppressWarnings("unused")
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
